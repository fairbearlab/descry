package runner

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/fairbearlab/descry/check"
	"github.com/fairbearlab/descry/event"
	"github.com/fairbearlab/descry/metrics"
)

// ctxCheck blocks until ctx is done, then returns obs and err, or ctx.Err()
// when ctxErr is set: the shapes a check returns when it is cut short.
type ctxCheck struct {
	started chan struct{} // receives once per Run, at entry
	obs     check.Observation
	err     error
	ctxErr  bool
}

func (c *ctxCheck) Name() string { return "ctx" }

func (c *ctxCheck) Run(ctx context.Context, _ check.Target) (check.Observation, error) {
	c.started <- struct{}{}
	<-ctx.Done()
	if c.ctxErr {
		return check.Observation{}, ctx.Err()
	}
	return c.obs, c.err
}

// countingSink counts publishes.
type countingSink struct{ n atomic.Int64 }

func (s *countingSink) Publish(context.Context, cloudevents.Event) error { s.n.Add(1); return nil }

// TestShutdown_InFlightRunDropped: a run cut short by the cancel publishes no
// event and reports no Result, whether the check returns a down Observation
// (the bundled HTTP check's shape), a Result error, a not-run error or its own
// ctx error.
func TestShutdown_InFlightRunDropped(t *testing.T) {
	for name, shape := range map[string]ctxCheck{
		"down observation": {obs: check.Observation{Status: check.StatusDown, ErrorClass: check.ErrUnknown, ObservedAt: time.Now().UTC()}},
		"ctx error":        {ctxErr: true},
		"other error":      {err: errors.New("custom check failure")},
		"not-run error":    {err: check.ErrNotRun},
	} {
		t.Run(name, func(t *testing.T) {
			const iv = 10 * time.Second
			chk := &ctxCheck{started: make(chan struct{}, 1), obs: shape.obs, err: shape.err, ctxErr: shape.ctxErr}
			s := &countingSink{}
			fc := newFakeClock(t)
			r := New(chk, s, event.Config{Source: "test"}, []check.Target{{URL: "http://inflight"}}, iv, 1)
			tr := startTestRunner(t, r, fc)
			tr.advanceTo(firstSlot("http://inflight", iv))
			<-chk.started

			tr.stop()
			if !errors.Is(tr.err, context.Canceled) {
				t.Fatalf("Run returned %v, want context.Canceled", tr.err)
			}
			for res := range tr.Results() {
				t.Errorf("Result %+v for a run cut short by shutdown; want none", res)
			}
			if n := s.n.Load(); n != 0 {
				t.Errorf("%d events published for a run cut short by shutdown; want 0", n)
			}
		})
	}
}

// TestShutdown_CompletedBeforeCancelIsKept: the drop is only for runs that
// return after the cancel; a run that returned before it is published and
// reported as usual.
func TestShutdown_CompletedBeforeCancelIsKept(t *testing.T) {
	const iv = 10 * time.Second
	chk := &fakeCheck{calls: make(chan call, 1)}
	fc := newFakeClock(t)
	chk.now = fc.Now
	s := &countingSink{}
	r := New(chk, s, event.Config{Source: "test"}, []check.Target{{URL: "http://done"}}, iv, 1)
	tr := startTestRunner(t, r, fc)
	tr.advanceTo(firstSlot("http://done", iv))
	recvCall(t, chk.calls)
	tr.awaitAck(t)
	tr.stop()
	res := recv(t, tr.Results())
	if res.Err != nil || res.Target.URL != "http://done" {
		t.Fatalf("Result = %+v, want the completed run", res)
	}
	if s.n.Load() != 1 {
		t.Fatalf("published %d, want 1", s.n.Load())
	}
}

// blockingSink blocks in Publish, ignoring ctx, until release closes.
type blockingSink struct {
	entered chan struct{}
	release chan struct{}
	done    atomic.Int64
}

func (s *blockingSink) Publish(context.Context, cloudevents.Event) error {
	s.entered <- struct{}{}
	<-s.release
	s.done.Add(1)
	return nil
}

// TestShutdown_GraceBlockedPublishStillReports is the documented exception:
// a run whose check returned before the cancel and whose sink is blocked in
// Publish outlives a WithShutdownGrace return, still delivers its event, and
// still reports its Result on the open channel. A check wedged at the same
// time, which returns after the cancel, is dropped.
func TestShutdown_GraceBlockedPublishStillReports(t *testing.T) {
	const iv = 10 * time.Second
	const grace = 50 * time.Millisecond
	s := &blockingSink{entered: make(chan struct{}, 2), release: make(chan struct{})}
	wedge := make(chan struct{})
	var once sync.Once
	releaseAll := func() { once.Do(func() { close(wedge); close(s.release) }) }
	t.Cleanup(releaseAll)

	// Target "pub" completes at once and blocks in Publish; target "wedged"
	// ignores ctx. Two workers, so both are in flight together.
	pubURL, wedgedURL := "http://pub", "http://wedged"
	chk := &routeCheck{wedged: wedgedURL, wedge: wedge, started: make(chan string, 2)}
	fc := newFakeClock(t)
	r := New(chk, s, event.Config{Source: "test"},
		[]check.Target{{URL: pubURL}, {URL: wedgedURL}}, iv, 2, WithShutdownGrace(grace))
	tr := startTestRunner(t, r, fc)
	tr.advance(iv) // both targets' first slots fall inside one interval
	<-chk.started
	<-chk.started
	<-s.entered // "pub" is blocked in Publish

	tr.stop()
	if !errors.Is(tr.err, ErrShutdownTimeout) {
		t.Fatalf("Run returned %v, want ErrShutdownTimeout", tr.err)
	}
	releaseAll()

	res := recv(t, tr.Results())
	if res.Target.URL != pubURL || res.Err != nil {
		t.Fatalf("Result = %+v, want the blocked publish's completion", res)
	}
	if s.done.Load() != 1 {
		t.Fatalf("publishes completed = %d, want 1 (the wedged check must not publish)", s.done.Load())
	}
	tr.awaitAck(t)
	tr.awaitAck(t)
	select {
	case res := <-tr.Results():
		t.Fatalf("second Result %+v; the wedged run returned after the cancel and must be dropped", res)
	default:
	}
}

// routeCheck returns up at once, except for the wedged URL, which blocks
// until wedge closes, ignoring ctx.
type routeCheck struct {
	wedged  string
	wedge   chan struct{}
	started chan string
}

func (c *routeCheck) Name() string { return "route" }

func (c *routeCheck) Run(_ context.Context, t check.Target) (check.Observation, error) {
	c.started <- t.URL
	if t.URL == c.wedged {
		<-c.wedge
	}
	return check.Observation{Status: check.StatusUp, StatusCode: 200, ObservedAt: time.Now().UTC(),
		Labels: t.Labels, Extra: map[string]any{}}, nil
}

// deadlineCtx ends like a context whose deadline passed: once its parent is
// done, Err reports context.DeadlineExceeded.
type deadlineCtx struct{ context.Context }

func (c deadlineCtx) Err() error {
	if c.Context.Err() != nil {
		return context.DeadlineExceeded
	}
	return nil
}

// Value: protects=a Runner.Run ctx that ends by deadline cuts in-flight runs
// short as cancelled, so the metrics decorator records nothing for them;
// fails_when=checks run on Run's ctx directly (the decorator then sees
// DeadlineExceeded and records a false down); why_new=the other shutdown
// tests cancel Run's ctx, which never produced the deadline shape; seam=none;
// no_claim=a real deadline timer (deadlineCtx stands in for one)
func TestShutdown_DeadlineRecordsNoVerdict(t *testing.T) {
	const iv = 10 * time.Second
	c, err := metrics.New(metrics.Options{})
	if err != nil {
		t.Fatal(err)
	}
	inner := &ctxCheck{started: make(chan struct{}, 1),
		obs: check.Observation{Status: check.StatusDown, ErrorClass: check.ErrTimeout, ObservedAt: time.Now().UTC()}}
	seen := make(chan error, 1)
	chk := c.Check(observingCheck{Check: inner, seen: seen})
	s := &countingSink{}
	fc := newFakeClock(t)
	r := New(chk, s, event.Config{Source: "test"}, []check.Target{{URL: "http://deadline"}}, iv, 1)
	r.clock = fc
	acks := make(chan struct{}, 16)
	r.afterDone = func() { acks <- struct{}{} }
	parent, cancel := context.WithCancel(context.Background())
	ret := make(chan error, 1)
	go func() { ret <- r.Run(deadlineCtx{parent}) }()
	fc.waitReset(1)
	fc.Advance(firstSlot("http://deadline", iv).Sub(fc.Now()))
	<-inner.started

	cancel() // Run's ctx now reports DeadlineExceeded
	if err := <-ret; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run returned %v, want context.DeadlineExceeded", err)
	}
	if got := <-seen; !errors.Is(got, context.Canceled) {
		t.Fatalf("check saw ctx.Err() = %v at return, want context.Canceled", got)
	}
	if n := s.n.Load(); n != 0 {
		t.Errorf("%d events published for a run cut short by a deadline; want 0", n)
	}
	mfs, err := c.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == "descry_check_up" || mf.GetName() == "descry_check_runs_total" {
			t.Errorf("%s exported %d series for a run cut short by a deadline; want none", mf.GetName(), len(mf.GetMetric()))
		}
	}
}

// observingCheck reports the ctx error its inner check returned under.
type observingCheck struct {
	check.Check
	seen chan<- error
}

func (o observingCheck) Run(ctx context.Context, t check.Target) (check.Observation, error) {
	obs, err := o.Check.Run(ctx, t)
	o.seen <- ctx.Err()
	return obs, err
}
