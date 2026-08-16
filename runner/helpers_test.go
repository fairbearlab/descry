package runner

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/fairbearlab/descry/check"
	"github.com/fairbearlab/descry/event"
	"github.com/fairbearlab/descry/sink"
)

// This file holds every test double for the runner package: one fake
// check, the sinks, the single-timer fake clock, a slog capture handler,
// and the newTestRunner harness. Tests set no global state except the slog
// default (see captureLogs), so nothing here calls t.Parallel.

// epoch is the fake clock's starting instant: a whole minute, so slot
// arithmetic in tests is easy to read.
var epoch = time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)

// --- fake check ---

// call records one invocation of fakeCheck.Run.
type call struct {
	url string
	at  time.Time // fake-clock time at Run entry (real time when now is nil)
}

// fakeCheck is the one check double. Zero value: returns StatusUp immediately.
//   - gate != nil: Run blocks until gate is closed or ctx is done.
//   - calls != nil: Run sends a call at entry (buffer it generously).
//   - delay > 0: Run sleeps that long in real time (scale tests).
//   - err != nil: Run returns it.
//
// It also detects a target running concurrently with itself (overlap) and the
// peak number of concurrent runs across all targets (peak).
type fakeCheck struct {
	gate  chan struct{}
	calls chan call
	delay time.Duration
	err   error
	now   func() time.Time

	total   atomic.Int64
	overlap atomic.Int64 // times a URL was entered while already running

	mu       sync.Mutex
	inflight map[string]int
	current  int
	peak     int
}

func (c *fakeCheck) Name() string { return "fake" }

func (c *fakeCheck) Run(ctx context.Context, t check.Target) (check.Observation, error) {
	c.total.Add(1)
	c.mu.Lock()
	if c.inflight == nil {
		c.inflight = map[string]int{}
	}
	if c.inflight[t.URL] > 0 {
		c.overlap.Add(1)
	}
	c.inflight[t.URL]++
	c.current++
	if c.current > c.peak {
		c.peak = c.current
	}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.inflight[t.URL]--
		c.current--
		c.mu.Unlock()
	}()

	if c.calls != nil {
		at := time.Now()
		if c.now != nil {
			at = c.now()
		}
		c.calls <- call{url: t.URL, at: at}
	}
	if c.gate != nil {
		select {
		case <-c.gate:
		case <-ctx.Done():
		}
	}
	if c.delay > 0 {
		select {
		case <-time.After(c.delay):
		case <-ctx.Done():
		}
	}
	if c.err != nil {
		return check.Observation{}, c.err
	}
	return check.Observation{
		Status: check.StatusUp, StatusCode: 200, ObservedAt: time.Now().UTC(),
		Labels: t.Labels, Extra: map[string]any{},
	}, nil
}

func (c *fakeCheck) peakConcurrency() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peak
}

// --- sinks ---

// nopSink accepts every event.
type nopSink struct{}

func (nopSink) Publish(_ context.Context, _ cloudevents.Event) error { return nil }

var _ sink.EventSink = nopSink{}

// closedSink counts publishes and flags any that arrive after close() —
// the "no Publish after Run returns" shutdown contract.
type closedSink struct {
	closed     atomic.Bool
	published  atomic.Int64
	afterClose atomic.Int64
}

func (s *closedSink) Publish(_ context.Context, _ cloudevents.Event) error {
	if s.closed.Load() {
		s.afterClose.Add(1)
	}
	s.published.Add(1)
	return nil
}

// flakySink fails its first failsLeft Publish calls, then succeeds.
type flakySink struct {
	failsLeft atomic.Int64
	calls     atomic.Int64
}

func (s *flakySink) Publish(_ context.Context, _ cloudevents.Event) error {
	s.calls.Add(1)
	if s.failsLeft.Add(-1) >= 0 {
		return errors.New("transient publish failure")
	}
	return nil
}

// alwaysFailSink fails every Publish call.
type alwaysFailSink struct{ calls atomic.Int64 }

func (s *alwaysFailSink) Publish(_ context.Context, _ cloudevents.Event) error {
	s.calls.Add(1)
	return errors.New("sink down")
}

// --- fake clock ---

// fakeClock is a hand-rolled single-timer clock. The scheduler creates exactly
// one timer and reuses it; creating a second while one exists panics,
// which turns that design assumption into a regression detector.
//
// Advance moves time and fires the timer if its deadline has passed. Step
// moves the wall clock without elapsing time: like a real Go timer, the
// pending deadline is monotonic and shifts with the step. BlockUntil(n) parks
// until n (0 or 1) timers are armed — after Advance, BlockUntil(1) means the
// scheduler has processed everything due and is waiting again.
//
// Tripwire: if this needs multi-timer ordering or grows past ~80 lines,
// swap to jonboulle/clockwork behind the same clock/timer interfaces.
type fakeClock struct {
	tb      testing.TB
	mu      sync.Mutex
	now     time.Time
	tm      *fakeTimer
	resets  int           // total Reset calls; newTestRunner waits for the first
	changed chan struct{} // closed and replaced on every state change
}

type fakeTimer struct {
	fc       *fakeClock
	c        chan time.Time
	deadline time.Time
	live     bool
}

func newFakeClock(tb testing.TB) *fakeClock {
	tb.Helper()
	return &fakeClock{tb: tb, now: epoch, changed: make(chan struct{})}
}

func (fc *fakeClock) Now() time.Time { fc.mu.Lock(); defer fc.mu.Unlock(); return fc.now }

func (fc *fakeClock) NewTimer(d time.Duration) timer {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if fc.tm != nil {
		panic("fakeClock: second timer created while one is pending (scheduler must reuse its one timer)")
	}
	fc.tm = &fakeTimer{fc: fc, c: make(chan time.Time, 1), deadline: fc.now.Add(d), live: true}
	fc.notify()
	return fc.tm
}

// notify wakes BlockUntil waiters. Caller holds mu.
func (fc *fakeClock) notify() { close(fc.changed); fc.changed = make(chan struct{}) }

// fireLocked delivers the timer if live. Caller holds mu.
func (fc *fakeClock) fireLocked() {
	if fc.tm == nil || !fc.tm.live {
		return
	}
	fc.tm.live = false
	select {
	case fc.tm.c <- fc.now:
	default:
	}
	fc.notify()
}

// Advance elapses d, firing the timer if its deadline is reached.
func (fc *fakeClock) Advance(d time.Duration) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.now = fc.now.Add(d)
	if fc.tm != nil && fc.tm.live && !fc.tm.deadline.After(fc.now) {
		fc.fireLocked()
	}
}

// Step moves the wall clock by d (negative = backward) without elapsing time.
// The pending timer's deadline shifts with it (monotonic semantics), so it
// still fires after the originally armed duration.
func (fc *fakeClock) Step(d time.Duration) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.now = fc.now.Add(d)
	if fc.tm != nil {
		fc.tm.deadline = fc.tm.deadline.Add(d)
	}
}

// FireEarly delivers the timer now, before its deadline (a spurious wake).
func (fc *fakeClock) FireEarly() { fc.mu.Lock(); defer fc.mu.Unlock(); fc.fireLocked() }

// BlockUntil parks until exactly n timers are armed, or fails the test after 5s.
func (fc *fakeClock) BlockUntil(n int) {
	fc.tb.Helper()
	fc.waitFor("BlockUntil", func() bool {
		armed := 0
		if fc.tm != nil && fc.tm.live {
			armed = 1
		}
		return armed == n
	})
}

// waitReset parks until the timer has been Reset at least n times in total.
func (fc *fakeClock) waitReset(n int) {
	fc.tb.Helper()
	fc.waitFor("waitReset", func() bool { return fc.resets >= n })
}

// waitFor parks until cond (evaluated under mu) holds; fails after 5s.
func (fc *fakeClock) waitFor(what string, cond func() bool) {
	fc.tb.Helper()
	deadline := time.After(5 * time.Second)
	for {
		fc.mu.Lock()
		ok := cond()
		ch := fc.changed
		fc.mu.Unlock()
		if ok {
			return
		}
		select {
		case <-ch:
		case <-deadline:
			fc.tb.Fatalf("fakeClock: %s timed out", what)
		}
	}
}

func (t *fakeTimer) C() <-chan time.Time { return t.c }

func (t *fakeTimer) Stop() bool {
	t.fc.mu.Lock()
	defer t.fc.mu.Unlock()
	was := t.live
	t.live = false
	t.fc.notify()
	return was
}

// Reset re-arms for d. The scheduler computes d as next−now and then calls
// Reset; if the test advanced the clock in between, a naive now+d deadline
// would land past the intended slot with no time left to flow (a real clock
// would simply fire late). So while the timer is live, or a fire is pending
// unconsumed, the new deadline is min(old, now+d): never later than the slot
// the scheduler is arming for, at worst one early wake, which the scheduler
// treats as a re-arm. d <= 0 fires immediately.
func (t *fakeTimer) Reset(d time.Duration) bool {
	t.fc.mu.Lock()
	defer t.fc.mu.Unlock()
	was := t.live
	pending := false
	select {
	case <-t.c:
		pending = true
	default:
	}
	deadline := t.fc.now.Add(d)
	if (was || pending) && t.deadline.Before(deadline) {
		deadline = t.deadline
	}
	t.deadline = deadline
	t.live = true
	t.fc.resets++
	t.fc.notify()
	if !t.deadline.After(t.fc.now) {
		t.fc.fireLocked()
	}
	return was
}

// --- slog capture ---

// captureHandler records every log record at or above min.
type captureHandler struct {
	min slog.Level
	mu  sync.Mutex
	rec []slog.Record
}

func (h *captureHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.min }
func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.rec = append(h.rec, r)
	return nil
}
func (h *captureHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(_ string) slog.Handler      { return h }

// records returns the captured records at exactly level l.
func (h *captureHandler) records(l slog.Level) []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []slog.Record
	for _, r := range h.rec {
		if r.Level == l {
			out = append(out, r)
		}
	}
	return out
}

// captureLogs installs a capturing slog default for the test's lifetime.
func captureLogs(t *testing.T, level slog.Level) *captureHandler {
	t.Helper()
	h := &captureHandler{min: level}
	prev := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return h
}

// --- harness ---

// testRunner is a Runner started under a fake clock. stop cancels Run and
// waits for it to return, after which entries are safe to read from the test.
// acks receives one signal per worker ack (via the afterDone hook), so tests
// can wait until a completed run's done is on the channel before advancing to
// the next slot — with a fake clock, time can otherwise outrun the worker
// goroutine and turn a finished run into a (correct but unwanted) skip.
type testRunner struct {
	*Runner
	fc     *fakeClock
	acks   chan struct{}
	cancel context.CancelFunc
	ret    chan error
	err    error
	once   sync.Once
}

// newTestRunner builds and starts a Runner on a fresh fake clock. If chk.now
// is nil it is wired to the fake clock so call stamps are fake-clock times.
func newTestRunner(t *testing.T, chk *fakeCheck, targets []check.Target,
	interval time.Duration, concurrency int) *testRunner {
	t.Helper()
	fc := newFakeClock(t)
	if chk.now == nil {
		chk.now = fc.Now
	}
	r := New(chk, nopSink{}, event.Config{Source: "test"}, targets, interval, concurrency)
	return startTestRunner(t, r, fc)
}

// startTestRunner runs r under fc and returns once the scheduler has armed
// for its first slot and is waiting.
func startTestRunner(t *testing.T, r *Runner, fc *fakeClock) *testRunner {
	t.Helper()
	r.clock = fc
	tr := &testRunner{Runner: r, fc: fc, acks: make(chan struct{}, 1024), ret: make(chan error, 1)}
	r.afterDone = func() { tr.acks <- struct{}{} }
	ctx, cancel := context.WithCancel(context.Background())
	tr.cancel = cancel
	go func() { tr.ret <- r.Run(ctx) }()
	t.Cleanup(tr.stop)
	fc.waitReset(1)
	return tr
}

// stop cancels Run and waits for it to return.
func (tr *testRunner) stop() {
	tr.once.Do(func() {
		tr.cancel()
		select {
		case tr.err = <-tr.ret:
		case <-time.After(5 * time.Second):
			panic("testRunner: Run did not return after cancel")
		}
	})
}

// advance elapses d and waits for the scheduler to be armed again, i.e. every
// slot that came due inside d has been dispatched or skipped.
func (tr *testRunner) advance(d time.Duration) {
	tr.fc.Advance(d)
	tr.fc.BlockUntil(1)
}

// advanceTo elapses time to exactly at (which must not be in the past).
func (tr *testRunner) advanceTo(at time.Time) {
	if d := at.Sub(tr.fc.Now()); d > 0 {
		tr.advance(d)
	}
}

// awaitAck waits until a worker has put one entry's done ack on the channel.
func (tr *testRunner) awaitAck(t *testing.T) {
	t.Helper()
	select {
	case <-tr.acks:
	case <-time.After(2 * time.Second):
		t.Fatal("no worker ack within 2s")
	}
}

// completeOne reads one Result and waits for its worker's ack.
func (tr *testRunner) completeOne(t *testing.T) Result {
	t.Helper()
	r := recv(t, tr.Results())
	tr.awaitAck(t)
	return r
}

// firstSlot returns the wall-clock slot at which target URL first fires under
// interval iv from the fake epoch: the same function the scheduler uses.
func firstSlot(url string, iv time.Duration) time.Time {
	return slotAfter(epoch, iv, phaseOf(url, iv))
}

// recv reads one Result within 2s or fails.
func recv(t *testing.T, ch <-chan Result) Result {
	t.Helper()
	select {
	case r, ok := <-ch:
		if !ok {
			t.Fatal("Results closed")
		}
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("no Result within 2s")
	}
	return Result{}
}

// recvCall reads one call within 2s or fails.
func recvCall(t *testing.T, ch <-chan call) call {
	t.Helper()
	select {
	case c := <-ch:
		return c
	case <-time.After(2 * time.Second):
		t.Fatal("no check call within 2s")
	}
	return call{}
}

// expectNoCall asserts nothing arrives on ch for a short real-time window.
func expectNoCall(t *testing.T, ch <-chan call) {
	t.Helper()
	select {
	case c := <-ch:
		t.Fatalf("unexpected check call for %s at %v", c.url, c.at)
	case <-time.After(30 * time.Millisecond):
	}
}

// expectNoResult asserts nothing arrives on ch for a short real-time window.
func expectNoResult(t *testing.T, ch <-chan Result) {
	t.Helper()
	select {
	case r := <-ch:
		t.Fatalf("unexpected Result for %s: %v", r.Target.URL, r.Err)
	case <-time.After(30 * time.Millisecond):
	}
}

func targetsN(n int, prefix string) []check.Target {
	ts := make([]check.Target, n)
	for i := range ts {
		u := prefix + "/" + strconv.Itoa(i)
		ts[i] = check.Target{URL: u, Labels: map[string]string{"url": u}}
	}
	return ts
}
