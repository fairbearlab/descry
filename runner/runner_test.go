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
	"github.com/fairbearlab/descry/sink"
)

// --- fake check helpers ---

// instantCheck returns an observation immediately.
type instantCheck struct {
	status check.Status
}

func (c *instantCheck) Name() string { return "instant" }
func (c *instantCheck) Run(_ context.Context, t check.Target) (check.Observation, error) {
	return check.Observation{
		Status:     c.status,
		ObservedAt: time.Now().UTC(),
		Labels:     t.Labels,
		Extra:      map[string]any{},
	}, nil
}

// blockingCheck blocks on a channel until released. Used to force in-flight overlap.
type blockingCheck struct {
	gate  chan struct{} // close to unblock all pending runs
	calls atomic.Int64
}

func (c *blockingCheck) Name() string { return "blocking" }
func (c *blockingCheck) Run(ctx context.Context, t check.Target) (check.Observation, error) {
	c.calls.Add(1)
	select {
	case <-c.gate:
	case <-ctx.Done():
	}
	return check.Observation{
		Status:     check.StatusUp,
		ObservedAt: time.Now().UTC(),
		Labels:     t.Labels,
		Extra:      map[string]any{},
	}, nil
}

// countingCheck records the peak concurrency across calls.
type countingCheck struct {
	mu      sync.Mutex
	current int
	peak    int
	total   atomic.Int64
}

func (c *countingCheck) Name() string { return "counting" }
func (c *countingCheck) Run(_ context.Context, t check.Target) (check.Observation, error) {
	c.total.Add(1)
	c.mu.Lock()
	c.current++
	if c.current > c.peak {
		c.peak = c.current
	}
	c.mu.Unlock()

	time.Sleep(20 * time.Millisecond) // hold concurrency slot briefly

	c.mu.Lock()
	c.current--
	c.mu.Unlock()

	return check.Observation{
		Status:     check.StatusUp,
		ObservedAt: time.Now().UTC(),
		Labels:     t.Labels,
		Extra:      map[string]any{},
	}, nil
}

// --- fake sink ---

// discardSink drops all events (concurrency-safe no-op).
type discardSink struct{}

func (discardSink) Publish(_ context.Context, _ cloudevents.Event) error { return nil }

// ensure discardSink satisfies EventSink at compile time
var _ sink.EventSink = discardSink{}

// flakySink fails its first `failsLeft` Publish calls, then succeeds. Used to
// exercise the bounded-retry path in runOne.
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

// --- tests ---

// TestSkipTick_IncrementsCounter verifies that when a check run is still
// in-flight at the next tick, the tick is skipped and Skipped() grows.
func TestSkipTick_IncrementsCounter(t *testing.T) {
	gate := make(chan struct{})
	chk := &blockingCheck{gate: gate}

	target := check.Target{URL: "http://example.com", Labels: map[string]string{"url": "http://example.com"}}
	r := New(chk, discardSink{}, event.Config{Source: "test"}, []check.Target{target},
		10*time.Millisecond, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go func() { _ = r.Run(ctx) }()

	// Wait until we see at least one skip, or ctx expires.
	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		if r.Skipped() > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	close(gate) // unblock all in-flight runs
	<-ctx.Done()

	if r.Skipped() == 0 {
		t.Error("expected Skipped() > 0, got 0")
	}
}

// TestBoundedPool_RespectsCap verifies that the runner never exceeds the
// configured concurrency limit, even with many targets.
func TestBoundedPool_RespectsCap(t *testing.T) {
	const numTargets = 20
	const maxConcurrency = 3

	chk := &countingCheck{}
	targets := make([]check.Target, numTargets)
	for i := range targets {
		targets[i] = check.Target{URL: "http://example.com", Labels: map[string]string{"url": "http://example.com"}}
	}

	r := New(chk, discardSink{}, event.Config{Source: "test"}, targets,
		50*time.Millisecond, // first fire is at each target's phase offset, ≤ one interval in
		maxConcurrency)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() { _ = r.Run(ctx) }()

	// Wait for all targets to have run at least once.
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if chk.total.Load() >= int64(numTargets) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()

	chk.mu.Lock()
	peak := chk.peak
	chk.mu.Unlock()

	if peak > maxConcurrency {
		t.Errorf("peak concurrency = %d, want <= %d", peak, maxConcurrency)
	}
}

// TestPerCheckTimeout verifies that a context cancellation propagates into the
// check's Run call, allowing long-running checks to be interrupted.
func TestPerCheckTimeout(t *testing.T) {
	gate := make(chan struct{}) // never closed — forces ctx cancellation to fire
	chk := &blockingCheck{gate: gate}

	target := check.Target{URL: "http://example.com", Labels: map[string]string{"url": "http://example.com"}}
	r := New(chk, discardSink{}, event.Config{Source: "test"}, []check.Target{target},
		10*time.Millisecond, // first fire within 10ms; the check then blocks until ctx expires
		1)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_ = r.Run(ctx)
	elapsed := time.Since(start)

	// The runner should have returned promptly after ctx expired.
	if elapsed > 500*time.Millisecond {
		t.Errorf("Run took %v, expected < 500ms after ctx cancel", elapsed)
	}
}

// TestRunOne_RetriesThenSucceeds verifies the bounded-retry path: two transient
// Publish failures followed by success yields a nil Result.Err and exactly 3
// Publish calls.
func TestRunOne_RetriesThenSucceeds(t *testing.T) {
	s := &flakySink{}
	s.failsLeft.Store(2) // fail twice, succeed on the 3rd attempt
	tgt := check.Target{URL: "http://x", Labels: map[string]string{"url": "http://x"}}
	r := New(&instantCheck{status: check.StatusUp}, s, event.Config{Source: "t"},
		[]check.Target{tgt}, time.Second, 1) // one slot fires and finishes (300ms of back-off) before the next

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	res := <-r.Results()
	cancel() // no second slot may dispatch before we read the call count
	if res.Err != nil {
		t.Fatalf("Err = %v, want nil after retry success", res.Err)
	}
	if got := s.calls.Load(); got != 3 {
		t.Fatalf("Publish calls = %d, want 3", got)
	}
}

// TestRunOne_RetriesExhausted verifies that when every attempt fails the runner
// makes exactly maxPublishAttempts tries and reports the error on Results.
func TestRunOne_RetriesExhausted(t *testing.T) {
	s := &alwaysFailSink{}
	tgt := check.Target{URL: "http://x", Labels: map[string]string{"url": "http://x"}}
	r := New(&instantCheck{status: check.StatusUp}, s, event.Config{Source: "t"},
		[]check.Target{tgt}, time.Second, 1) // one slot fires and finishes (300ms of back-off) before the next

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	res := <-r.Results()
	cancel() // no second slot may dispatch before we read the call count
	if res.Err == nil {
		t.Fatal("Err = nil, want non-nil after exhausted retries")
	}
	if got := s.calls.Load(); got != maxPublishAttempts {
		t.Fatalf("Publish calls = %d, want %d", got, maxPublishAttempts)
	}
}

// TestRun_ShutdownDoesNotRequireResultsDrain verifies that diagnostics are
// best-effort: callers can ignore Results without wedging shutdown.
func TestRun_ShutdownDoesNotRequireResultsDrain(t *testing.T) {
	target := check.Target{URL: "http://example.com", Labels: map[string]string{"url": "http://example.com"}}
	r := New(&instantCheck{status: check.StatusUp}, discardSink{}, event.Config{Source: "test"},
		[]check.Target{target}, time.Millisecond, 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	time.Sleep(25 * time.Millisecond) // enough ticks to fill the small results buffer
	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not shut down when Results was not drained")
	}
}
