package metrics

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
	"github.com/fairbearlab/descry/runner"
)

// blockingCheck holds every run until its ctx is done, so a runner using it
// skips slots and, with nobody draining, drops Results.
type blockingCheck struct{}

func (blockingCheck) Name() string { return "blocking" }

func (blockingCheck) Run(ctx context.Context, _ check.Target) (check.Observation, error) {
	<-ctx.Done()
	return check.Observation{}, ctx.Err()
}

type nopSink struct{}

func (nopSink) Publish(context.Context, cloudevents.Event) error { return nil }

// startSaturated runs a real Runner that saturates at once: two 1ms targets,
// one worker and a check that never finishes, so one target's slots skip as
// in-flight and the other's as queued. The runner-default interval is long
// so the drop warning logs once. The returned stop cancels Run and waits for
// it to return (Results() is closed by then); cleanup calls it too.
func startSaturated(t *testing.T) (*runner.Runner, func()) {
	t.Helper()
	targets := []check.Target{
		{URL: "https://a.example/", Interval: time.Millisecond},
		{URL: "https://b.example/", Interval: time.Millisecond},
	}
	r := runner.New(blockingCheck{}, nopSink{}, event.Config{Source: "descry/metrics-test"}, targets, time.Hour, 1)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	stop := sync.OnceFunc(func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("Run = %v, want context.Canceled", err)
		}
	})
	t.Cleanup(stop)
	return r, stop
}

// TestCounterFuncs_RealRunner proves the CounterFuncs read the runner's exact
// counters at gather time, with real (non-zero) skips and drops.
func TestCounterFuncs_RealRunner(t *testing.T) {
	r, _ := startSaturated(t)
	c, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	c.SetRunner(r)
	waitFor(t, "real skips and drops", func() bool { return r.Skipped() > 0 && r.Dropped() > 0 })

	skippedBefore, droppedBefore := r.Skipped(), r.Dropped()
	got := samples(t, c)
	skippedAfter, droppedAfter := r.Skipped(), r.Dropped()

	if s := got["descry_runner_skipped_slots_total"]; s < float64(skippedBefore) || s > float64(skippedAfter) {
		t.Errorf("skipped_slots_total = %v, want within [%d, %d]", s, skippedBefore, skippedAfter)
	}
	if d := got["descry_runner_dropped_results_total"]; d < float64(droppedBefore) || d > float64(droppedAfter) {
		t.Errorf("dropped_results_total = %v, want within [%d, %d]", d, droppedBefore, droppedAfter)
	}
}

// TestSkipsObserved_Saturated proves the best-effort skip series is bounded by
// the exact ones when Results() overflows: observed <= skipped, and the
// undercount skipped - observed is at most dropped. The drain mirrors the
// CLI's: ErrSkippedQueued is tested before ErrSkipped, which it wraps.
func TestSkipsObserved_Saturated(t *testing.T) {
	r, stop := startSaturated(t)
	var resultErrs atomic.Int64
	c, err := New(Options{ResultErrors: resultErrs.Load})
	if err != nil {
		t.Fatal(err)
	}
	c.SetRunner(r)

	gate := make(chan struct{})
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		<-gate
		for res := range r.Results() {
			switch {
			case errors.Is(res.Err, runner.ErrSkippedQueued):
				c.ObserveSkip(SkipQueued)
			case errors.Is(res.Err, runner.ErrSkipped):
				c.ObserveSkip(SkipInFlight)
			case res.Err != nil:
				resultErrs.Add(1)
			}
		}
	}()

	// Let Results() overflow before draining, then drain until both kinds
	// have been seen.
	waitFor(t, "dropped results", func() bool { return r.Dropped() > 0 })
	close(gate)
	waitFor(t, "both skip kinds observed", func() bool {
		got := samples(t, c)
		return got[`descry_runner_skips_observed_total{kind="queued"}`] > 0 &&
			got[`descry_runner_skips_observed_total{kind="in_flight"}`] > 0
	})
	stop()
	<-drained

	got := samples(t, c)
	observed := got[`descry_runner_skips_observed_total{kind="queued"}`] + got[`descry_runner_skips_observed_total{kind="in_flight"}`]
	skipped := got["descry_runner_skipped_slots_total"]
	dropped := got["descry_runner_dropped_results_total"]
	if dropped == 0 {
		t.Fatal("no dropped results; the saturation case did not happen")
	}
	if observed > skipped {
		t.Errorf("observed skips %v > skipped slots %v", observed, skipped)
	}
	if skipped-observed > dropped {
		t.Errorf("skipped %v - observed %v > dropped %v", skipped, observed, dropped)
	}

	// result_errors_total reads Options.ResultErrors live at every gather.
	want(t, got, "descry_runner_result_errors_total", float64(resultErrs.Load()))
	resultErrs.Add(5)
	want(t, samples(t, c), "descry_runner_result_errors_total", float64(resultErrs.Load()))
}
