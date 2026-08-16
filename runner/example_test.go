package runner_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/fairbearlab/descry/check"
	"github.com/fairbearlab/descry/checks/http"
	"github.com/fairbearlab/descry/event"
	"github.com/fairbearlab/descry/runner"
)

// ExampleErrSkipped shows how a consumer draining Results classifies a skipped
// slot. Check ErrSkippedQueued first: it wraps ErrSkipped, so errors.Is(err,
// ErrSkipped) is true for both kinds.
func ExampleErrSkipped() {
	classify := func(res runner.Result) string {
		switch {
		case errors.Is(res.Err, runner.ErrSkippedQueued):
			return "skipped: prior run still queued (pool too small)"
		case errors.Is(res.Err, runner.ErrSkipped):
			return "skipped: prior run still running (slow check)"
		case res.Err != nil:
			return "failed: " + res.Err.Error()
		default:
			return "ok"
		}
	}
	for _, res := range []runner.Result{
		{Err: nil},
		{Err: runner.ErrSkipped},
		{Err: runner.ErrSkippedQueued},
	} {
		fmt.Println(classify(res))
	}
	// Output:
	// ok
	// skipped: prior run still running (slow check)
	// skipped: prior run still queued (pool too small)
}

type discardSink struct{}

func (discardSink) Publish(context.Context, cloudevents.Event) error { return nil }

// ExampleNew_perTargetIntervals wires two targets on different cadences into
// one Runner. Target.Interval of 0 means "use the runner default"; each target
// first fires at a stable per-URL offset within its interval, then every
// interval on the same wall-clock slot.
func ExampleNew_perTargetIntervals() {
	targets := []check.Target{
		{URL: "https://example.com/", Interval: 30 * time.Second},
		{URL: "https://example.org/", Interval: 5 * time.Minute},
		{URL: "https://example.net/"}, // 0 → runner default below
	}
	r := runner.New(http.New(5*time.Second), discardSink{}, event.Config{Source: "example"},
		targets, time.Minute, 4)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // a real program would run until shutdown
	go func() {
		for res := range r.Results() {
			if errors.Is(res.Err, runner.ErrSkipped) {
				fmt.Println("skipped:", res.Target.URL)
			}
		}
	}()
	_ = r.Run(ctx)
}
