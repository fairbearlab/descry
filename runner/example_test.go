package runner_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
	if err := r.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Println("run:", err) // e.g. an invalid event config
	}
}

// verdictBook is an event consumer that treats the absence of a fresh event
// as unknown: it keeps each subject's last verdict with the event's time and
// stops trusting it once it is older than the subject's expected cadence. The
// runner calls Publish from several goroutines, so mu guards last.
type verdictBook struct {
	maxAge time.Duration // e.g. 2 × interval + timeout
	mu     sync.Mutex
	last   map[string]lastVerdict
}

type lastVerdict struct {
	status string
	at     time.Time
}

// Publish is the consumer's sink: it records the verdict an event carries.
func (b *verdictBook) Publish(_ context.Context, e cloudevents.Event) error {
	var data struct {
		Status string `json:"status"`
	}
	if err := e.DataAs(&data); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.last[e.Subject()] = lastVerdict{data.Status, e.Time()}
	return nil
}

// verdict is the subject's status at now: its last verdict while fresh, else
// "unknown". It never returns a verdict it has no current evidence for.
func (b *verdictBook) verdict(subject string, now time.Time) string {
	b.mu.Lock()
	v, ok := b.last[subject]
	b.mu.Unlock()
	if !ok || now.Sub(v.at) > b.maxAge {
		return "unknown"
	}
	return v.status
}

// Example_absenceIsUnknown shows a consumer that does not let a verdict
// outlive its evidence. A check that could not run (check.ErrNotRun) and a
// run cut short by shutdown publish no event, so a consumer that kept the last
// event forever would keep reporting a verdict nobody re-checked. Here the
// Results drain reports not-runs as a prober problem, and the event side
// expires each verdict after the target's expected cadence.
func Example_absenceIsUnknown() {
	const interval, timeout = time.Minute, 10 * time.Second
	book := &verdictBook{maxAge: 2*interval + timeout, last: map[string]lastVerdict{}}
	enc, err := event.NewEncoder(event.Config{Source: "example"})
	if err != nil {
		panic(err)
	}
	t0 := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	tgt := check.Target{URL: "https://example.com/", Labels: map[string]string{"url": "https://example.com/"}}

	// The runner publishes one up event for the target...
	e, err := enc.Encode(check.Observation{Status: check.StatusUp, StatusCode: 200, ObservedAt: t0, Labels: tgt.Labels})
	if err != nil {
		panic(err)
	}
	_ = book.Publish(context.Background(), e)

	// ...and then its checks cannot run: Results carries ErrNotRun and no
	// event is published.
	for _, res := range []runner.Result{
		{Target: tgt, Err: fmt.Errorf("%w: dial: too many open files", check.ErrNotRun)},
	} {
		switch {
		case errors.Is(res.Err, check.ErrNotRun):
			fmt.Println("prober problem, no verdict:", res.Target.URL)
		case errors.Is(res.Err, runner.ErrSkipped):
			fmt.Println("skipped:", res.Target.URL)
		case res.Err != nil:
			fmt.Println("failed:", res.Err)
		}
	}

	fmt.Println("at +1m:", book.verdict("https://example.com/", t0.Add(time.Minute)))
	fmt.Println("at +5m:", book.verdict("https://example.com/", t0.Add(5*time.Minute)))
	// Output:
	// prober problem, no verdict: https://example.com/
	// at +1m: up
	// at +5m: unknown
}
