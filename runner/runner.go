package runner

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fairbearlab/descry/check"
	"github.com/fairbearlab/descry/event"
	"github.com/fairbearlab/descry/sink"
)

// Result is the outcome of a single check run.
type Result struct {
	Target check.Target
	Err    error
}

// target wraps a check.Target with a per-target mutex for skip-tick detection.
type target struct {
	t  check.Target
	mu sync.Mutex // held while a run is in-flight
}

// Runner schedules checks on a ticker, enforces concurrency limits, and
// publishes CloudEvents to an EventSink.
type Runner struct {
	chk      check.Check
	sink     sink.EventSink
	evtCfg   event.Config
	targets  []*target
	interval time.Duration
	sem      chan struct{} // buffered-channel semaphore
	results  chan Result
	skipped  atomic.Int64
	wg       sync.WaitGroup // tracks in-flight runOne goroutines
}

// New creates a Runner. concurrency must be >= 1.
func New(chk check.Check, s sink.EventSink, evtCfg event.Config, targets []check.Target,
	interval time.Duration, concurrency int) *Runner {
	if concurrency < 1 {
		concurrency = 1
	}
	ts := make([]*target, len(targets))
	for i, t := range targets {
		ts[i] = &target{t: t}
	}
	return &Runner{
		chk:      chk,
		sink:     s,
		evtCfg:   evtCfg,
		targets:  ts,
		interval: interval,
		sem:      make(chan struct{}, concurrency),
		results:  make(chan Result, len(targets)+1),
	}
}

// Results returns the results channel. Callers should drain it.
func (r *Runner) Results() <-chan Result { return r.results }

// Skipped returns the number of ticks skipped due to in-flight runs.
func (r *Runner) Skipped() int64 { return r.skipped.Load() }

// Run starts the ticker loop. It fires once immediately, then on each tick.
// Returns ctx.Err() when ctx is cancelled — this is not considered fatal.
//
// On shutdown it stops dispatching, waits for all in-flight runs to finish, then
// closes the results channel so a draining consumer can exit cleanly. Waiting for
// in-flight runs also guarantees no Publish races with a deferred sink Close.
func (r *Runner) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	r.tick(ctx) // fire immediately on start
	for {
		select {
		case <-ctx.Done():
			r.wg.Wait()
			close(r.results)
			return ctx.Err()
		case <-ticker.C:
			r.tick(ctx)
		}
	}
}

// tick dispatches all targets for one scheduler round. If a target's prior run
// is still in-flight (TryLock fails), the tick is skipped and the counter is
// incremented.
func (r *Runner) tick(ctx context.Context) {
	for _, tg := range r.targets {
		if !tg.mu.TryLock() {
			slog.Warn("skipping tick; prior run in flight", "url", tg.t.URL)
			r.skipped.Add(1)
			continue
		}
		r.wg.Add(1)
		go func(tg *target) {
			defer func() {
				tg.mu.Unlock() // mark this target as no longer in-flight
				r.wg.Done()
			}()
			// Acquire a semaphore slot inside the goroutine so a full pool never
			// blocks tick dispatch. Bail out if shutdown beats us to the slot.
			select {
			case r.sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-r.sem }() // release semaphore slot
			r.runOne(ctx, tg.t)
		}(tg)
	}
}

// runOne executes a single check, maps it to a CloudEvent, and publishes it
// with bounded retry. Errors are reported on the results channel but never
// block the scheduler.
func (r *Runner) runOne(ctx context.Context, t check.Target) {
	obs, err := r.chk.Run(ctx, t)
	if err != nil {
		r.reportResult(Result{Target: t, Err: err})
		return
	}
	e, err := event.ToCloudEvent(obs, r.evtCfg)
	if err != nil {
		r.reportResult(Result{Target: t, Err: err})
		return
	}

	// Best-effort produce: up to 3 attempts with short back-off. The back-off is
	// ctx-aware so shutdown isn't delayed by a sleeping goroutine.
	var pubErr error
backoff:
	for attempt := 0; attempt < 3; attempt++ {
		if pubErr = r.sink.Publish(ctx, e); pubErr == nil {
			break
		}
		if attempt == 2 {
			break // final attempt failed; don't back off with no retry left
		}
		select {
		case <-ctx.Done():
			break backoff
		case <-time.After(time.Duration(attempt+1) * 100 * time.Millisecond):
		}
	}
	if pubErr != nil {
		slog.Error("publish failed after retries", "url", t.URL, "err", pubErr)
	}
	r.reportResult(Result{Target: t, Err: pubErr})
}

// reportResult is best-effort. Results are useful for diagnostics, but a slow
// or absent consumer must not block scheduler progress or shutdown.
func (r *Runner) reportResult(res Result) {
	select {
	case r.results <- res:
	default:
		slog.Warn("dropping result; results channel full", "url", res.Target.URL, "err", res.Err)
	}
}
