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
	chk     check.Check
	sink    sink.EventSink
	evtCfg  event.Config
	targets []*target
	interval time.Duration
	sem     chan struct{} // buffered-channel semaphore
	results chan Result
	skipped atomic.Int64
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
func (r *Runner) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	r.tick(ctx) // fire immediately on start
	for {
		select {
		case <-ctx.Done():
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
		r.sem <- struct{}{} // acquire semaphore slot (blocks if pool is full)
		go func(tg *target) {
			defer func() {
				<-r.sem      // release semaphore slot
				tg.mu.Unlock() // mark this target as no longer in-flight
			}()
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
		r.results <- Result{Target: t, Err: err}
		return
	}
	e, err := event.ToCloudEvent(obs, r.evtCfg)
	if err != nil {
		r.results <- Result{Target: t, Err: err}
		return
	}

	// Best-effort produce: up to 3 attempts with short back-off.
	var pubErr error
	for attempt := 0; attempt < 3; attempt++ {
		if pubErr = r.sink.Publish(ctx, e); pubErr == nil {
			break
		}
		time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
	}
	if pubErr != nil {
		slog.Error("publish failed after retries", "url", t.URL, "err", pubErr)
	}
	r.results <- Result{Target: t, Err: pubErr}
}
