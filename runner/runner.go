// Package runner schedules a check.Check against a set of targets, each on its
// own interval, bounds concurrency with a fixed worker pool, and publishes the
// resulting CloudEvents to a sink.EventSink with bounded retry.
//
// # Design
//
// One scheduler goroutine owns a min-heap of entries keyed by next fire time
// and hands due entries to a pool of `concurrency` workers. Scheduler-owned
// goroutines are therefore O(concurrency), regardless of target count.
//
//	                  ┌──────────────────────────────────────────────────────┐
//	                  │  Runner.Run(ctx)                                     │
//	                  │                                                      │
//	New():            │   scheduler goroutine (sole owner of heap + flags)   │
//	entries[i] =      │   ┌───────────────────────────────────────────────┐  │
//	 {target,         │   │ timer := clock.NewTimer(0)  // ONE, reused     │  │
//	  interval,       │   │ loop:                                          │  │
//	  redacted,       │   │  drainDone()  // non-blocking: clear inflight  │  │
//	  phase =         │   │               // + started for every finished  │  │
//	  fnv1a64(URL)    │   │               // entry BEFORE judging due      │  │
//	  % interval,     │   │  e = heap.peek()  (empty → wait ctx only)      │  │
//	  next = epoch-   │   │  if e.next-now > e.interval → re-anchor (D19)  │  │
//	  aligned slot    │   │  timer.Reset(e.next-now); wait ── timer ──┐    │  │
//	  (see Phase),    │   │      ▲                                    │    │  │
//	  inflight=false, │   │      │ done ch ─ clear inflight/started, loop  │  │
//	  started=false}  │   │  if now < e.next → loop (early wake, re-arm)   │  │
//	                  │   │  pop e                                         │  │
//	                  │   │  if e.inflight → skipped++, results <-         │  │
//	                  │   │     {t, started ? ErrSkipped : ErrSkippedQueued}│ │
//	                  │   │  else e.inflight = true;                       │  │
//	                  │   │       select { work <- e | <-ctx.Done: return }│  │
//	                  │   │  e.next += k·interval  (k = ⌊(now-next)/iv⌋+1) │  │
//	                  │   │  heap.push(e)                                  │  │
//	                  │   └───────────────┬───────────────────────────────┘  │
//	                  │                   │ work chan (cap = len(entries))    │
//	                  │                   ▼                                   │
//	                  │   worker × concurrency:  for e := range work {        │
//	                  │       if ctx.Err() != nil { done <- e; continue }     │
//	                  │       e.started.Store(true)                           │
//	                  │       runOne(ctx, e)   // check → CloudEvent → Publish│
//	                  │       done <- e        // cap = len(entries): at most │
//	                  │       afterDone?()     // one ack per entry (test hook)│
//	                  │   }                                                   │
//	                  │                                                      │
//	                  │  ctx.Done: close(work) → wg.Wait() → close(results)  │
//	                  └──────────────────────────────────────────────────────┘
//
// Ownership: the heap, e.next and e.inflight are touched ONLY by the scheduler
// goroutine. Workers read e.t / e.redacted (immutable after New) and set
// e.started (atomic.Bool, read by the scheduler at judgment, cleared by it on
// done). There is no mutex in the runner: two atomics (per-entry `started`,
// runner-wide `lastDropWarn`) plus the `skipped`/`dropped` counters.
//
// # Phase
//
// Each target fires at a stable per-URL offset within its interval:
// phase = FNV-1a-64(URL) mod interval, anchored to the wall clock:
//
//	next = now.Truncate(interval) + phase; if next <= now { next += interval }
//
// A process restart or a consumer-side runner rebuild at any instant leaves
// every target on the slot it already had, so phase never drifts across
// restarts. Slots that fall inside downtime are lost (a runner cannot make up a
// slot it was not running for): for a restart shorter than one interval the gap
// between two observations of one target is at most two intervals. The first
// check of each target therefore happens up to one interval after Run starts,
// not immediately.
//
// # Skips
//
// A target is never run concurrently with itself. If a target comes due while
// its prior run is still in flight, that slot is skipped: Skipped() increments
// and a Result carrying ErrSkipped (prior run had started: the check is slow)
// or ErrSkippedQueued (prior run was still queued behind a saturated pool) is
// sent on Results(). The entry's next slot advances by whole intervals, so
// phase is kept. A stall of any length (host sleep, forward clock step) yields
// one run and one O(1) reschedule, never a skip flood.
//
//	      due & !inflight            worker dequeues           worker finishes,
//	IDLE ─────────────────▶ QUEUED ─────────────────▶ RUNNING ─────────────────▶ IDLE
//	 ▲   (inflight=true,      (started=true)              (done <- e; scheduler
//	 │    work <- e)             │                          clears inflight+started)
//	 │                           │
//	 │  due & inflight & !started ─▶ results <- ErrSkippedQueued  ("pool too small")
//	 │  due & inflight &  started ─▶ results <- ErrSkipped        ("check is slow")
//	 └── either way: skipped++, next += k·interval, phase kept
//
// # Accounting
//
// Per target: completed runs + ErrSkipped + ErrSkippedQueued + dropped == slots
// the scheduler processed (a coalesced stall is one processed slot). Dropped()
// counts Results discarded because Results() was full; it is the only path a
// Result can take that is not on the channel, and it is counted and warned once
// per runner-default interval.
package runner

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fairbearlab/descry/check"
	"github.com/fairbearlab/descry/event"
	"github.com/fairbearlab/descry/sink"
)

const (
	// maxPublishAttempts is the total number of Publish tries per observation.
	maxPublishAttempts = 3
	// basePublishBackoff is multiplied by the attempt number for linear back-off.
	basePublishBackoff = 100 * time.Millisecond
)

// ErrSkipped is reported on Results when a target's slot is skipped because
// its prior run was still in flight and had already started: the check itself
// is slower than the target's interval. Test with errors.Is; it also matches
// ErrSkippedQueued.
var ErrSkipped = errors.New("runner: slot skipped; prior run still in flight")

// ErrSkippedQueued is reported on Results when a target's slot is skipped
// because its prior run was still queued behind a saturated worker pool and had
// not started: the pool is too small for the workload. It wraps ErrSkipped, so
// errors.Is(err, ErrSkipped) is true for both; check ErrSkippedQueued first
// when classifying.
var ErrSkippedQueued = fmt.Errorf("%w: prior run queued behind a saturated pool", ErrSkipped)

// Result is the outcome of a single scheduled slot: a completed run (Err nil or
// the check/publish error), or a skipped slot (Err is ErrSkipped or
// ErrSkippedQueued).
type Result struct {
	Target check.Target
	Err    error
}

// clock and timer are the scheduler's only sources of time. realClock is the
// production implementation; tests inject a fake through Runner.clock.
type clock interface {
	Now() time.Time
	NewTimer(d time.Duration) timer
}

type timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

type realClock struct{}

func (realClock) Now() time.Time                 { return time.Now() }
func (realClock) NewTimer(d time.Duration) timer { return realTimer{time.NewTimer(d)} }

type realTimer struct{ *time.Timer }

func (t realTimer) C() <-chan time.Time { return t.Timer.C }

// entry is one scheduled target. Fields under "scheduler-owned" are read and
// written only by the scheduler goroutine.
type entry struct {
	t        check.Target
	interval time.Duration
	phase    time.Duration // FNV-1a(URL) mod interval
	redacted string        // check.RedactURL(t.URL), precomputed for the log paths

	// scheduler-owned
	next     time.Time
	inflight bool

	// started is set by the worker on dequeue and cleared by the scheduler when
	// it receives the entry's done ack. It decides ErrSkipped vs ErrSkippedQueued.
	started atomic.Bool
}

// schedHeap is a min-heap of entries keyed by next fire time.
type schedHeap []*entry

func (h schedHeap) Len() int           { return len(h) }
func (h schedHeap) Less(i, j int) bool { return h[i].next.Before(h[j].next) }
func (h schedHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *schedHeap) Push(x any)        { *h = append(*h, x.(*entry)) }
func (h *schedHeap) Pop() any          { old := *h; n := len(old); e := old[n-1]; *h = old[:n-1]; return e }

// Runner schedules checks on per-target intervals, bounds concurrency with a
// fixed worker pool, and publishes CloudEvents to an EventSink.
type Runner struct {
	chk         check.Check
	sink        sink.EventSink
	evtCfg      event.Config
	entries     []*entry
	interval    time.Duration // default for targets with Interval <= 0; also the drop-warn rate-limit window
	concurrency int
	results     chan Result

	skipped      atomic.Int64
	dropped      atomic.Int64
	lastDropWarn atomic.Int64 // unix nanos of the last "results channel full" warning; 0 = never

	clock clock

	// afterDone, when non-nil, is called by a worker immediately after it has
	// sent an entry's done ack. It exists so tests can observe "the ack is on
	// the channel" deterministically. Always nil in production.
	afterDone func()
}

// New creates a Runner. interval is the default cadence for targets whose
// Interval is <= 0 and must be > 0 (New panics otherwise). concurrency < 1 is
// treated as 1.
func New(chk check.Check, s sink.EventSink, evtCfg event.Config, targets []check.Target,
	interval time.Duration, concurrency int) *Runner {
	if interval <= 0 {
		panic(fmt.Sprintf("runner.New: default interval must be > 0, got %v", interval))
	}
	if concurrency < 1 {
		concurrency = 1
	}
	entries := make([]*entry, len(targets))
	for i, t := range targets {
		iv := t.Interval
		if iv <= 0 {
			iv = interval
		}
		entries[i] = &entry{
			t:        t,
			interval: iv,
			phase:    phaseOf(t.URL, iv),
			redacted: check.RedactURL(t.URL),
		}
	}
	return &Runner{
		chk:         chk,
		sink:        s,
		evtCfg:      evtCfg,
		entries:     entries,
		interval:    interval,
		concurrency: concurrency,
		// Sized for the steady state: one completion + one skip outstanding per
		// target. This is a heuristic, not an exactness claim — a check spanning
		// k intervals has k skips + 1 completion outstanding, and a consumer that
		// stops draining overflows it by design; that loss is counted in
		// Dropped() and warned once per default interval.
		results: make(chan Result, 2*len(targets)+1),
		clock:   realClock{},
	}
}

// phaseOf returns the target's stable offset within its interval:
// FNV-1a-64(url) mod interval. FNV (not hash/maphash) because it is
// deterministic across processes, which is what makes cadence restart-invariant.
func phaseOf(url string, interval time.Duration) time.Duration {
	h := fnv.New64a()
	_, _ = h.Write([]byte(url))
	return time.Duration(h.Sum64() % uint64(interval)) // #nosec G115 -- interval > 0, result < interval
}

// slotAfter returns the first wall-clock slot for (interval, phase) strictly
// after now: now.Truncate(interval) + phase, bumped by one interval if that is
// not in the future.
func slotAfter(now time.Time, interval, phase time.Duration) time.Time {
	next := now.Truncate(interval).Add(phase)
	if !next.After(now) {
		next = next.Add(interval)
	}
	return next
}

// Results returns the results channel. Callers should drain it: every
// completed run and every skipped slot produces one Result, and a full channel
// drops (see Dropped).
func (r *Runner) Results() <-chan Result { return r.results }

// Skipped returns the number of slots skipped because the target's prior run
// was still in flight (both ErrSkipped and ErrSkippedQueued kinds).
func (r *Runner) Skipped() int64 { return r.skipped.Load() }

// Dropped returns the number of Results discarded because Results() was full,
// i.e. the consumer was not draining. Mirrors Skipped.
func (r *Runner) Dropped() int64 { return r.dropped.Load() }

// Run starts the scheduler and worker pool and blocks until ctx is cancelled.
// Each target first fires at its phase offset within its interval (up to one
// interval after Run starts), then every interval on the same wall-clock slot.
// Returns ctx.Err() when ctx is cancelled — this is not considered fatal.
//
// On shutdown it stops dispatching, waits for all in-flight runs to finish, then
// closes the results channel so a draining consumer can exit cleanly. Entries
// that were queued but not yet started are acked, not run. Waiting for in-flight
// runs also guarantees no Publish races with a deferred sink Close.
func (r *Runner) Run(ctx context.Context) error {
	work := make(chan *entry, len(r.entries))
	// done is sized so a worker's ack never blocks: an entry is dispatched only
	// while !inflight, and inflight clears only when the scheduler receives the
	// ack, so at most one ack per entry is ever outstanding.
	done := make(chan *entry, len(r.entries))

	var wg sync.WaitGroup
	for range r.concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.worker(ctx, work, done)
		}()
	}

	err := r.schedule(ctx, work, done)
	close(work)
	wg.Wait()
	close(r.results)
	return err
}

// schedule is the scheduler goroutine's loop; it is the sole owner of the heap
// and of every entry's next/inflight.
func (r *Runner) schedule(ctx context.Context, work chan<- *entry, done <-chan *entry) error {
	now := r.clock.Now()
	h := make(schedHeap, len(r.entries))
	for i, e := range r.entries {
		e.next = slotAfter(now, e.interval, e.phase)
		h[i] = e
	}
	heap.Init(&h)

	if len(h) == 0 {
		<-ctx.Done()
		return ctx.Err()
	}

	tm := r.clock.NewTimer(time.Hour) // one timer, reused; armed for real on the first lap
	defer tm.Stop()

	for {
		// Drain every finished ack before judging anything due, so a target
		// whose worker acked before the timer fired is never a false skip.
		r.drainDone(done)

		e := h[0]
		now = r.clock.Now()

		// Re-anchor guard: a backward wall-clock step leaves next more than one
		// interval away. The heap top is the earliest entry, so if it is stale
		// the step has happened; recompute every stale entry's epoch slot in
		// one pass so the stall is bounded to one interval in either direction,
		// and say so once per step.
		if e.next.Sub(now) > e.interval {
			n := 0
			for _, x := range h {
				if x.next.Sub(now) > x.interval {
					x.next = slotAfter(now, x.interval, x.phase)
					n++
				}
			}
			heap.Init(&h)
			slog.Info("clock stepped back; re-anchored schedule", "targets", n)
			continue
		}

		if wait := e.next.Sub(now); wait > 0 {
			tm.Reset(wait)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case de := <-done:
				r.finish(de)
			case <-tm.C():
				// Loop back: re-read now; an early wake (now < e.next) simply re-arms.
			}
			continue
		}

		// e is due.
		if e.inflight {
			r.skipped.Add(1)
			err := ErrSkippedQueued
			if e.started.Load() {
				err = ErrSkipped
			}
			// LogAttrs with a pre-redacted string: the disabled path boxes nothing.
			slog.LogAttrs(ctx, slog.LevelDebug, "skipping slot; prior run in flight",
				slog.String("url", e.redacted))
			r.reportResult(Result{Target: e.t, Err: err})
		} else {
			e.inflight = true
			select {
			case work <- e:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Advance by whole intervals so phase is kept; O(1) however long the
		// stall (a clock jump of hours is one run and one reschedule).
		k := (now.Sub(e.next) / e.interval) + 1
		e.next = e.next.Add(k * e.interval)
		heap.Fix(&h, 0)
	}
}

// drainDone consumes every ack currently on done without blocking.
func (r *Runner) drainDone(done <-chan *entry) {
	for {
		select {
		case e := <-done:
			r.finish(e)
		default:
			return
		}
	}
}

// finish records that e's run (or its cancelled dispatch) is over.
func (r *Runner) finish(e *entry) {
	e.inflight = false
	e.started.Store(false)
}

// worker runs entries from work until it is closed. Once ctx is cancelled it
// acks queued entries without running them, so shutdown produces no burst of
// context.Canceled Results.
func (r *Runner) worker(ctx context.Context, work <-chan *entry, done chan<- *entry) {
	for e := range work {
		if ctx.Err() == nil {
			e.started.Store(true)
			r.runOne(ctx, e.t)
		}
		done <- e
		if r.afterDone != nil {
			r.afterDone()
		}
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
	for attempt := 0; attempt < maxPublishAttempts; attempt++ {
		if pubErr = r.sink.Publish(ctx, e); pubErr == nil {
			break
		}
		if attempt == maxPublishAttempts-1 {
			break // final attempt failed; don't back off with no retry left
		}
		select {
		case <-ctx.Done():
			break backoff
		case <-time.After(time.Duration(attempt+1) * basePublishBackoff):
		}
	}
	if pubErr != nil {
		slog.Error("publish failed after retries", "url", check.RedactURL(t.URL), "err", pubErr)
	}
	r.reportResult(Result{Target: t, Err: pubErr})
}

// reportResult is best-effort. Results are useful for diagnostics, but a slow
// or absent consumer must not block scheduler progress or shutdown. A drop is
// counted in Dropped() and warned at most once per runner-default interval
// (no mutex; one atomic CAS on the last-warn timestamp).
func (r *Runner) reportResult(res Result) {
	select {
	case r.results <- res:
		return
	default:
	}
	r.dropped.Add(1)
	now := r.clock.Now().UnixNano()
	last := r.lastDropWarn.Load()
	if (last == 0 || now-last >= int64(r.interval)) && r.lastDropWarn.CompareAndSwap(last, now) {
		slog.Warn("dropping results; results channel full (consumer not draining)",
			"url", check.RedactURL(res.Target.URL), "err", res.Err, "dropped", r.dropped.Load())
	}
}
