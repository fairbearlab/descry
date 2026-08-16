package runner

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/fairbearlab/descry/check"
	"github.com/fairbearlab/descry/event"
)

// Scale harness (PERF-PLAN §3.5). Real clock, real goroutines: these measure
// what the scheduler does to a process at 10k targets. Every criterion is
// printed as PASS/FAIL; nothing is asserted here (D12: PR1 logs, PR2 gates in a
// dedicated CI job). Skipped under -short and under the race detector.
//
// Run: go test -run 'TestScale' -v ./runner/

// skipUnlessScale skips the scale harness under -short or the race detector.
func skipUnlessScale(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("scale harness: skipped under -short")
	}
	if raceEnabled {
		t.Skip("scale harness: skipped under the race detector")
	}
}

// slotTracker is the check wrapper that measures start-lateness on the
// monotonic clock (D30). For each URL it keeps the wall-clock slot the
// scheduler is expected to fire next (the same phase arithmetic the scheduler
// uses) and, on each run, records lateness = time.Since(t0) − slot.Sub(t0):
// the monotonic elapsed time minus the slot's fixed wall offset from t0, so
// wall-clock slew after t0 cannot enter the number. After a run the expected
// slot advances by whole intervals past now, mirroring the scheduler's
// catch-up rule, so a run that follows a skipped slot is measured against the
// slot it was dispatched for.
type slotTracker struct {
	inner    check.Check
	clock    *startClock
	interval time.Duration

	mu       sync.Mutex
	t0       time.Time // the scheduler's own start instant, from startClock
	expected map[string]time.Time
	starts   map[string][]time.Duration // monotonic start offsets per URL
	lateness []time.Duration
}

// startClock is the real clock plus a record of the scheduler's first Now():
// the instant every entry's first slot is computed from. The tracker seeds
// its expected slots from that same instant, so tracker and scheduler agree
// exactly on which slot each run belongs to.
type startClock struct {
	realClock
	once sync.Once
	t0   time.Time
}

func (c *startClock) Now() time.Time {
	now := time.Now()
	c.once.Do(func() { c.t0 = now })
	return now
}

func newSlotTracker(inner check.Check, sc *startClock, iv time.Duration) *slotTracker {
	return &slotTracker{inner: inner, clock: sc, interval: iv,
		expected: map[string]time.Time{}, starts: map[string][]time.Duration{}}
}

func (st *slotTracker) Name() string { return "slot-tracker" }

func (st *slotTracker) Run(ctx context.Context, t check.Target) (check.Observation, error) {
	st.mu.Lock()
	if st.t0.IsZero() {
		st.t0 = st.clock.t0 // set by the scheduler's first Now(), before any dispatch
	}
	elapsed := time.Since(st.t0) // monotonic
	exp, ok := st.expected[t.URL]
	if !ok {
		exp = slotAfter(st.t0, st.interval, phaseOf(t.URL, st.interval))
	}
	late := elapsed - exp.Sub(st.t0)
	st.lateness = append(st.lateness, late)
	st.starts[t.URL] = append(st.starts[t.URL], elapsed)
	// Advance to the first slot after "now" as the scheduler sees it.
	now := st.t0.Add(elapsed)
	if k := now.Sub(exp)/st.interval + 1; k > 0 {
		exp = exp.Add(k * st.interval)
	}
	st.expected[t.URL] = exp
	st.mu.Unlock()
	return st.inner.Run(ctx, t)
}

// percentiles returns p50/p90/p99/max of ds (sorted in place).
func percentiles(ds []time.Duration) (p50, p90, p99, maxD time.Duration) {
	if len(ds) == 0 {
		return 0, 0, 0, 0
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	p := func(q float64) time.Duration { return ds[int(float64(len(ds)-1)*q)] }
	return p(0.50), p(0.90), p(0.99), ds[len(ds)-1]
}

// regime is one §3.5 configuration.
type regime struct {
	name        string
	n           int
	interval    time.Duration
	checkD      time.Duration
	concurrency int
	duration    time.Duration
}

// regimeResult is what one run of a regime observed.
type regimeResult struct {
	completed, slow, queued int
	dropped                 int64
	skipped                 int64
	perURL                  map[string]int // Results per URL (completions + skips)
	idleGoroutines, peak    int
	heapDeltaKB             uint64
	elapsed                 time.Duration
	tracker                 *slotTracker
	tEnd                    time.Time
}

// runRegime executes one regime and collects everything the §3.5 table needs.
func runRegime(t *testing.T, rg regime) *regimeResult {
	t.Helper()
	targets := targetsN(rg.n, "https://example.com")
	inner := &fakeCheck{delay: rg.checkD}
	sc := &startClock{}
	tracker := newSlotTracker(inner, sc, rg.interval)

	var msBefore runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&msBefore)
	idle := runtime.NumGoroutine()

	r := New(tracker, nopSink{}, event.Config{Source: "scale"}, targets, rg.interval, rg.concurrency)
	r.clock = sc
	res := &regimeResult{perURL: make(map[string]int, rg.n), idleGoroutines: idle, tracker: tracker}

	// Sample goroutines every millisecond while Run is active.
	stopSample := make(chan struct{})
	sampled := make(chan int, 1)
	go func() {
		peak := 0
		tk := time.NewTicker(time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stopSample:
				sampled <- peak
				return
			case <-tk.C:
				if g := runtime.NumGoroutine(); g > peak {
					peak = g
				}
			}
		}
	}()

	// Drain Results and classify.
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for x := range r.Results() {
			res.perURL[x.Target.URL]++
			switch {
			case errors.Is(x.Err, ErrSkippedQueued):
				res.queued++
			case errors.Is(x.Err, ErrSkipped):
				res.slow++
			default:
				res.completed++
			}
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	runDone := make(chan struct{})
	go func() { _ = r.Run(ctx); close(runDone) }()
	time.Sleep(rg.duration)
	res.tEnd = time.Now()
	cancel()
	<-runDone
	res.elapsed = time.Since(start)
	<-drained
	close(stopSample)
	res.peak = <-sampled
	res.dropped = r.Dropped()
	res.skipped = r.Skipped()

	var msAfter runtime.MemStats
	runtime.ReadMemStats(&msAfter)
	if msAfter.HeapAlloc > msBefore.HeapAlloc {
		res.heapDeltaKB = (msAfter.HeapAlloc - msBefore.HeapAlloc) / 1024
	}
	return res
}

func pf(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}

// TestScale_Healthy: 10k targets, 10s interval, 20ms check, concurrency 64.
// §3.5 criteria: zero skips, Dropped()==0, scheduler-owned goroutines ≤
// concurrency+8; p99 start-lateness printed, never asserted.
func TestScale_Healthy(t *testing.T) {
	skipUnlessScale(t)
	rg := regime{name: "healthy", n: 10_000, interval: 10 * time.Second, checkD: 20 * time.Millisecond,
		concurrency: 64, duration: 11 * time.Second}
	res := runRegime(t, rg)
	p50, p90, p99, maxL := percentiles(res.tracker.lateness)
	schedGoroutines := res.peak - res.idleGoroutines

	t.Logf("[%s] n=%d interval=%v check=%v conc=%d ran=%v", rg.name, rg.n, rg.interval, rg.checkD, rg.concurrency, res.elapsed.Round(time.Millisecond))
	t.Logf("[%s] completed=%d skipped=%d (slow=%d queued=%d) dropped=%d", rg.name, res.completed, res.skipped, res.slow, res.queued, res.dropped)
	t.Logf("[%s] goroutines: idle=%d peak=%d scheduler-owned≈%d (bound conc+8=%d) heapΔ=%dKB", rg.name, res.idleGoroutines, res.peak, schedGoroutines, rg.concurrency+8, res.heapDeltaKB)
	t.Logf("[%s] start-lateness (monotonic, D30): p50=%v p90=%v p99=%v max=%v samples=%d", rg.name,
		p50.Round(time.Microsecond), p90.Round(time.Microsecond), p99.Round(time.Microsecond), maxL.Round(time.Microsecond), len(res.tracker.lateness))
	t.Logf("[%s] %s zero skips | %s Dropped()==0 | %s goroutines ≤ conc+8 | (lateness printed only)", rg.name,
		pf(res.skipped == 0), pf(res.dropped == 0), pf(schedGoroutines <= rg.concurrency+8))
}

// TestScale_Saturated: 500 targets, 100ms interval, 20ms check, concurrency 64
// (≈156ms of work per 100ms). §3.5 criteria: accounting identity per target
// (completed + ErrSkipped + ErrSkippedQueued + dropped == slots processed; with
// a real clock the boundary slot at each end is ±1), Dropped()==0, no
// starvation (every target runs at least once per two intervals), goroutines
// flat; p99 lateness of completed runs printed.
func TestScale_Saturated(t *testing.T) {
	skipUnlessScale(t)
	rg := regime{name: "saturated", n: 500, interval: 100 * time.Millisecond, checkD: 20 * time.Millisecond,
		concurrency: 64, duration: 2 * time.Second}
	res := runRegime(t, rg)
	p50, p90, p99, maxL := percentiles(res.tracker.lateness)
	schedGoroutines := res.peak - res.idleGoroutines

	// Accounting identity: per target, Results on the channel == slots the
	// scheduler processed. Expected slots are counted from the wall clock over
	// [t0, tEnd]; the first and last slot can straddle Run start / cancel, so
	// each target may differ by at most one at either end.
	identityOK, worstDelta := true, 0
	for url, got := range res.perURL {
		first := slotAfter(res.tracker.t0, rg.interval, phaseOf(url, rg.interval))
		want := 0
		if !first.After(res.tEnd) {
			want = int(res.tEnd.Sub(first)/rg.interval) + 1
		}
		d := got - want
		if d < 0 {
			d = -d
		}
		if d > worstDelta {
			worstDelta = d
		}
		if d > 1 {
			identityOK = false
		}
	}
	// No starvation: max gap between consecutive run starts per target.
	var maxGap time.Duration
	for _, starts := range res.tracker.starts {
		for i := 1; i < len(starts); i++ {
			if g := starts[i] - starts[i-1]; g > maxGap {
				maxGap = g
			}
		}
	}
	starveBound := 2*rg.interval + rg.checkD
	total := res.completed + res.slow + res.queued
	skipPct := 0.0
	if total > 0 {
		skipPct = 100 * float64(res.slow+res.queued) / float64(total)
	}

	t.Logf("[%s] n=%d interval=%v check=%v conc=%d ran=%v (work/interval ≈ %.0f%%)", rg.name, rg.n, rg.interval, rg.checkD, rg.concurrency,
		res.elapsed.Round(time.Millisecond), 100*float64(rg.n)*float64(rg.checkD)/float64(rg.concurrency)/float64(rg.interval))
	t.Logf("[%s] slots=%d completed=%d skipped=%d (slow=%d queued=%d, %.1f%%) dropped=%d", rg.name, total, res.completed, res.skipped, res.slow, res.queued, skipPct, res.dropped)
	t.Logf("[%s] identity: worst per-target |results − expected slots| = %d (boundary tolerance 1)", rg.name, worstDelta)
	t.Logf("[%s] starvation: max gap between a target's runs = %v (bound 2×interval+check = %v)", rg.name, maxGap.Round(time.Microsecond), starveBound)
	t.Logf("[%s] goroutines: idle=%d peak=%d scheduler-owned≈%d (bound conc+8=%d)", rg.name, res.idleGoroutines, res.peak, schedGoroutines, rg.concurrency+8)
	t.Logf("[%s] start-lateness of completed runs (monotonic): p50=%v p90=%v p99=%v max=%v", rg.name,
		p50.Round(time.Microsecond), p90.Round(time.Microsecond), p99.Round(time.Microsecond), maxL.Round(time.Microsecond))
	t.Logf("[%s] %s identity | %s Dropped()==0 | %s no starvation | %s goroutines flat | (lateness printed only)", rg.name,
		pf(identityOK), pf(res.dropped == 0), pf(maxGap <= starveBound), pf(schedGoroutines <= rg.concurrency+8))
}

// TestScale_Footprint sweeps 100 / 1k / 10k targets (20ms check, concurrency
// 64) for one interval and prints peak goroutines and heap growth — the
// before/after companion of the handoff's TestBurstShape table.
func TestScale_Footprint(t *testing.T) {
	skipUnlessScale(t)
	for _, n := range []int{100, 1_000, 10_000} {
		iv := 4 * time.Second // 10k × 20ms / 64 ≈ 3.1s of work fits inside it
		rg := regime{name: fmt.Sprintf("footprint-%d", n), n: n, interval: iv, checkD: 20 * time.Millisecond,
			concurrency: 64, duration: iv + 500*time.Millisecond}
		res := runRegime(t, rg)
		_, _, p99, _ := percentiles(res.tracker.lateness)
		t.Logf("n=%-6d peakGoroutines=%-5d (idle %d, scheduler-owned≈%d) heapΔ=%-7dKB completed=%-6d skipped=%d dropped=%d p99lateness=%v",
			n, res.peak, res.idleGoroutines, res.peak-res.idleGoroutines, res.heapDeltaKB, res.completed, res.skipped, res.dropped, p99.Round(time.Microsecond))
	}
}
