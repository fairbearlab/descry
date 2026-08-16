package runner

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/fairbearlab/descry/check"
	"github.com/fairbearlab/descry/event"
)

// The tests here are the scheduler's invariants (PERF-PLAN §3.1, each one a
// test) driven by the single-timer fake clock in helpers_test.go. Real-clock
// tests are marked; everything else is deterministic under -race -count=N.

// ---------- New ----------

func TestNew_PanicsOnNonPositiveDefaultInterval(t *testing.T) {
	for _, iv := range []time.Duration{0, -time.Second} {
		func() {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatalf("New(interval=%v) did not panic", iv)
				}
				if s, _ := r.(string); !strings.Contains(s, "default interval") {
					t.Fatalf("panic message %q does not name the argument", r)
				}
			}()
			New(&fakeCheck{}, nopSink{}, event.Config{Source: "test"}, nil, iv, 1)
		}()
	}
}

func TestNew_ConcurrencyFloorIsOne(t *testing.T) {
	for _, c := range []int{0, -3} {
		if r := New(&fakeCheck{}, nopSink{}, event.Config{Source: "test"}, nil, time.Second, c); r.concurrency != 1 {
			t.Fatalf("concurrency %d → %d, want 1", c, r.concurrency)
		}
	}
}

func TestNew_PerTargetIntervalDefaultsAndOverrides(t *testing.T) {
	ts := []check.Target{
		{URL: "http://a"},
		{URL: "http://b", Interval: -1},
		{URL: "http://c", Interval: 3 * time.Second},
	}
	r := New(&fakeCheck{}, nopSink{}, event.Config{Source: "test"}, ts, 10*time.Second, 1)
	want := []time.Duration{10 * time.Second, 10 * time.Second, 3 * time.Second}
	for i, e := range r.entries {
		if e.interval != want[i] {
			t.Errorf("entry %d interval = %v, want %v", i, e.interval, want[i])
		}
		if e.phase < 0 || e.phase >= e.interval {
			t.Errorf("entry %d phase %v not in [0, %v)", i, e.phase, e.interval)
		}
	}
}

func TestNew_ResultsCapIsSteadyStateHeuristic(t *testing.T) {
	for _, n := range []int{0, 1, 7} {
		r := New(&fakeCheck{}, nopSink{}, event.Config{Source: "test"}, targetsN(n, "http://x"), time.Second, 1)
		if got := cap(r.results); got != 2*n+1 {
			t.Errorf("n=%d: cap(results) = %d, want %d", n, got, 2*n+1)
		}
	}
}

func TestNew_RedactedURLNeverCarriesUserinfo(t *testing.T) {
	secret := "s3cret" // #nosec G101 -- test fixture; the assertion is that it never reaches the log path
	r := New(&fakeCheck{}, nopSink{}, event.Config{Source: "test"},
		[]check.Target{{URL: "https://user:" + secret + "@example.com/p?q=1"}}, time.Second, 1)
	if got := r.entries[0].redacted; strings.Contains(got, secret) {
		t.Fatalf("redacted URL leaks the password: %q", got)
	}
}

// ---------- phase ----------

func TestPhase_DeterministicBoundedAndFNV(t *testing.T) {
	for _, iv := range []time.Duration{time.Second, 30 * time.Second, 15 * time.Minute} {
		a, b := phaseOf("https://example.com/x", iv), phaseOf("https://example.com/x", iv)
		if a != b {
			t.Fatalf("iv=%v: phase not deterministic (%v vs %v)", iv, a, b)
		}
		if a < 0 || a >= iv {
			t.Fatalf("iv=%v: phase %v out of [0, iv)", iv, a)
		}
	}
	// A pinned value: FNV-1a-64 is stable across processes and Go versions, which
	// is what makes cadence restart-invariant (hash/maphash would not be).
	if got := phaseOf("https://example.com/x", 10*time.Second); got != 5_857_744_516*time.Nanosecond {
		t.Fatalf("phaseOf pinned value changed: %v — the hash is no longer FNV-1a-64", got)
	}
}

func TestSlotAfter_EpochAligned(t *testing.T) {
	now := epoch.Add(7 * time.Second)
	cases := []struct {
		phase time.Duration
		want  time.Time
	}{
		{3 * time.Second, epoch.Add(13 * time.Second)}, // slot at :03 already passed → :13
		{9 * time.Second, epoch.Add(9 * time.Second)},  // :09 still ahead
		{7 * time.Second, epoch.Add(17 * time.Second)}, // exactly now is not "after" → next
	}
	for _, c := range cases {
		if got := slotAfter(now, 10*time.Second, c.phase); !got.Equal(c.want) {
			t.Errorf("phase %v: slot = %v, want %v", c.phase, got, c.want)
		}
	}
}

// ---------- Run: cadence ----------

func TestRun_ZeroTargetsBlocksOnCtx(t *testing.T) { // real clock
	r := New(&fakeCheck{}, nopSink{}, event.Config{Source: "test"}, nil, time.Second, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := r.Run(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) < 40*time.Millisecond {
		t.Fatalf("Run(zero targets) = %v after %v; want to block until ctx", err, time.Since(start))
	}
	if _, ok := <-r.Results(); ok {
		t.Fatal("Results not closed after Run returned")
	}
}

func TestRun_FiresAtPhaseThenEveryInterval(t *testing.T) {
	const iv = 10 * time.Second
	chk := &fakeCheck{calls: make(chan call, 16)}
	tr := newTestRunner(t, chk, []check.Target{{URL: "http://a"}}, iv, 1)

	slot := firstSlot("http://a", iv)
	tr.advanceTo(slot.Add(-time.Nanosecond))
	expectNoCall(t, chk.calls) // not before its phase

	for i := range 3 {
		want := slot.Add(time.Duration(i) * iv)
		tr.advanceTo(want)
		if c := recvCall(t, chk.calls); !c.at.Equal(want) {
			t.Fatalf("run %d at %v, want %v", i, c.at, want)
		}
		tr.completeOne(t)
	}
	if chk.overlap.Load() != 0 || tr.Skipped() != 0 {
		t.Fatalf("overlap=%d skipped=%d", chk.overlap.Load(), tr.Skipped())
	}
}

func TestRun_MixedIntervals(t *testing.T) {
	// A 1s target and a 3s target over 6s: 6 and 2 runs, each exactly on its
	// own slot. Advanced slot by slot (a single 6s jump would be a stall and
	// coalesce, invariant 5).
	ts := []check.Target{{URL: "http://one", Interval: time.Second}, {URL: "http://three", Interval: 3 * time.Second}}
	chk := &fakeCheck{calls: make(chan call, 64)}
	tr := newTestRunner(t, chk, ts, time.Minute, 2)

	type slot struct {
		at  time.Time
		url string
	}
	var slots []slot
	for _, tg := range ts {
		for at := firstSlot(tg.URL, tg.Interval); !at.After(epoch.Add(6 * time.Second)); at = at.Add(tg.Interval) {
			slots = append(slots, slot{at, tg.URL})
		}
	}
	sort.Slice(slots, func(a, b int) bool { return slots[a].at.Before(slots[b].at) })

	got := map[string]int{}
	for _, sl := range slots {
		tr.advanceTo(sl.at)
		c := recvCall(t, chk.calls)
		if c.url != sl.url || !c.at.Equal(sl.at) {
			t.Fatalf("got %s at %v, want %s at %v", c.url, c.at, sl.url, sl.at)
		}
		got[c.url]++
		tr.completeOne(t)
	}
	expectNoCall(t, chk.calls)
	if got["http://one"] != 6 || got["http://three"] != 2 || tr.Skipped() != 0 {
		t.Fatalf("runs = %v skipped=%d, want one:6 three:2 skipped:0", got, tr.Skipped())
	}
}

func TestRun_HeapOrderAcrossManyTargets(t *testing.T) {
	// 50 targets with distinct phases: each first fires exactly at its own slot,
	// in slot order.
	const iv = 10 * time.Second
	ts := targetsN(50, "http://t")
	chk := &fakeCheck{calls: make(chan call, 64)}
	tr := newTestRunner(t, chk, ts, iv, 8)

	slots := make([]time.Time, len(ts))
	for i, tg := range ts {
		slots[i] = firstSlot(tg.URL, iv)
	}
	order := make([]int, len(ts))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool { return slots[order[a]].Before(slots[order[b]]) })

	for _, i := range order {
		tr.advanceTo(slots[i])
		c := recvCall(t, chk.calls)
		if c.url != ts[i].URL || !c.at.Equal(slots[i]) {
			t.Fatalf("got %s at %v, want %s at %v", c.url, c.at, ts[i].URL, slots[i])
		}
		tr.completeOne(t)
	}
	expectNoCall(t, chk.calls)
}

func TestRun_DuplicateURLsShareAPhaseAndBothRun(t *testing.T) {
	const iv = 10 * time.Second
	ts := []check.Target{{URL: "http://dup", Labels: map[string]string{"n": "1"}}, {URL: "http://dup", Labels: map[string]string{"n": "2"}}}
	chk := &fakeCheck{calls: make(chan call, 16)}
	tr := newTestRunner(t, chk, ts, iv, 2)
	if a, b := tr.entries[0].phase, tr.entries[1].phase; a != b {
		t.Fatalf("phases differ for identical URLs: %v vs %v", a, b)
	}
	tr.advanceTo(firstSlot("http://dup", iv))
	seen := map[string]bool{}
	for range 2 {
		recvCall(t, chk.calls)
		seen[tr.completeOne(t).Target.Labels["n"]] = true
	}
	if !seen["1"] || !seen["2"] {
		t.Fatalf("results did not report both duplicates independently: %v", seen)
	}
}

// ---------- skips ----------

// TestSkip_SlowCheckReportsErrSkipped is the CRITICAL regression of the old
// TestSkipTick_IncrementsCounter: a check longer than its interval yields
// exactly one ErrSkipped per missed slot, Skipped() counts it, the target
// never overlaps itself, and phase is kept when it fires again.
func TestSkip_SlowCheckReportsErrSkipped(t *testing.T) {
	const iv = 10 * time.Second
	chk := &fakeCheck{gate: make(chan struct{}), calls: make(chan call, 16)}
	tr := newTestRunner(t, chk, []check.Target{{URL: "http://slow"}}, iv, 1)

	slot := firstSlot("http://slow", iv)
	tr.advanceTo(slot)
	recvCall(t, chk.calls) // running (started=true)

	tr.advance(iv) // slot+iv comes due while the run is in flight
	res := recv(t, tr.Results())
	if !errors.Is(res.Err, ErrSkipped) || errors.Is(res.Err, ErrSkippedQueued) {
		t.Fatalf("Err = %v, want ErrSkipped (not Queued)", res.Err)
	}
	if tr.Skipped() != 1 {
		t.Fatalf("Skipped() = %d, want 1", tr.Skipped())
	}
	expectNoCall(t, chk.calls) // never concurrent with itself

	close(chk.gate)
	if r := tr.completeOne(t); r.Err != nil {
		t.Fatalf("completion Err = %v", r.Err)
	}
	// Next fire is on the original phase, two intervals after the first slot —
	// not "now + interval".
	tr.advance(iv)
	if c := recvCall(t, chk.calls); !c.at.Equal(slot.Add(2 * iv)) {
		t.Fatalf("re-fire at %v, want %v (phase kept)", c.at, slot.Add(2*iv))
	}
	if chk.overlap.Load() != 0 {
		t.Fatalf("target overlapped itself %d times", chk.overlap.Load())
	}
}

// TestSkip_QueuedReportsErrSkippedQueued (D28): concurrency 1, target A blocks
// the only worker; B is dispatched but sits queued. At the next slot A reports
// ErrSkipped (started) and B reports ErrSkippedQueued (not started).
func TestSkip_QueuedReportsErrSkippedQueued(t *testing.T) {
	const iv = 10 * time.Second
	chk := &fakeCheck{gate: make(chan struct{}), calls: make(chan call, 16)}
	tr := newTestRunner(t, chk, []check.Target{{URL: "http://a"}, {URL: "http://b"}}, iv, 1)

	tr.advance(iv) // both slots pass; the earlier-phased one starts, the other queues
	first := recvCall(t, chk.calls)
	expectNoCall(t, chk.calls)

	tr.advance(iv)
	got := map[string]error{}
	for range 2 {
		r := recv(t, tr.Results())
		got[r.Target.URL] = r.Err
	}
	for url, err := range got {
		if !errors.Is(err, ErrSkipped) {
			t.Fatalf("%s: %v is not an ErrSkipped kind", url, err)
		}
		queued := errors.Is(err, ErrSkippedQueued)
		if url == first.url && queued {
			t.Fatalf("%s started but was reported queued", url)
		}
		if url != first.url && !queued {
			t.Fatalf("%s was queued but reported as slow: %v", url, err)
		}
	}
	if tr.Skipped() != 2 {
		t.Fatalf("Skipped() = %d, want 2 (both kinds count)", tr.Skipped())
	}
	close(chk.gate)
}

// TestSkip_KOutstanding (D20): a check spanning three intervals produces exactly
// three ErrSkipped and one completion, and a draining consumer drops nothing.
func TestSkip_KOutstanding(t *testing.T) {
	const iv = 10 * time.Second
	chk := &fakeCheck{gate: make(chan struct{}), calls: make(chan call, 16)}
	tr := newTestRunner(t, chk, []check.Target{{URL: "http://k"}}, iv, 1)

	tr.advanceTo(firstSlot("http://k", iv))
	recvCall(t, chk.calls)
	for i := 1; i <= 3; i++ {
		tr.advance(iv)
		if r := recv(t, tr.Results()); !errors.Is(r.Err, ErrSkipped) {
			t.Fatalf("skip %d: Err = %v", i, r.Err)
		}
	}
	close(chk.gate)
	if r := tr.completeOne(t); r.Err != nil {
		t.Fatalf("completion Err = %v", r.Err)
	}
	expectNoResult(t, tr.Results())
	if tr.Skipped() != 3 || tr.Dropped() != 0 || chk.overlap.Load() != 0 {
		t.Fatalf("skipped=%d dropped=%d overlap=%d", tr.Skipped(), tr.Dropped(), chk.overlap.Load())
	}
}

// ---------- clock steps ----------

// TestStall_OneRunNoSkipFlood (invariant 5): a forward jump of 1000 intervals
// is one run and one O(1) reschedule on the original phase.
func TestStall_OneRunNoSkipFlood(t *testing.T) {
	const iv = time.Second
	chk := &fakeCheck{calls: make(chan call, 16)}
	tr := newTestRunner(t, chk, []check.Target{{URL: "http://s"}}, iv, 1)

	tr.advance(1000 * iv)
	recvCall(t, chk.calls)
	tr.completeOne(t)
	expectNoCall(t, chk.calls)
	if tr.Skipped() != 0 {
		t.Fatalf("skip flood: Skipped() = %d", tr.Skipped())
	}
	// Next fire is phase-aligned: the first slot at or after now.
	next := slotAfter(tr.fc.Now(), iv, tr.entries[0].phase)
	tr.advanceTo(next)
	if c := recvCall(t, chk.calls); !c.at.Equal(next) {
		t.Fatalf("after stall fired at %v, want %v", c.at, next)
	}
}

// TestBackwardStep_ReanchorsWithinOneInterval (D19): after a 1h backward wall
// step every target fires within one interval, phase is kept, and the guard
// logs exactly once.
func TestBackwardStep_ReanchorsWithinOneInterval(t *testing.T) {
	const iv = 30 * time.Second
	logs := captureLogs(t, slog.LevelInfo)
	chk := &fakeCheck{calls: make(chan call, 128)}
	tr := newTestRunner(t, chk, targetsN(50, "http://b"), iv, 4)

	tr.fc.Step(-time.Hour)
	// Nothing wakes the scheduler until its already-armed (monotonic) timer
	// fires, at most one interval later; that lap re-anchors everything.
	tr.advance(iv)
	now := tr.fc.Now()
	tr.stop() // entries are safe to read once Run has returned

	for _, e := range tr.entries {
		if e.next.Before(now) || e.next.After(now.Add(iv)) {
			t.Errorf("%s: next=%v not within [now, now+iv] (now=%v)", e.t.URL, e.next, now)
		}
		if off := e.next.Sub(e.next.Truncate(iv)); off != e.phase {
			t.Errorf("%s: phase lost: next offset %v, phase %v", e.t.URL, off, e.phase)
		}
	}
	if got := logs.records(slog.LevelInfo); len(got) != 1 || !strings.Contains(got[0].Message, "re-anchored") {
		t.Fatalf("want exactly one re-anchor Info log, got %d: %+v", len(got), got)
	}
}

// TestEarlyWake_ReArmsWithoutRunning: a timer that fires before next (wall
// slew, spurious wake) is re-armed; nothing runs, nothing is skipped.
func TestEarlyWake_ReArmsWithoutRunning(t *testing.T) {
	const iv = 10 * time.Second
	chk := &fakeCheck{calls: make(chan call, 16)}
	tr := newTestRunner(t, chk, []check.Target{{URL: "http://e"}}, iv, 1)

	tr.fc.FireEarly()
	tr.fc.BlockUntil(1) // re-armed
	expectNoCall(t, chk.calls)
	expectNoResult(t, tr.Results())
	if tr.Skipped() != 0 {
		t.Fatalf("Skipped() = %d after early wake", tr.Skipped())
	}
	slot := firstSlot("http://e", iv)
	tr.advanceTo(slot)
	if c := recvCall(t, chk.calls); !c.at.Equal(slot) {
		t.Fatalf("fired at %v, want %v", c.at, slot)
	}
}

// ---------- rebuild continuity ----------

// TestRebuild_Continuity (D13): a runner rebuilt mid-cycle from the same targets
// fires each target at the same wall-clock slot the old one would have.
func TestRebuild_Continuity(t *testing.T) {
	const iv = 10 * time.Second
	ts := targetsN(5, "http://r")
	a := newTestRunner(t, &fakeCheck{}, ts, iv, 2)
	a.stop()
	wantNext := make([]time.Time, len(ts))
	for i, e := range a.entries {
		wantNext[i] = e.next
	}

	// Rebuild at an instant that is after A started but before A's earliest slot.
	earliest := wantNext[0]
	for _, n := range wantNext[1:] {
		if n.Before(earliest) {
			earliest = n
		}
	}
	fc := newFakeClock(t)
	fc.Advance(earliest.Sub(epoch) / 2)
	b := startTestRunner(t, New(&fakeCheck{}, nopSink{}, event.Config{Source: "test"}, ts, iv, 2), fc)
	b.stop()
	for i, e := range b.entries {
		if !e.next.Equal(wantNext[i]) {
			t.Errorf("%s: rebuilt next=%v, original %v", e.t.URL, e.next, wantNext[i])
		}
	}
}

// TestRebuild_AcrossSlotBoundary (D26): A stops at slot−ε, B is built at slot+ε.
// The slot inside downtime is lost; B first fires at slot+interval, so the gap
// between consecutive observations of that target is exactly two intervals.
func TestRebuild_AcrossSlotBoundary(t *testing.T) {
	const iv = 10 * time.Second
	ts := []check.Target{{URL: "http://gap"}}
	slot := firstSlot("http://gap", iv)

	chkA := &fakeCheck{calls: make(chan call, 16)}
	a := newTestRunner(t, chkA, ts, iv, 1)
	a.advanceTo(slot)
	last := recvCall(t, chkA.calls).at // observed at slot
	a.advanceTo(slot.Add(iv - time.Millisecond))
	a.stop() // down at slot+iv−ε

	fc := newFakeClock(t)
	fc.Advance(slot.Add(iv + time.Millisecond).Sub(epoch)) // back at slot+iv+ε
	chkB := &fakeCheck{calls: make(chan call, 16), now: fc.Now}
	b := startTestRunner(t, New(chkB, nopSink{}, event.Config{Source: "test"}, ts, iv, 1), fc)
	_ = b

	fc.Advance(iv - 2*time.Millisecond) // → slot+2iv−ε: not yet
	fc.BlockUntil(1)
	expectNoCall(t, chkB.calls)
	fc.Advance(time.Millisecond) // → slot+2iv
	first := recvCall(t, chkB.calls).at
	if !first.Equal(slot.Add(2 * iv)) {
		t.Fatalf("B first fired at %v, want %v", first, slot.Add(2*iv))
	}
	if gap := first.Sub(last); gap != 2*iv {
		t.Fatalf("observation gap = %v, want exactly 2×interval", gap)
	}
}

// ---------- false skip (D27) ----------

// TestFalseSkip_AckBeforeTimerIsNeverSkipped: completion is scheduler receipt of
// done. A worker that has sent its ack before the slot fires is drained before
// the due judgment, so the target runs again instead of being reported skipped.
func TestFalseSkip_AckBeforeTimerIsNeverSkipped(t *testing.T) {
	const iv = 10 * time.Second
	chk := &fakeCheck{gate: make(chan struct{}), calls: make(chan call, 16)}
	fc := newFakeClock(t)
	chk.now = fc.Now
	tr := startTestRunner(t, New(chk, nopSink{}, event.Config{Source: "test"}, []check.Target{{URL: "http://f"}}, iv, 1), fc)
	r := tr.Runner

	slot := firstSlot("http://f", iv)
	fc.Advance(slot.Sub(epoch))
	fc.BlockUntil(1)
	recvCall(t, chk.calls)
	fc.Advance(iv - time.Nanosecond) // slot+iv−ε
	fc.BlockUntil(1)
	close(chk.gate)   // the run finishes at slot+iv−ε …
	tr.completeOne(t) // … and its ack is on done before the timer fires

	fc.Advance(time.Nanosecond) // slot+iv
	c := recvCall(t, chk.calls)
	if !c.at.Equal(slot.Add(iv)) {
		t.Fatalf("second run at %v, want %v", c.at, slot.Add(iv))
	}
	if r.Skipped() != 0 {
		t.Fatalf("false skip: Skipped() = %d", r.Skipped())
	}
}

// ---------- pool & shutdown ----------

func TestPool_ExactlyConcurrencyWorkers(t *testing.T) {
	const iv, n, conc = 10 * time.Second, 20, 3
	chk := &fakeCheck{gate: make(chan struct{}), calls: make(chan call, 64)}
	tr := newTestRunner(t, chk, targetsN(n, "http://p"), iv, conc)

	tr.advance(iv) // every slot passes; all 20 dispatched into a 3-worker pool
	for range conc {
		recvCall(t, chk.calls)
	}
	expectNoCall(t, chk.calls)
	if got := chk.peakConcurrency(); got != conc {
		t.Fatalf("peak concurrency = %d, want exactly %d", got, conc)
	}
	close(chk.gate)
	for range n - conc {
		recvCall(t, chk.calls)
	}
	if got := chk.peakConcurrency(); got > conc {
		t.Fatalf("peak concurrency = %d, want <= %d", got, conc)
	}
}

// TestShutdown_QueuedEntriesAckedNotRun (D16): with entries queued behind a
// busy pool, cancel yields only the in-flight completion — no burst of
// context.Canceled Results, and queued entries are never run.
func TestShutdown_QueuedEntriesAckedNotRun(t *testing.T) {
	const iv = 10 * time.Second
	chk := &fakeCheck{gate: make(chan struct{}), calls: make(chan call, 16)}
	tr := newTestRunner(t, chk, targetsN(5, "http://q"), iv, 1)

	tr.advance(iv) // 1 running, 4 queued
	recvCall(t, chk.calls)
	tr.stop() // cancel; the blocked run returns on ctx; queued entries are acked
	close(chk.gate)

	var results []Result
	for r := range tr.Results() {
		results = append(results, r)
	}
	if chk.total.Load() != 1 {
		t.Fatalf("check ran %d times after cancel, want 1 (queued entries must not run)", chk.total.Load())
	}
	if len(results) > 1 {
		t.Fatalf("got %d Results on shutdown, want <= 1: %+v", len(results), results)
	}
	for _, r := range results {
		if errors.Is(r.Err, context.Canceled) {
			t.Fatalf("context.Canceled surfaced on Results: %+v", r)
		}
	}
	if !errors.Is(tr.err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", tr.err)
	}
}

// TestShutdown_NoPublishAfterRunReturns: Run waits for in-flight runs, so a
// deferred sink Close never races a Publish.
func TestShutdown_NoPublishAfterRunReturns(t *testing.T) { // real clock
	const n = 20
	chk := &fakeCheck{delay: 5 * time.Millisecond}
	s := &closedSink{}
	r := New(chk, s, event.Config{Source: "test"}, targetsN(n, "http://c"), 20*time.Millisecond, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	_ = r.Run(ctx)
	s.closed.Store(true)
	time.Sleep(20 * time.Millisecond) // give any straggler the chance to be caught
	if s.published.Load() == 0 {
		t.Fatal("nothing published; test is vacuous")
	}
	if s.afterClose.Load() != 0 {
		t.Fatalf("%d Publish calls after Run returned", s.afterClose.Load())
	}
	drained := 0
	for range r.Results() { // drains to the close; hangs (and fails on timeout) if not closed
		drained++
	}
	t.Logf("drained %d buffered Results after Run returned", drained)
}

// TestRun_ShutdownDoesNotRequireResultsDrain: diagnostics are best-effort;
// ignoring Results must not wedge shutdown.
func TestRun_ShutdownDoesNotRequireResultsDrain(t *testing.T) { // real clock
	r := New(&fakeCheck{}, nopSink{}, event.Config{Source: "test"}, []check.Target{{URL: "http://d"}}, time.Millisecond, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	time.Sleep(25 * time.Millisecond) // enough slots to fill the small results buffer
	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not shut down when Results was not drained")
	}
	if r.Dropped() == 0 {
		t.Log("note: no drops observed; the buffer never filled on this host")
	}
}

// ---------- results channel ----------

// TestReportResult_DropCountedAndWarnedOncePerInterval (D20/D25): a full channel
// counts every drop but warns at most once per runner-default interval.
func TestReportResult_DropCountedAndWarnedOncePerInterval(t *testing.T) {
	const iv = 10 * time.Second
	logs := captureLogs(t, slog.LevelWarn)
	fc := newFakeClock(t)
	r := New(&fakeCheck{}, nopSink{}, event.Config{Source: "test"}, nil, iv, 1) // cap(results) == 1
	r.clock = fc
	res := Result{Target: check.Target{URL: "http://drop"}}
	r.reportResult(res) // fills the buffer
	r.reportResult(res) // drop 1 → Warn
	r.reportResult(res) // drop 2 → rate-limited
	if r.Dropped() != 2 {
		t.Fatalf("Dropped() = %d, want 2", r.Dropped())
	}
	if got := len(logs.records(slog.LevelWarn)); got != 1 {
		t.Fatalf("warns inside one interval = %d, want 1", got)
	}
	fc.Advance(iv)
	r.reportResult(res) // drop 3 → a new interval → Warn again
	if r.Dropped() != 3 || len(logs.records(slog.LevelWarn)) != 2 {
		t.Fatalf("Dropped()=%d warns=%d, want 3 and 2", r.Dropped(), len(logs.records(slog.LevelWarn)))
	}
}

// TestSkipPath_ZeroAllocs (D11/D23): the skip branch allocates nothing beyond
// the Result send — reused timer, pre-built sentinels, LogAttrs with a
// pre-redacted string on the disabled Debug path.
func TestSkipPath_ZeroAllocs(t *testing.T) {
	r := New(&fakeCheck{}, nopSink{}, event.Config{Source: "test"}, []check.Target{{URL: "http://alloc"}}, time.Second, 1)
	e := r.entries[0]
	e.inflight = true
	ctx := context.Background()
	allocs := testing.AllocsPerRun(1000, func() {
		r.skip(ctx, e)
		<-r.results
	})
	if allocs != 0 {
		t.Fatalf("skip path allocs/op = %v, want 0", allocs)
	}
}

// ---------- retry ladder (unchanged behavior; real clock) ----------

func TestRunOne_RetriesThenSucceeds(t *testing.T) {
	s := &flakySink{}
	s.failsLeft.Store(2)
	r := New(&fakeCheck{}, s, event.Config{Source: "t"}, []check.Target{{URL: "http://x"}}, time.Second, 1)
	r.runOne(context.Background(), r.entries[0].t)
	res := recv(t, r.Results())
	if res.Err != nil {
		t.Fatalf("Err = %v, want nil after retry success", res.Err)
	}
	if got := s.calls.Load(); got != 3 {
		t.Fatalf("Publish calls = %d, want 3", got)
	}
}

func TestRunOne_RetriesExhausted(t *testing.T) {
	s := &alwaysFailSink{}
	r := New(&fakeCheck{}, s, event.Config{Source: "t"}, []check.Target{{URL: "http://x"}}, time.Second, 1)
	r.runOne(context.Background(), r.entries[0].t)
	res := recv(t, r.Results())
	if res.Err == nil {
		t.Fatal("Err = nil, want non-nil after exhausted retries")
	}
	if got := s.calls.Load(); got != maxPublishAttempts {
		t.Fatalf("Publish calls = %d, want %d", got, maxPublishAttempts)
	}
}

func TestRunOne_CheckErrorIsReported(t *testing.T) {
	want := errors.New("boom")
	r := New(&fakeCheck{err: want}, nopSink{}, event.Config{Source: "test"}, []check.Target{{URL: "http://x"}}, time.Second, 1)
	r.runOne(context.Background(), r.entries[0].t)
	if res := recv(t, r.Results()); !errors.Is(res.Err, want) {
		t.Fatalf("Err = %v, want %v", res.Err, want)
	}
}

// ---------- fake clock self-test ----------

func TestFakeClock_SecondTimerPanics(t *testing.T) {
	fc := newFakeClock(t)
	fc.NewTimer(time.Second)
	defer func() {
		if recover() == nil {
			t.Fatal("second NewTimer did not panic")
		}
	}()
	fc.NewTimer(time.Second)
}
