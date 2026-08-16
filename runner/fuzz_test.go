package runner

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/fairbearlab/descry/check"
	"github.com/fairbearlab/descry/event"
)

// FuzzScheduler drives the scheduler under the fake clock
// with a decoded script of targets, clock movements and consumer behaviour, and
// checks it against a *differential* oracle: `shadow` below is an independent
// model of the scheduler's TIMING ONLY — when a slot is processed, how far next
// advances, when the re-anchor guard fires. The shadow knows nothing about the
// worker pool, inflight, skips or Results, which is what makes the accounting
// identity a real assertion instead of a restatement of runner.go: the shadow
// says how many slots were *processed*, and the runtime must account for every
// one of them as either a check invocation or a skip.
//
// Why the model can be exact without modelling the pool: under forward-only
// clock movement a slot is processed on (now, next, interval) alone. Whether the
// due entry is dispatched or skipped depends on worker scheduling, but that
// choice changes neither the slot count nor the next arithmetic. The one
// timing-sensitive path is the backward-step re-anchor guard, because a lap
// driven by a worker ack would evaluate it at an unpredictable `now`; the
// backward step is therefore only ever applied while no dispatch has yet
// happened (see runFuzzScheduler).

const (
	maxFuzzTargets = 16
	maxFuzzOps     = 24
	// fuzzSettleWait is real time, not fake time: it bounds the wait for worker
	// goroutines to ack, so a bug reports a diagnostic instead of hanging.
	fuzzSettleWait = 5 * time.Second
)

var (
	fuzzIntervals = []time.Duration{time.Second, 2 * time.Second, 5 * time.Second, 30 * time.Second}
	fuzzSteps     = []time.Duration{time.Second, 30 * time.Second, time.Minute, time.Hour, 25 * time.Hour}

	fuzzMinInterval = fuzzIntervals[0]
	fuzzMaxInterval = fuzzIntervals[len(fuzzIntervals)-1]
	// fuzzTick is the jitter unit: small enough that a jitter advance sometimes
	// lands short of the armed deadline and sometimes overshoots it.
	fuzzTick = fuzzMinInterval / 8
)

// --- decoder ---

// fuzzBytes hands out the input one byte at a time and returns 0 once
// exhausted, so a truncated or short input is still a valid (smaller) script
// rather than a decode failure.
type fuzzBytes struct {
	b []byte
	i int
}

func (r *fuzzBytes) next() byte {
	if r.i >= len(r.b) {
		return 0
	}
	v := r.b[r.i]
	r.i++
	return v
}

func (r *fuzzBytes) exhausted() bool { return r.i >= len(r.b) }

// --- shadow: an independent model of the scheduler's timing ---

// shadow mirrors schedule()'s loop and nothing else. It reuses the production
// phaseOf and slotAfter deliberately: both are separately pinned by
// TestPhase_DeterministicBoundedAndFNV and TestSlotAfter_EpochAligned, and this
// target is about the scheduler LOOP, not about the hash.
//
// Be honest about what that buys. lap() below is a line-for-line transcription
// of schedule()'s guard, due test and coalesce arithmetic — it is a frozen
// golden copy, not an independent re-derivation. So assertions (g) and (i) are
// REGRESSION DETECTORS: they catch any future edit that changes the schedule,
// but a bug that is in schedule() today and was faithfully copied here is
// invisible to them, because both were written from one reading. The
// assertions that are genuinely independent of runner.go are (a), (b), (b2),
// (e), (f) and (h): they hold the runtime to an external account of what it
// must have done, and the shadow supplies only the slot count.
type shadow struct {
	interval []time.Duration
	phase    []time.Duration
	next     []time.Time
	slots    []int // slots processed per entry (dispatched or skipped)

	now       time.Time
	deadline  time.Time // the instant the scheduler's one timer is armed for
	reanchors int
	total     int
}

func newShadow(targets []check.Target, defIV time.Duration, start time.Time) *shadow {
	s := &shadow{now: start}
	for _, tg := range targets {
		iv := tg.Interval
		if iv <= 0 {
			iv = defIV
		}
		ph := phaseOf(tg.URL, iv)
		s.interval = append(s.interval, iv)
		s.phase = append(s.phase, ph)
		s.next = append(s.next, slotAfter(start, iv, ph))
		s.slots = append(s.slots, 0)
	}
	s.deadline = s.next[s.min()]
	return s
}

// min is the heap top. Ties resolve to the lowest index here and to heap order
// in the runner; distinct FNV phases make an exact tie between two entries'
// next instants effectively impossible, and a tie between two *due* entries
// changes nothing (both are processed in the same lap).
func (s *shadow) min() int {
	m := 0
	for i := 1; i < len(s.next); i++ {
		if s.next[i].Before(s.next[m]) {
			m = i
		}
	}
	return m
}

// lap is one wake of the scheduler: process everything due, then re-arm for the
// new heap top.
func (s *shadow) lap() {
	for {
		i := s.min()
		// The guard keys off the HEAP MINIMUM, exactly as schedule() does. A
		// non-minimum entry can therefore stay stale past this lap; that is the
		// reason assertion (c) carries the looser +step bound.
		if s.next[i].Sub(s.now) > s.interval[i] {
			for j := range s.next {
				if s.next[j].Sub(s.now) > s.interval[j] {
					s.next[j] = slotAfter(s.now, s.interval[j], s.phase[j])
				}
			}
			s.reanchors++
			continue
		}
		if s.next[i].After(s.now) {
			break
		}
		s.slots[i]++
		s.total++
		// Whole intervals, so phase is kept and a stall of any length is one
		// processed slot (invariant 5).
		k := (s.now.Sub(s.next[i]) / s.interval[i]) + 1
		s.next[i] = s.next[i].Add(k * s.interval[i])
	}
	s.deadline = s.next[s.min()]
}

// advance elapses d. The scheduler only re-evaluates when its timer fires, so
// the model only laps when the armed deadline is reached.
func (s *shadow) advance(d time.Duration) {
	s.now = s.now.Add(d)
	if !s.now.Before(s.deadline) {
		s.lap()
	}
}

// step moves the wall clock without elapsing time. The armed deadline is
// monotonic and shifts with it (fakeClock.Step, real time.Timer semantics), so
// the harness must track it separately from min(next) from here on.
func (s *shadow) step(d time.Duration) {
	s.now = s.now.Add(d)
	s.deadline = s.deadline.Add(d)
}

// --- fuzz check ---

// fuzzCheck is local to this file on purpose: the shared fakeCheck is tuned for
// the hand-written invariant tests and must not grow fuzz-only knobs. It counts
// invocations per URL and flags any target entered while already running.
type fuzzCheck struct {
	gate chan struct{}
	slow map[string]bool // URLs whose Run blocks until the gate is released

	mu       sync.Mutex
	calls    map[string]int
	inflight map[string]int
	overlap  int
}

func (c *fuzzCheck) Name() string { return "fuzz" }

func (c *fuzzCheck) Run(ctx context.Context, t check.Target) (check.Observation, error) {
	c.mu.Lock()
	c.calls[t.URL]++
	if c.inflight[t.URL] > 0 {
		c.overlap++
	}
	c.inflight[t.URL]++
	slow := c.slow[t.URL]
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.inflight[t.URL]--
		c.mu.Unlock()
	}()

	if slow {
		select {
		case <-c.gate:
		case <-ctx.Done():
		}
	}
	// Same shape fakeCheck returns, so event.ToCloudEvent always succeeds and a
	// non-nil Result.Err can only be a skip sentinel (assertion (h)).
	return check.Observation{
		Status: check.StatusUp, StatusCode: 200, ObservedAt: time.Now().UTC(),
		Labels: t.Labels, Extra: map[string]any{},
	}, nil
}

// snapshot copies the counters once the workers are quiescent.
func (c *fuzzCheck) snapshot() (calls map[string]int, overlap int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	calls = make(map[string]int, len(c.calls))
	for k, v := range c.calls {
		calls[k] = v
	}
	return calls, c.overlap
}

// --- seed corpus ---

// The seeds are built from a readable struct rather than opaque blobs in
// testdata/fuzz so a reader can see which scenario each one
// reproduces; `go test -fuzz` still writes crashers to
// runner/testdata/fuzz/FuzzScheduler as usual.
type fuzzSeed struct {
	name        string
	concurrency int // 1..4
	defIV       int // index into fuzzIntervals
	backStep    int // 0 = none, else 1+index into fuzzSteps
	pre         int // 0..7: eighths of the gap to advance before the step
	targets     []fuzzSeedTarget
	ops         []fuzzSeedOp
}

type fuzzSeedTarget struct {
	interval int // 0 = the runner default, else 1+index into fuzzIntervals
	slow     bool
}

type fuzzSeedOp struct{ kind, arg int }

// encode writes the byte layout runFuzzScheduler decodes.
func (s fuzzSeed) encode() []byte {
	// seedByte masks rather than converting outright: every value here is a small
	// literal from fuzzSeeds, so the mask is a proof for the reader (and gosec)
	// that nothing truncates, not a guard against input the fuzzer controls.
	seedByte := func(v int) byte { return byte(v & 0xff) }
	b := []byte{
		seedByte(len(s.targets) - 1),
		seedByte(s.concurrency - 1),
		seedByte(s.defIV),
		seedByte(s.backStep),
		seedByte(s.pre),
	}
	for _, tg := range s.targets {
		v := seedByte(tg.interval)
		if tg.slow {
			v |= 0x80
		}
		b = append(b, v)
	}
	for _, op := range s.ops {
		b = append(b, seedByte(op.kind), seedByte(op.arg))
	}
	return b
}

func fuzzSeeds() []fuzzSeed {
	slow := fuzzSeedTarget{interval: 4, slow: true} // 30s, blocks on the gate
	return []fuzzSeed{
		{
			// Mirrors TestSkip_SlowCheckReportsErrSkipped: one check slower than
			// its interval, so the following slot is an ErrSkipped.
			name: "slow-skip", concurrency: 1, defIV: 3,
			targets: []fuzzSeedTarget{slow},
			ops:     []fuzzSeedOp{{0, 0}, {0, 0}, {3, 0}, {0, 0}},
		},
		{
			// Mirrors TestSkip_QueuedReportsErrSkippedQueued: two slow
			// targets against a single worker, so one skip is ErrSkipped and the
			// other ErrSkippedQueued.
			name: "queued-skip", concurrency: 1, defIV: 3,
			targets: []fuzzSeedTarget{slow, slow},
			ops:     []fuzzSeedOp{{0, 0}, {0, 0}, {0, 0}, {0, 0}, {3, 0}},
		},
		{
			// Mirrors TestStall_OneRunNoSkipFlood (invariant 5): a 1200s forward
			// jump on a 1s interval is one processed slot, not a skip flood.
			name: "stall", concurrency: 1, defIV: 0,
			targets: []fuzzSeedTarget{{interval: 1}},
			ops:     []fuzzSeedOp{{2, 39}, {0, 0}, {0, 0}},
		},
		{
			// Mirrors TestBackwardStep_ReanchorsWithinOneInterval: mixed
			// intervals, a 1h backward wall step taken before any dispatch exists.
			name: "backward-step", concurrency: 4, defIV: 3, backStep: 4, pre: 3,
			targets: []fuzzSeedTarget{
				{interval: 1}, {interval: 2}, {interval: 3}, {interval: 4}, {interval: 0}, {interval: 1},
			},
			ops: []fuzzSeedOp{{0, 0}, {0, 0}, {1, 7}, {0, 0}},
		},
		{
			// Mirrors TestSkip_KOutstanding: a check spanning several
			// intervals produces one skip per missed slot, then one completion.
			name: "k-skips", concurrency: 1, defIV: 3,
			targets: []fuzzSeedTarget{slow},
			ops:     []fuzzSeedOp{{0, 0}, {0, 0}, {0, 0}, {0, 0}, {3, 0}, {0, 0}},
		},
		{
			// A consumer that stops draining: Results overflow the steady-state
			// buffer, every loss is counted in Dropped(), and the per-target
			// identity is relaxed accordingly (assertion (b)).
			name: "stalled-consumer", concurrency: 4, defIV: 0,
			targets: []fuzzSeedTarget{
				{interval: 1}, {interval: 1}, {interval: 1}, {interval: 1},
				{interval: 1}, {interval: 1}, {interval: 1}, {interval: 1},
				{interval: 1}, {interval: 1}, {interval: 1}, {interval: 1},
				{interval: 1}, {interval: 1}, {interval: 1}, {interval: 1},
			},
			ops: []fuzzSeedOp{{4, 0}, {2, 5}, {2, 5}, {2, 5}, {2, 5}, {0, 0}, {0, 0}, {4, 0}, {0, 0}},
		},
	}
}

// FuzzScheduler checks the scheduler loop against the shadow model above.
func FuzzScheduler(f *testing.F) {
	for _, s := range fuzzSeeds() {
		f.Add(s.encode())
	}
	f.Fuzz(runFuzzScheduler)
}

// runFuzzScheduler is deliberately one linear script — decode, drive, settle,
// assert — so a reviewer can read the oracle top to bottom and see that it does
// not restate the code under test.
func runFuzzScheduler(t *testing.T, data []byte) {
	in := &fuzzBytes{b: data}
	n := 1 + int(in.next())%maxFuzzTargets
	conc := 1 + int(in.next())%4
	defIV := fuzzIntervals[int(in.next())%len(fuzzIntervals)]
	backStepSel := int(in.next()) % (len(fuzzSteps) + 1)
	preSel := int(in.next()) % 8

	targets := make([]check.Target, n)
	slow := make(map[string]bool, n)
	for i := range targets {
		b := in.next()
		url := "http://fuzz/" + strconv.Itoa(i) // distinct, so per-URL maps are unambiguous
		var iv time.Duration
		if sel := int(b&0x07) % (len(fuzzIntervals) + 1); sel > 0 {
			iv = fuzzIntervals[sel-1]
		}
		targets[i] = check.Target{URL: url, Interval: iv, Labels: map[string]string{"url": url}}
		if b&0x80 != 0 {
			slow[url] = true
		}
	}

	// Info keeps fuzz output quiet and still counts the re-anchor log; the Debug
	// skip log stays disabled, as in production.
	logs := captureLogs(t, slog.LevelInfo)
	chk := &fuzzCheck{
		gate: make(chan struct{}), slow: slow,
		calls: map[string]int{}, inflight: map[string]int{},
	}
	r := New(chk, nopSink{}, event.Config{Source: "fuzz"}, targets, defIV, conc)
	fc := newFakeClock(t)
	tr := startTestRunner(t, r, fc)

	sh := newShadow(targets, defIV, epoch)
	// Cross-check the model's derivation against the runner's, which incidentally
	// pins New's per-target interval rule. interval and phase are immutable after
	// New, so reading them while Run is live is safe.
	for i, e := range r.entries {
		if e.interval != sh.interval[i] || e.phase != sh.phase[i] {
			t.Fatalf("entry %d: runner (interval=%v phase=%v) != model (interval=%v phase=%v)",
				i, e.interval, e.phase, sh.interval[i], sh.phase[i])
		}
	}

	advance := func(d time.Duration) {
		if d <= 0 {
			return
		}
		fc.Advance(d)
		sh.advance(d)
		fc.BlockUntil(1) // the lap is complete and the scheduler is armed again
	}

	var stepMag time.Duration
	if backStepSel > 0 {
		// The step is taken before any dispatch can exist: the pre-step advance is
		// strictly less than the gap to the earliest slot, and startTestRunner
		// already waited for the first arm, so the scheduler is provably parked
		// with an empty done channel. A lap driven by a worker ack could otherwise
		// evaluate the re-anchor guard at a `now` the model cannot predict.
		gap := sh.deadline.Sub(sh.now)
		advance(time.Duration(preSel) * gap / 8)
		stepMag = fuzzSteps[backStepSel-1]
		fc.Step(-stepMag)
		sh.step(-stepMag)
	}

	// One guaranteed lap before the script proper. Two reasons, both measured:
	// without it ~41% of inputs (every input shorter than 6+n bytes decodes to
	// zero ops) processed no slot at all and every assertion below reduced to
	// 0 == 0; and a backward step whose observing lap never ran left assertion
	// (c) with nothing to check but next > now.
	advance(sh.deadline.Sub(sh.now))

	var gateOnce sync.Once
	releaseGate := func() { gateOnce.Do(func() { close(chk.gate) }) }

	acks, received := 0, 0
	skipResults := map[string]int{}
	drainAcks := func() {
		for {
			select {
			case <-tr.acks:
				acks++
			default:
				return
			}
		}
	}
	take := func(res Result) {
		received++
		if res.Err == nil {
			return
		}
		// (h) the only non-nil Err reachable here is one of the two pre-built
		// sentinels themselves — not a wrapper, and never context.Canceled.
		if !sameError(res.Err, ErrSkipped) && !sameError(res.Err, ErrSkippedQueued) {
			t.Fatalf("unexpected Result Err for %s: %v", res.Target.URL, res.Err)
		}
		if !errors.Is(res.Err, ErrSkipped) {
			t.Fatalf("%v does not satisfy errors.Is(err, ErrSkipped)", res.Err)
		}
		skipResults[res.Target.URL]++
	}
	drainResults := func() {
		for {
			select {
			case res := <-tr.Results():
				take(res)
			default:
				return
			}
		}
	}

	draining := true
	for range maxFuzzOps {
		if in.exhausted() {
			break
		}
		kind := int(in.next()) % 5
		arg := int(in.next())
		switch kind {
		case 0: // advance exactly to the armed deadline: a slot is guaranteed to fire
			advance(sh.deadline.Sub(sh.now))
		case 1: // jitter: sometimes short of the deadline, sometimes past it
			advance(time.Duration(arg%16+1) * fuzzTick)
		case 2: // stall: a long forward jump, which must coalesce (invariant 5)
			advance(time.Duration(arg%64+1) * fuzzMaxInterval)
		case 3: // release the gate; idempotent
			releaseGate()
		case 4: // toggle the consumer between draining and stalled (exercises Dropped())
			draining = !draining
		}
		// Always drain acks: the acks channel is capped, and a wedged worker would
		// turn a real bug into a timeout.
		drainAcks()
		if draining {
			drainResults()
		}
	}

	// Settle: with the gate open and the clock frozen no further slot can be
	// processed, so Skipped() is final and every dispatch must ack. Waiting for
	// all of them is what makes "every dispatched slot really invoked the check"
	// an observation rather than an assumption.
	releaseGate()
	wantAcks := sh.total - int(tr.Skipped())
	timeout := time.After(fuzzSettleWait)
	for acks < wantAcks {
		select {
		case <-tr.acks:
			acks++
			if draining {
				drainResults()
			}
		case <-timeout:
			t.Fatalf("settle: %d/%d worker acks after %v (slots=%d skipped=%d)",
				acks, wantAcks, fuzzSettleWait, sh.total, tr.Skipped())
		}
	}

	now := fc.Now()
	tr.stop()
	for res := range tr.Results() { // drains to the close; (f) hangs if never closed
		take(res)
	}

	calls, overlap := chk.snapshot()
	totalCalls, totalSkipResults := 0, 0
	for _, c := range calls {
		totalCalls += c
	}
	for _, c := range skipResults {
		totalSkipResults += c
	}
	skipped, dropped := tr.Skipped(), tr.Dropped()

	// (a) a target is never run concurrently with itself.
	if overlap != 0 {
		t.Errorf("target overlapped itself %d times", overlap)
	}

	// (b) accounting identity: the shadow counts slots processed; the runtime
	// must account for each as a check invocation or a skip.
	if int64(totalCalls)+skipped != int64(sh.total) {
		t.Errorf("accounting: calls(%d) + skipped(%d) != slots(%d)", totalCalls, skipped, sh.total)
	}
	if dropped == 0 {
		// With drops the per-target skip count is unrecoverable from Results (a
		// drop carries away the only record of which target lost a slot), so the
		// per-target identity is only asserted when nothing was dropped.
		for i, tg := range targets {
			if got := calls[tg.URL] + skipResults[tg.URL]; got != sh.slots[i] {
				t.Errorf("%s: calls(%d) + skips(%d) = %d, want %d slots",
					tg.URL, calls[tg.URL], skipResults[tg.URL], got, sh.slots[i])
			}
		}
	}

	// (b2) every run and every skip reports exactly one Result, on the channel or
	// on the drop counter.
	if int64(received)+dropped != int64(totalCalls)+skipped {
		t.Errorf("results: received(%d) + dropped(%d) != calls(%d) + skipped(%d)",
			received, dropped, totalCalls, skipped)
	}

	// (e) Skipped() is consistent with what the consumer could observe.
	if int64(totalSkipResults) > skipped || skipped > int64(totalSkipResults)+dropped {
		t.Errorf("skips: %d on Results, Skipped()=%d, Dropped()=%d", totalSkipResults, skipped, dropped)
	}
	if dropped == 0 && int64(totalSkipResults) != skipped {
		t.Errorf("skips: %d on Results but Skipped()=%d with no drops", totalSkipResults, skipped)
	}

	if !now.Equal(sh.now) {
		t.Fatalf("model clock %v != fake clock %v", sh.now, now)
	}
	// (c) bounded next. The design invariant is stated as now <= next <= now+interval
	// after any clock step, which is not what the code guarantees: the re-anchor
	// guard triggers on the HEAP MINIMUM, so with mixed intervals a short-interval
	// entry can still be stale on a lap where the earliest entry is not, and it
	// keeps its extra delay until a later lap catches it. That excess is bounded
	// by the step magnitude — a step of s adds exactly s to every entry's
	// next-now, and next-now <= interval beforehand — and additionally by
	// fuzzMaxInterval, because for s > fuzzMaxInterval the heap minimum is
	// necessarily stale, so the guard fires and the pass re-anchors every entry.
	// Hence the slack below, which is the real invariant, not a fudge.
	slack := min(stepMag, fuzzMaxInterval)
	for i, e := range r.entries { // entries are scheduler-owned until Run returns
		if !now.Before(e.next) {
			t.Errorf("%s: next=%v is not after now=%v", e.t.URL, e.next, now)
		}
		if d := e.next.Sub(now); d > e.interval+slack {
			t.Errorf("%s: next-now=%v exceeds interval(%v)+slack(%v)", e.t.URL, d, e.interval, slack)
		}
		// (d) phase is kept through every advance, coalesce and re-anchor.
		if off := e.next.Sub(e.next.Truncate(e.interval)); off != e.phase {
			t.Errorf("%s: next offset %v, phase %v", e.t.URL, off, e.phase)
		}
		// (g) the differential: the runner's schedule is the model's, exactly.
		if !e.next.Equal(sh.next[i]) {
			t.Errorf("%s: runner next=%v, model next=%v", e.t.URL, e.next, sh.next[i])
		}
	}

	// (f) shutdown is quiet: Run reports cancellation, Results is closed, and no
	// Result ever carried context.Canceled (take() rejects any such Err above).
	if !errors.Is(tr.err, context.Canceled) {
		t.Errorf("Run returned %v, want context.Canceled", tr.err)
	}
	if _, ok := <-tr.Results(); ok {
		t.Error("Results not closed after Run returned")
	}

	// (i) the re-anchor guard is the runner's only Info log, so the count says the
	// guard fired when — and only when — the model says. In practice this is a
	// 0-vs-1 check: the script takes at most one backward step, and one pass
	// re-anchors every stale entry, so the model never reaches 2. It still catches
	// a guard that fires spuriously or not at all.
	if got := len(logs.records(slog.LevelInfo)); got != sh.reanchors {
		t.Errorf("re-anchor Info logs = %d, model = %d", got, sh.reanchors)
	}
}

// sameError reports whether got IS want, not merely wraps it. The runner must
// report the pre-built sentinels themselves — that is what keeps the skip path
// allocation-free (TestSkipPath_ZeroAllocs) — and errors.Is alone would accept
// any future wrapper. Written through `any` so it is an identity comparison
// rather than the error comparison errorlint rightly flags elsewhere.
func sameError(got, want error) bool {
	var a, b any = got, want
	return a == b
}
