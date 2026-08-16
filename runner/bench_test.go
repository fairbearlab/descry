package runner

import (
	"context"
	"testing"
	"time"

	"github.com/fairbearlab/descry/event"
)

// benchRound measures one full scheduler round over n targets: every target
// comes due once, is dispatched to the pool, runs a nop check, maps to a
// CloudEvent, publishes to a nop sink, and reports a Result. It is the
// before/after companion of the pre-rewrite BenchmarkTick* (one tick() over n
// targets), driven by the fake clock so a round is exactly one interval.
//
// Run: go test -run '^$' -bench 'Round' -benchmem ./runner/
func benchRound(b *testing.B, n, concurrency int) {
	const iv = time.Hour // long enough that one Advance(iv) is one round
	fc := newFakeClock(b)
	r := New(&fakeCheck{}, nopSink{}, event.Config{Source: "bench"}, targetsN(n, "https://example.com"), iv, concurrency)
	r.clock = fc
	acks := make(chan struct{}, n)
	r.afterDone = func() { acks <- struct{}{} }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = r.Run(ctx); close(done) }()
	fc.waitReset(1)

	// One warm-up round so the first measured lap is steady state.
	round := func() {
		fc.Advance(iv)
		for range n {
			<-r.results
		}
		for range n {
			<-acks
		}
		fc.BlockUntil(1) // scheduler re-armed for the next round
	}
	round()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		round()
	}
	b.StopTimer()
	cancel()
	<-done
	if r.Skipped() != 0 || r.Dropped() != 0 {
		b.Fatalf("benchmark drove the scheduler wrong: skipped=%d dropped=%d", r.Skipped(), r.Dropped())
	}
}

func BenchmarkRound100Targets(b *testing.B)  { benchRound(b, 100, 32) }
func BenchmarkRound1000Targets(b *testing.B) { benchRound(b, 1000, 32) }
func BenchmarkRound1000Conc256(b *testing.B) { benchRound(b, 1000, 256) }
func BenchmarkRound10kTargets(b *testing.B)  { benchRound(b, 10_000, 64) }

// BenchmarkSkipPath measures the skip branch alone: sentinel selection, the
// disabled Debug log, and the Result send. Expected 0 allocs/op.
func BenchmarkSkipPath(b *testing.B) {
	r := New(&fakeCheck{}, nopSink{}, event.Config{Source: "bench"}, targetsN(1, "https://example.com"), time.Second, 1)
	e := r.entries[0]
	e.inflight = true
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r.skip(ctx, e)
		<-r.results
	}
}
