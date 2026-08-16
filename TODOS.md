# TODOS

Parked work, with enough context to pick up cold. Each entry says what, why, where to
start, and what it depends on. Priorities: P1 next release, P2 when a consumer asks or
the next time the area is touched, P3 nice-to-have.

## Runner

### Dynamic target set (`Runner.SetTargets`)

**What:** Let a running `Runner` add, remove, and update targets in place without a
rebuild: `SetTargets([]check.Target)` (or `Add`/`Remove`) that diffs against the
scheduler's heap and leaves unchanged targets on their existing wall-clock slots.

**Why:** Consumers with live target reload today must tear down and rebuild the runner on
every change: goroutines restart, in-flight checks are cancelled mid-run, and the consumer
carries a generation supervisor. Live reload is a standard property of monitoring
schedulers (Prometheus service discovery reloads scrape targets in place); a static-slice
constructor is the unusual design. Since v0.3.0 the phase is anchored to the wall clock,
so a rebuild is cadence-neutral — this is churn reduction, not correctness.

**Context:** After the v0.3.0 scheduler rewrite (per-target intervals, heap scheduler,
single-owner scheduler goroutine, epoch-aligned phase), the heap is mutated by exactly one
goroutine, which is what makes this safe to add: a `reload chan []check.Target` handled in
the scheduler's `select`. Define update semantics explicitly: same URL + changed interval
re-phases that target only; removed targets are dropped from the heap but an in-flight run
is allowed to finish; added targets get their epoch-aligned first slot. Start at
`runner/runner.go` (scheduler loop) and `runner/helpers_test.go` (fake clock). Keep the
API generic — no consumer vocabulary.

**Effort:** M · **Priority:** P2 · **Depends on:** v0.3.0 (landed)

### Forward wall-clock step test

**What:** A runner test in which the wall clock jumps *forward* while the monotonic clock
does not (the mirror of `TestBackwardStep_ReanchorsWithinOneInterval`): the armed timer
still fires at its monotonic deadline, the scheduler finds `now` far past `next`, and the
O(1) catch-up (`next += k·interval`) must yield exactly one run and a phase-aligned next
slot, with no skip flood and no re-anchor log.

**Why:** `TestStall_OneRunNoSkipFlood` advances both clocks together (a host sleep), and
the fuzz target's step is backward-only. A forward wall step with the monotonic clock
standing still (VM restore, NTP step after boot) is the one clock-movement case with zero
coverage. `fakeClock.Step` in `runner/helpers_test.go` already supports either sign.

**Effort:** S · **Priority:** P3 · **Depends on:** none

### `testRunner.stop()` should fail, not panic, on a wedged `Run`

**What:** `runner/helpers_test.go` `(*testRunner).stop` panics after 5 s if `Run` has not
returned. Replace with a `t.Fatalf` (needs the `*testing.T` on the helper) so the test's
own diagnostic — usually the `t.Fatalf` that fired first — is what a reader sees, not a
goroutine dump from the helper.

**Why:** Today a real scheduler hang buries the message that says what actually broke.

**Effort:** S · **Priority:** P3 · **Depends on:** none

## Event

### Attribute and reduce `ToCloudEvent` allocations

**What:** Profile `event.ToCloudEvent` with `-memprofile`, attribute its 15 allocs/op, and
remove the avoidable ones.

**Why:** Measured 530 ns/op, 888 B/op, 15 allocs/op (darwin/arm64, Go 1.26). Not a
bottleneck at any realistic workload, but it is the largest per-event allocation site
and an isolated win once CI allocation guards exist to lock it in. Both sinks' `Publish`
already sit at `MarshalJSON`'s own 3-alloc floor, so this is the remaining per-event cost.

**Context:** `event/event.go` `ToCloudEvent`; benchmark in `event/bench_test.go`. A first
CPU profile was dominated by GC and scheduler noise, indicating allocation pressure rather
than a hot loop; the individual sites were never attributed. Likely candidates:
`Extra`/`Labels` map handling and repeated `SetExtension` calls on the `cloudevents.Event`.
No API change expected. Best done after a CI perf gate exists so the improvement is
guarded.

**Effort:** S · **Priority:** P3 · **Depends on:** none (prefer after the CI perf gate)

## CI

### Perf gate: same-job benchstat + structural asserts + fuzz job

**What:** A PR-triggered workflow that (1) benchmarks merge-base and head in the same job
and fails on a ≥ +10 % ns/op regression (p < 0.05) on the runner dispatch and sink
benchmarks via `benchstat` — relative only, no stored baseline; (2) runs the scale harness
(`runner/scale_test.go`, not under `-race`, `-short` off) with its structural criteria
*asserted*: healthy regime → zero skips, `Dropped()==0`, scheduler-owned goroutines ≤
concurrency + 8; saturated regime → accounting identity, no starvation, goroutines flat;
start-lateness printed, never asserted (shared runners); (3) runs `-fuzz=FuzzScheduler
-fuzztime=60s` alongside the existing SSRF fuzz job; (4) extends `AllocsPerRun` guards to
`checks/http` (`assertSafeURL`, `controlContext`), already measured.

**Why:** v0.3.0 records the numbers and prints PASS/FAIL; nothing gates them yet. Every
job is PR-triggered, not cron (idle repos get scheduled workflows auto-disabled).

**Effort:** M · **Priority:** P2 · **Depends on:** v0.3.0 (landed)

## Sink

### Batch publishing seam (`BatchSink` + `sink.NewBatcher`)

**What:** An optional, type-asserted interface, in the `io.WriterTo` idiom:

```go
// BatchSink is optionally implemented by sinks that can publish many events in one
// operation. sink.NewBatcher uses it; the runner does not know it exists.
// EventSink remains the minimal contract.
type BatchSink interface {
    EventSink
    PublishBatch(ctx context.Context, es []cloudevents.Event) error
}
```

and a sink-side wrapper `sink.NewBatcher(inner BatchSink, size int, maxDelay time.Duration)`
that accepts `Publish` per event, buffers, calls `inner.PublishBatch` on size or timer, and
flushes on `Close`. The runner is untouched (its worker shape and shutdown invariant stand).
Precedent: CloudEvents batched JSON content mode (`application/cloudevents-batch+json`,
already in `sdk-go/v2`), OTel `BatchSpanProcessor`, Kafka producer batching.

**Why:** A Postgres or Kafka consumer would want one round-trip per batch instead of per
event. A prototype measured FileSink 2.3× serial / 2.5× parallel. But descry is not
throughput-bound at any current workload, so this is not planned — it is worth doing when a
consumer asks, not speculatively.

**Context — decisions a batching PR must make, in order:**
0. **Failure surfacing, first.** With sink-side batching, `Publish` returning nil means
   "accepted into the batch", so the runner's retry ladder and `Results()` no longer see
   durable write failures. This reverses the runner's no-silent-failure property for durable
   writes; the PR must restore an equivalent consumer-visible failure path (an error
   callback on the batcher, a `Dropped()`-style counter mirroring the runner's, a `Close`
   error, or a bounded in-batcher retry that then drops with a log *and* a counter) and
   decide the fate of `runOne`'s 3-attempt ladder under batching (as-is it retries a buffer
   append and still pays up to 300 ms of backoff on a failure it cannot see — vestigial).
1. Durability contract for FileSink batching: time-based flush ceiling (e.g. ≤ 100 ms),
   size ceiling, `Sync()`? Today: flushed before `Publish` returns. A 32 KB in-memory buffer
   loses events on crash.
2. Partial-failure retry policy: "3 of 50 rows failed" — retry the whole batch, only the
   failed items (needs a per-item error return like `[]error`), or fall back to per-item
   `Publish` for the failed subset? Interaction with the existing 3-attempt ladder.
3. `Close()` ordering and flush-on-shutdown (consumer closes the batcher after `Run`
   returns; the batcher's `Close` flushes then closes `inner`).

**Effort:** M · **Priority:** P3 · **Depends on:** a consumer that needs it; v0.3.0 (landed)

## Completed

_(none yet — entries move here with the PR that closed them)_
