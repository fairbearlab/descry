# Operating descry

This is the operator's half of the contract: what the scheduler guarantees, what
the signals on `Results()` mean, and how to size a deployment so the signals stay
quiet. [ARCHITECTURE.md](ARCHITECTURE.md) covers the producer/consumer seam;
this document covers running the thing.

It applies equally to the `descry` binary and to a program that embeds
`runner.Runner` directly.

## The cadence model in one paragraph

Each target fires on its own interval (`targets[].interval`, falling back to the
top-level `interval`). Within that interval it fires at a stable offset —
`phase = FNV-1a-64(url) mod interval` — anchored to the wall clock, so 10,000
targets spread themselves evenly across the interval instead of stampeding on
every tick, and a target keeps the same slot across process restarts. (FNV is a
spreading hash, not a keyed one: a target list written by someone hostile could
pick URLs that share a slot. The pool absorbs the burst and any overflow is
visible as skips — nothing is silent — but if your target list is not your own,
know that the spread is best-effort.) One
scheduler goroutine owns a min-heap of next-fire times and hands due targets to a
fixed pool of `concurrency` workers, so goroutine count is O(concurrency), not
O(targets) — and never more workers than targets, since a target is dispatched
only while it is not already in flight. A target is never run concurrently with
itself: if it comes due while its previous run is still in flight, that slot is
**skipped** and reported. "Itself" means the configured entry: the same URL
listed twice is two independent targets, probed and reported separately (with
equal intervals they share a slot); `descry` warns once per duplicated URL at
startup.

### First fire is up to one interval after start

The first check of a target happens at its phase offset, **not** immediately at
startup. A target with a 60s interval may not be checked for 60s after `Run`
begins.

Consequence for health gates: any liveness or freshness gate that expects a
result from every target within the first interval will false-alarm on every
restart. Allow at least 2× interval; 3× is comfortable.

### Restarts lose at most one slot

Slots inside downtime are not made up — a runner cannot check a target it was not
running for. For a restart shorter than one interval, the gap between two
observations of one target is at most **two intervals** (stopped at slot−ε, back
at slot+ε, next observation at slot+interval). Longer downtime loses
⌈downtime/interval⌉ slots. Phase itself never drifts, because it is anchored to
the wall clock rather than to process start.

### Clock steps

Forward step (host sleep, VM resume): one late run per target, then normal
cadence. Catch-up is O(1) — a jump of hours produces a single run, never a flood.

Backward step (RTC wrong at boot, NTP steps back): the scheduler notices that
entries are more than one interval in the future, re-anchors them to the current
epoch slot, and logs once:

```
level=INFO msg="clock stepped back; re-anchored schedule" targets=50
```

The stall is bounded: after a step of `s`, every target fires within its
interval plus `min(s, longest interval)` — exactly one interval when the step is
longer than the longest interval (the guard is evaluated on the earliest entry;
with mixed intervals a shorter-interval target can wait one extra lap when the
earliest one is not stale). Continuous NTP slew is absorbed slot by slot and
never accumulates.

## Signals

Everything the runner knows shows up in exactly two places: the `Results()`
channel and two counters. Nothing is silent.

| Signal | Type | Means | What to do |
|---|---|---|---|
| `Result{Err: nil}` | channel | Check ran, event published | nothing |
| `Result{Err: <check, mapping, or publish error>}` | channel | The check itself returned an error (only possible with a custom `Check` implementation — the bundled `httpcheck` never returns one), or the observation could not be delivered: `event.ToCloudEvent` refused it, or the sink rejected it up to 3 times (fewer if the context was cancelled mid-retry) | your check, your event config, or your sink — **not** the target |
| `Result{Err: runner.ErrSkipped}` | channel | Slot skipped: the prior run **had started** and was still running. The check is slower than the target's interval | lengthen that target's interval, or shorten `timeout` |
| `Result{Err: runner.ErrSkippedQueued}` | channel | Slot skipped: the prior run was still **queued** behind a saturated worker pool and had not started. The pool is too small | raise `concurrency` (see sizing below) |
| `Runner.Skipped() int64` | counter | Total skipped slots, both kinds | trend it; a nonzero rate in steady state means undersized |
| `Runner.Dropped() int64` | counter | Results discarded because `Results()` was full — i.e. the consumer stopped draining | fix the consumer; you are losing observations |

### "Down" is not an error

A target that times out, refuses the connection, returns a 500, or is blocked by
the SSRF guard is a **successful check of a down target**: the bundled
`checks/http` reports it as `Observation.Status = down` with an `ErrorClass`
(`timeout`, `connection_refused`, `dns_failure`, `http_error`, `ssrf_blocked`, …)
on the published event, and returns no error. `httpcheck.Check.Run` never returns
a non-nil error.

So `Result.Err` is a narrow channel by design — it carries skips, check errors
(from a custom `Check`), mapping failures, and publish failures, and nothing
about target health. **Uptime alarms
belong on the events, not on `Results()`.** `Results()` is where you watch the
*engine*.

### Classifying skips

`ErrSkippedQueued` wraps `ErrSkipped`, so `errors.Is(err, runner.ErrSkipped)` is
true for both. **Check `ErrSkippedQueued` first** when you want them apart:

```go
for res := range r.Results() {
    switch {
    case errors.Is(res.Err, runner.ErrSkippedQueued):
        // pool too small — a capacity signal
    case errors.Is(res.Err, runner.ErrSkipped):
        // check too slow for its interval — a per-target signal
    case res.Err != nil:
        // a mapping or publish failure — your pipeline, not the target
    }
}
```

The distinction is the point: both look like "a missing observation" from the
outside, but one is fixed by adding workers and the other by changing that
target's cadence. Alarm on them separately.

A skip **is** a lost observation. A consumer that counts every errored `Result`
as a lost observation is counting correctly; a consumer that treats every errored
`Result` as *the target is down* is not — a skip says nothing about the target.

### Draining is mandatory

`Results()` is buffered at `2×len(targets)+1`, sized for the steady state of one
completion plus one skip per target. It is deliberately not unbounded: a consumer
that stops draining must not be able to stall the scheduler or grow memory
without limit. When the buffer is full the runner drops the `Result`, increments
`Dropped()`, and warns at most once per default interval:

```
level=WARN msg="dropping results; results channel full (consumer not draining)" url=... err=... dropped=17
```

`Dropped()` is the only path a `Result` can take that is not the channel, and it
is counted — so `completed + ErrSkipped + ErrSkippedQueued` equals the slots the
scheduler processed, per target, while `Dropped() == 0`; a dropped `Result` takes
its target with it, so with drops only the fleet-wide sum `+ dropped` is
checkable. If you are exporting one number, make it that identity.

## Sizing `concurrency`

Work arriving per unit time, in worker-seconds per second:

```
load = Σ over targets ( p99 check duration / that target's interval )
```

For a uniform fleet that is just **N × p99 / interval**. Set `concurrency` above
that with headroom — the p99 is not the tail, and one slow target holds a worker
for the full `timeout`.

Worked examples, both measured:

| Fleet | load | `concurrency` | Result |
|---|---|---|---|
| 10,000 targets, 10s interval, 20ms p99 | 10000 × 0.02 / 10 = **20** | 64 | zero skips, `Dropped() == 0`, 69 goroutines |
| 500 targets, 100ms interval, 20ms p99 | 500 × 0.02 / 0.1 = **100** | 64 | at 156% of capacity → ~35% of slots skipped, mostly `ErrSkippedQueued` |

The second row is what undersizing looks like from the outside: a steady stream
of `ErrSkippedQueued` and a `Skipped()` counter climbing linearly. That is the
signal to raise `concurrency` — not to lengthen intervals.

Upper bound worth knowing: a check that ignores its context holds a worker until
`timeout` expires, so the pool's worst-case throughput is
`concurrency / timeout` checks per second. Keep `timeout` tight.

Memory is not the constraint at these sizes — the scheduler heap is roughly
7–9 MB at 10,000 targets (run to run) and goroutine count stays flat at
`concurrency + 1` plus the runtime's own.

## The `interval <= timeout` warning

At startup, `descry` logs one warning per target whose effective interval is
not longer than the check `timeout` (equal counts: a response that uses its full
timeout lands exactly on the next slot):

```
level=WARN msg="check timeout exceeds interval; slow responses will surface as ErrSkipped" url=... interval=5s timeout=10s
```

This is **not** an error, and the config still loads. A fast endpoint on a tight
cadence is a legitimate configuration — the warning names the risk: if that
target ever responds slowly, its next slot arrives before the current run
finishes and surfaces as `ErrSkipped` instead of blocking. If you see the warning
and later see `ErrSkipped` for the same URL, those are the same fact twice.

Fix it by raising that target's `interval` above `timeout`, or by lowering
`timeout` — a per-check timeout longer than the cadence is rarely what you want.

## Log lines and their levels

| Level | Message | Meaning |
|---|---|---|
| DEBUG | `skipping slot; prior run in flight` | one per skipped slot. Debug on purpose — `Results()` is the signal; the log would be a flood under saturation |
| INFO | `clock stepped back; re-anchored schedule` | once per backward wall-clock step |
| WARN | `check timeout exceeds interval; …` | startup, once per offending target |
| WARN | `dropping results; results channel full …` | consumer not draining; rate-limited to once per default interval |
| ERROR | `publish failed after retries` | the sink rejected an event 3 times (linear back-off, 100ms × attempt). The observation is lost; the scheduler continues |

Publishing is best-effort by design: bounded retry, then log and continue. The
scheduler goroutine is never blocked by a sink — a slow `Publish` holds one
worker, and the target's later slots surface as `ErrSkipped`. A sink or `Check`
that ignores its context altogether holds that worker (and, at shutdown, `Run`)
until it returns; the bundled `httpcheck` honours both.

## Shutdown

Cancel the context passed to `Run`. The runner stops dispatching, waits for
in-flight runs to finish, then closes `Results()` so a draining consumer's loop
exits. Targets that were queued but not yet started are acked without running, so
shutdown produces no burst of `context.Canceled` results. No `Publish` happens
after `Run` returns — which is what makes a deferred sink `Close()` safe. The wait
for in-flight runs is unbounded by design (see above); `cmd/descry` restores
default signal handling once the first signal has been received, so a second
Ctrl-C terminates the process if a check or sink will not return. A `Runner` is
single-use: a second `Run` returns an error.

## Triage quick reference

| Symptom | Likely cause | Check |
|---|---|---|
| No results for the first minute after start | first fire is at the phase offset | expected; widen the health gate to ≥ 2× interval |
| `ErrSkippedQueued` climbing | `concurrency` below `Σ p99/interval` | raise `concurrency` |
| `ErrSkipped` on a few specific URLs | those endpoints are slower than their interval | raise their `interval`, or lower `timeout` |
| `Dropped() > 0` | consumer not draining `Results()` | fix the consumer loop; nothing else drops |
| One gap of ~2 intervals after a deploy | restart across a slot boundary | expected; the slot is not made up |
| All targets late by exactly one interval, once | wall-clock step | look for the INFO re-anchor line |
| Goroutines growing with target count | not this runner | it is O(concurrency); look at `net/http` connections |
