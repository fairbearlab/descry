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
equal intervals they share a slot). When `descry` is not serving per-target
metrics it warns once per duplicated URL at startup; with `--listen` and
`--metrics-per-target` on, two targets that share a metrics label (exact
duplicates, URLs that differ only in userinfo or query, or more than one URL
that renders as `<unparseable>`) are refused with exit 2 instead (see
[Metrics and health endpoints](#metrics-and-health-endpoints)). Targets that
differ only in query or userinfo also share an event subject; where they are
not refused, `descry` warns once per shared subject, and a `url` label on each
target (`targets[].labels.url`) tells their events apart.

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
One caveat for long intervals: the wait itself is a monotonic timer, which does
not advance while the host is suspended, so a slot armed before a suspend fires
late by up to the suspend duration (bounded by the remaining wait) rather than at
the wall-clock slot. Irrelevant at seconds-to-minutes cadences; visible only if
an interval is longer than the naps the host takes.

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
| `Result{Err: <check, mapping, or publish error>}` | channel | The check itself returned an error (only possible with a custom `Check` implementation — the bundled `httpcheck` never returns one), or the observation could not be delivered: the event could not be encoded (a payload that will not marshal — an `Extra` value such as a channel or NaN), or the sink rejected it up to 3 times (fewer if the context was cancelled mid-retry) | your check, your `Extra` payload, or your sink — **not** the target |
| `Run` returns `runner: event config: ...` | error | The event config (`Source`, `Type`) is invalid. The runner validates it once in `New` and `Run` refuses to start: `Results()` is closed with no `Result` and no check runs. The `descry` CLI checks the same config before opening the sink and exits 2 (config error) with `error: invalid event config`; the `source` in that message is printed through `check.RedactURL`, so one that does not parse shows as `<unparseable>` | fix `source` / the event config; this is a startup error, never a per-event one |
| `Result{Err: runner.ErrSkipped}` | channel | Slot skipped: the prior run **had started** and was still running. The check is slower than the target's interval | lengthen that target's interval, or shorten `timeout` |
| `Result{Err: runner.ErrSkippedQueued}` | channel | Slot skipped: the prior run was still **queued** behind a saturated worker pool and had not started. The pool is too small | raise `concurrency` (see sizing below) |
| `Runner.Skipped() int64` | counter | Total skipped slots, both kinds. Exported as `descry_runner_skipped_slots_total` | trend it; a nonzero rate in steady state means undersized |
| `Runner.Dropped() int64` | counter | Results discarded because `Results()` was full — i.e. the consumer stopped draining. Exported as `descry_runner_dropped_results_total` | fix the consumer; you are losing observations |

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

## Metrics and health endpoints

`descry --listen host:port` serves `/metrics`, `/healthz` and `/readyz` on one
address. Without `--listen` no port is opened and no collector is built.

| Flag | Default | Effect |
|---|---|---|
| `--listen host:port` | empty (off) | serve the endpoints there (`:9090` binds every interface). Checked with `net.SplitHostPort` before the config is loaded: anything that is not `host:port` (`9090`, say) exits 2. A bind failure (port in use, permission) exits 1, before the sink is opened, so no events file is created |
| `--metrics-per-target` | `true` | export the per-target series below. `false` keeps only the fleet-wide series. Only has an effect with `--listen` |

The endpoints are unauthenticated: bind them to loopback or a private interface.

| Endpoint | Status | Notes |
|---|---|---|
| `GET /metrics` | 200 | Prometheus text format, `descry_*` series only (no Go or process collectors). At most 2 scrapes in flight; a third concurrent scrape gets 503. A scrape that takes longer than 10s gets 503 |
| `GET /healthz` | 200 `ok` | whenever the server is serving. It says the process is up, nothing about targets |
| `GET /readyz` | 503 `not ready` / 200 `ok` | 503 until the runner starts, 200 while it runs, 503 from the first shutdown signal on (and if `Run` returns for any other reason). It does **not** wait for a first observation: a target's first check is up to one interval after start (see [First fire](#first-fire-is-up-to-one-interval-after-start)) |

`HEAD` is answered wherever `GET` is; any other method gets 405 and any other
path 404. The server reads request headers for at most 5s and accepts at most
about 8 KiB of them.

### Series

Per-target (labelled `url`; about seven series per target; dropped with
`--metrics-per-target=false`):

| Series | Type | Meaning |
|---|---|---|
| `descry_check_up{url}` | gauge | 1 when the last run was up, else 0 |
| `descry_check_status_code{url}` | gauge | last HTTP status code; 0 with no response |
| `descry_check_latency_seconds{url}` | gauge | last run's latency |
| `descry_check_last_run_timestamp_seconds{url}` | gauge | when the last run was observed, unix seconds |
| `descry_check_tls_expiry_timestamp_seconds{url}` | gauge | expiry of the final response's certificate, unix seconds; only for targets that report one (https) |
| `descry_check_runs_total{url,status}` | counter | runs, `status` `up` or `down` |

Fleet-wide (always served):

| Series | Type | Meaning |
|---|---|---|
| `descry_check_duration_seconds` | histogram | latency of every completed check |
| `descry_check_errors_total{class}` | counter | failed checks by error class (`timeout`, `ssrf_blocked`, …) |
| `descry_runner_skipped_slots_total` | counter | `Runner.Skipped()`, both skip kinds, exact. Alert on this one |
| `descry_runner_dropped_results_total` | counter | `Runner.Dropped()`, exact |
| `descry_runner_result_errors_total` | counter | non-skip error `Result`s seen by the drain (publish and mapping failures); best-effort |
| `descry_runner_skips_observed_total{kind}` | counter | skips seen by the drain, `kind` `queued` or `in_flight`; best-effort, triage only |
| `descry_runner_targets` | gauge | number of configured targets |
| `descry_build_info{version,commit}` | gauge | constant 1 |

The best-effort series undercount when the runner drops `Result`s; the exact
counters are the ones to alert on. The `metrics` package doc has the details.

### The `url` label and shared labels

The label is `scheme://host[:port]/path`: userinfo, query and fragment are
dropped, an empty path renders as `/`, and a URL with no safe rendering
(including one with a literal `@` other than the userinfo delimiter; write it
as `%40`) becomes `<unparseable>`. The path is exported as-is, so a fleet whose
URLs carry a secret in the path should run with `--metrics-per-target=false`.

Two targets with the same label would share one series set (counters summed,
gauges last-writer-wins), so with `--listen` and per-target series on, descry
refuses to start, naming config positions rather than URLs:

```
error: targets 1 and 2 share the metrics label "https://h/p" (labels drop userinfo, query and fragment); give each a distinct scheme, host or path, or run with --metrics-per-target=false
error: targets 2 and 5 have no safe metrics label (each renders as "<unparseable>"); fix the URL (escape a literal "@" as %40) or run with --metrics-per-target=false
```

The label comes from the target URL, not from a `url` label set in the
config, so setting one does not resolve this. A single `<unparseable>` target
is allowed. Without `--listen`, or with
`--metrics-per-target=false`, nothing is refused: exact duplicates get the
startup warning and query variants run as distinct targets.

### Which event fields are clean

The same rendering is applied to the events, whether or not `--listen` is set:

| Field | Published as |
|---|---|
| `subject` | `scheme://host[:port]/path` of the target URL, or `<unparseable>`. Equal to the metrics `url` label, so events and series join on it |
| `data.final_url` | the same rendering of the URL after redirects |
| `subject` from a `url` label you set (`targets[].labels.url`) | verbatim |
| `source` | verbatim |
| `data.extra.body` | verbatim |

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

Lower bound worth knowing: **the cadence floor is 1ms** (`config.MinInterval`).
`config.Load` rejects a top-level or per-target `interval` below it. Below
roughly a millisecond every slot is already due before the scheduler can arm
its timer, so it spins on one core and `Skipped()`/`Dropped()` climb at up to
millions per second (a 1ns target measured ~800k slots/s) behind a single
rate-limited warning. No network probe needs a cadence that tight; the formula
above will have told you the pool cannot keep up long before. The floor is
config policy — a program embedding `runner.New` directly can pass any positive
interval and owns the consequences.

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
| INFO | `admin server listening` | `--listen` only: the server is up; `addr` is the bound address |
| WARN | `check timeout exceeds interval; …` | startup, once per offending target |
| WARN | `duplicate target URL; each entry is probed independently` | startup, once per URL listed more than once; only when not serving per-target metrics (otherwise it is an exit 2) |
| WARN | `targets share an event subject; set a distinct url label to tell them apart` | startup, once per subject shared by targets with different URLs: URLs differing only in query or userinfo, or two targets given the same `url` label. `subject` is printed through `check.RedactURL` |
| WARN | `admin server shutdown` | `--listen` only: in-flight requests did not finish within 5s at shutdown, so their connections were closed. The exit code is unchanged |
| WARN | `dropping results; results channel full …` | consumer not draining; rate-limited to once per default interval |
| WARN | `shutdown grace expired; …` | `WithShutdownGrace` only: `Run` returned with `workers` runs still in flight; see [Shutdown](#bounding-the-wait-withshutdowngrace) |
| ERROR | `publish failed after retries` | the sink rejected an event 3 times (linear back-off, 100ms × attempt). The observation is lost; the scheduler continues |

Publishing is best-effort by design: bounded retry, then log and continue. The
scheduler goroutine is never blocked by a sink — a slow `Publish` holds one
worker, and the target's later slots surface as `ErrSkipped`. A sink or `Check`
that ignores its context altogether holds that worker (and, at shutdown, `Run`,
unless `WithShutdownGrace` bounds it) until it returns; the bundled `httpcheck`
honours both.

Logging itself is synchronous `slog`: the two lines the scheduler goroutine can
emit (the re-anchor Info and the rate-limited drop Warn) are written inline. If
stderr is a pipe nobody reads, that write blocks the scheduler like any other
blocked stderr write blocks a Go program — give the log a reader.

The bundled sinks write one JSON object per line and flush before `Publish`
returns. If the underlying write fails part-way (ENOSPC on a file), the sink
resets, reports the failure, and terminates the torn fragment with its own
newline before the next successful record, so readers see one unparseable line
to skip rather than two records fused. `FileSink` does the same across restarts:
a file whose last byte is not `\n` is treated as torn on open.

## Shutdown

For the `descry` binary the order is:

1. SIGINT or SIGTERM arrives (or the admin server fails, which cancels the run
   the same way).
2. `/readyz` turns 503 and the run's context is cancelled.
3. `Run` returns once in-flight runs finish.
4. The `Results()` drain completes, so a final scrape still sees every counter.
5. With `--listen`, the admin server shuts down: it stops accepting and waits
   up to 5s for in-flight requests; any still running then are closed, with a
   WARN, and the exit code is unchanged.
6. The file sink, if any, is closed.

The runner's side is the same for any program: cancel the context passed to
`Run`. The runner stops dispatching, waits for in-flight runs to finish, then
closes `Results()` so a draining consumer's loop exits. Targets that were queued but not yet started are acked without running, so
shutdown produces no burst of `context.Canceled` results. No `Publish` happens
after `Run` returns — which is what makes a deferred sink `Close()` safe. If
that `Close` fails (the file sink's final flush or the file close itself), the
`descry` binary prints `error: closing file sink: …` and exits 1 even though
`Run` itself returned cleanly, so a supervisor sees the sink failure. By
default the wait for in-flight runs is unbounded (see above); `cmd/descry`
restores default signal handling once the first signal has been received, so a
second Ctrl-C terminates the process if a check or sink will not return. A
`Runner` is single-use: a second `Run` returns an error.

### Bounding the wait: `WithShutdownGrace`

A program embedding the runner, with a custom `Check` or sink and no second-signal
escape hatch, can bound the wait:

```go
r := runner.New(chk, s, evtCfg, targets, interval, concurrency,
    runner.WithShutdownGrace(10*time.Second))
```

If runs are still in flight when the grace expires, `Run` logs

```
level=WARN msg="shutdown grace expired; returning with runs still in flight (Results left open)" workers=1 grace=10s
```

and returns `errors.Join(ctx.Err(), runner.ErrShutdownTimeout)`. That return
gives up two guarantees of the unbounded path, deliberately:

- **A straggler may still `Publish` after `Run` returns.** Closing the sink
  right after `Run` can race a late write; either make the sink's `Publish`
  safe after `Close` (the bundled `FileSink` returns an error, which the
  straggler logs), or skip the `Close` and let process exit reclaim it.
- **`Results()` is not closed**, because a straggler still reports its outcome
  on it. A consumer draining `Results()` must also stop when `Run` returns —
  `for range r.Results()` alone would wait forever.

Test for it with `errors.Is(err, runner.ErrShutdownTimeout)`; `errors.Is(err,
context.Canceled)` stays true as well. Stragglers are not killed — Go cannot —
they exit when their `Check` or `Publish` returns. Size the grace above the
check `timeout` plus the sink's worst honest `Publish` latency, so it only
fires on a run that is genuinely wedged. The `descry` binary does not set it.

## Exit codes

| Code | When |
|---|---|
| 0 | `-h`; `--version`; a clean SIGINT/SIGTERM shutdown, with or without `--listen`; `Run` returning without error |
| 1 | `--listen` bind failure (port in use, permission; before the sink opens, so no events file); an admin server error, which cancels the run and prints `error: admin server: …`; any `Run` error other than cancellation; a file-sink `Close` failure when the code would otherwise be 0; `metrics.New` failing |
| 2 | flag parse error; missing `--config`; `--listen` not `host:port`; `config.Load` error; two targets sharing a metrics label (only with `--listen` and per-target series on); invalid sink settings; invalid event `source`; the file sink cannot be opened |

An admin server that does not finish shutting down within 5s is closed
forcibly with a WARN; that does not change the exit code.

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
| `descry` exits 1 after a clean shutdown with `error: closing file sink: …` | the file sink's final flush or file close failed (e.g. ENOSPC) | check disk space and the file's tail; a torn last line is terminated on the next open |
| `/readyz` 503 right after start, or during shutdown | the runner has not started yet, or a shutdown signal has arrived | expected; readiness is not "every target observed", so gate freshness on the metrics, not on `/readyz` |
| `descry` exits 1 at startup with `error: --listen …` | the address could not be bound (port in use, permission) | free the port or pick another; no events file was created |
| `descry` exits 2 with `error: targets … share the metrics label …` or `error: targets … have no safe metrics label …` | two targets render to the same `url` metrics label (or both to `<unparseable>`) while per-target series are served | give them distinct scheme, host or path (escape a literal `@` as `%40`), or run with `--metrics-per-target=false` |
| scrapes of `/metrics` get 503 | more than 2 scrapes in flight, or a scrape over 10s | fewer or slower-cadence scrapers; one Prometheus job per instance |
