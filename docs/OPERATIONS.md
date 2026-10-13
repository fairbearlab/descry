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
`--metrics-per-target` on, and only then, two targets that share a metrics
label (exact duplicates, URLs that differ only in userinfo, query, fragment or
the normalisation described under
[Which event fields are clean](#which-event-fields-are-clean), such as
`https://h` and `https://h/`, or more than one URL that renders as
`<unparseable>`) are refused with exit 2
instead (see [Metrics and health endpoints](#metrics-and-health-endpoints)).
Targets that differ only in userinfo, query, fragment or normalisation also
share an event subject; where they are
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
| `Runner.Dropped() int64` | counter | Results discarded because `Results()` was full — i.e. the consumer stopped draining. Exported as `descry_runner_dropped_results_total` | fix the consumer; you are losing `Result`s: a dropped `Result` can be a successful check, a skipped slot, a check that returned an error, an event that could not be encoded, or a failed publish, and you lose its diagnostics and the counts fed from `Results()` |

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
| `--listen host:port` | empty (off) | serve the endpoints there (`:9090` binds every interface). Checked with `net.SplitHostPort` before the config is loaded: anything that is not `host:port` (`9090`, say), or whose port is empty (`127.0.0.1:`, `:`), not a number (`:metricz`) or above 65535, exits 2 (`:0` picks a free port). A bind failure (port in use, permission) exits 1, before the sink is opened, so no events file is created |
| `--metrics-per-target` | `true` | export the per-target series below. `false` keeps only the fleet-wide series. Only has an effect with `--listen`; set without it, descry logs a WARN |

The endpoints are unauthenticated: bind them to loopback or a private interface.

| Endpoint | Status | Notes |
|---|---|---|
| `GET /metrics` | 200 | Prometheus exposition format (text, or protobuf if the scraper asks for it; OpenMetrics is not offered), `descry_*` series only (no Go or process collectors). At most 2 scrapes in flight; a further scrape gets 503 `Limit of concurrent requests reached (2), try again later.`, sometimes with fewer than 2 visibly in flight (see below). A scrape that takes longer than 10s gets 503 `Exceeded configured timeout of 10s.` |
| `GET /healthz` | 200 `ok` | whenever the server is serving. It says the process is up, nothing about targets |
| `GET /readyz` | 503 `not ready` / 200 `ok` | 503 until the runner starts, 200 while it runs, 503 from the first shutdown signal on (and if `Run` returns for any other reason). It does **not** wait for a first observation: a target's first check is up to one interval after start (see [First fire](#first-fire-is-up-to-one-interval-after-start)) |

`HEAD` is answered wherever `GET` is; any other method gets 405 and any other
path 404. The server reads request headers for at most 5s and accepts at most
about 8 KiB of them, and reads a whole request for at most 10s. Keep-alives
are off: the server closes every connection after one response (an HTTP/1.1
response says so with `Connection: close`; an HTTP/1.0 one carries no
`Connection` header), so an idle connection cannot hold one of the connection
slots below. A response must be written within 30s, well above the 10s scrape
timeout, so a client that requests `/metrics` and then stops reading cannot
hold its connection open; it does hold one of the 2 `/metrics` slots until
then, so two such clients make every other scrape get 503 for up to 30s. At
most 64 connections are open at once; a further one waits in the kernel's
accept backlog, unanswered, until one closes.

The 2-scrape limit applies twice: to the whole request, and inside the
Prometheus handler to the gather. A scrape that times out, or whose client
disconnects, gets its request slot back at once, but its gather keeps running
and keeps a gather slot until it finishes. Until then, a new scrape can get
503 with fewer than 2 scrapes visibly in flight.

A connection holds its slot from accept until it closes. A socket that never
sends its headers holds one for up to the 5s header timeout, so 64 such
sockets can make `/healthz` and `/readyz` probes time out. Bind `--listen` to
loopback; where the port is reachable from untrusted peers, firewall it, and
prefer exec or TCP probes over HTTP liveness probes.

### Series

Per-target (labelled `url`; about seven series per target; dropped with
`--metrics-per-target=false`):

| Series | Type | Meaning |
|---|---|---|
| `descry_check_up{url}` | gauge | 1 when the last run was up, else 0 (a check that returned an error counts as 0) |
| `descry_check_status_code{url}` | gauge | last HTTP status code; 0 when the request failed (no response, or a redirect refused by the SSRF guard or by the redirect limit: the check follows at most 4 redirects, and a 5th fails the run) or when the check returned an error |
| `descry_check_latency_seconds{url}` | gauge | last run's latency, millisecond resolution; left as it was when the check returned an error |
| `descry_check_last_run_timestamp_seconds{url}` | gauge | when the last run was observed (or when the check's error was recorded), unix seconds |
| `descry_check_tls_expiry_timestamp_seconds{url}` | gauge | expiry of the certificate of the final response after redirects, unix seconds; only for targets whose final response came over TLS (so an `http://` target that redirects to `https://` has it too). Absent until the first run that reports a certificate; after that it keeps its last value when a run reports none: a failed handshake, any other run whose request failed (a timeout, a refused connection, an SSRF block, a 5th redirect), and a successful run whose final response was plain HTTP, such as an `https://` target that redirects to `http://`, which keeps the old expiry while `descry_check_up` is 1. Read it together with `last_run` |
| `descry_check_runs_total{url,status}` | counter | runs, `status` `up` or `down`; any run that was not up, including a check that returned an error, counts as `down` |

A target's series appear on its first completed run, not at startup, so a
target has none until its first check finishes: that check starts up to one
interval after start (see
[First fire](#first-fire-is-up-to-one-interval-after-start)), later if it
waits for a free worker, and can then run for up to `timeout`. That first run
creates both `runs_total` values, the one that did not fire at 0, so a target
whose first run fails makes `status="down"` appear at 1, which `increase()`
does not count. A "check that returned an error" is only possible with a
custom `Check`; the bundled HTTP check reports a down target as a down
Observation and never returns one.

Fleet-wide (always served):

| Series | Type | Meaning |
|---|---|---|
| `descry_check_duration_seconds` | histogram | latency of every completed check, in buckets (seconds) of 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30 and `+Inf`. The 30s bucket sits past the 10s default `timeout`, so a timed-out probe lands in `le="30"`, not `+Inf`. A check that returned an error is not observed; a target refused before any request is sent (a URL the parse-time SSRF check rejects) observes 0, and one blocked at dial time or on a redirect observes the time spent until then |
| `descry_check_errors_total{class}` | counter | runs whose Observation carries an error class. `class` is one of `timeout`, `connection_refused`, `dns_failure`, `tls_error`, `ssrf_blocked`, `http_error`, `unknown`, all seven exported at 0 from startup. A check that returned an error, or a class outside the set, counts as `unknown`. With the bundled HTTP check this moves in step with `runs_total{status="down"}` |
| `descry_runner_skipped_slots_total` | counter | `Runner.Skipped()`, both skip kinds, exact. Alert on this one |
| `descry_runner_dropped_results_total` | counter | `Runner.Dropped()`, exact |
| `descry_runner_result_errors_total` | counter | non-skip error `Result`s seen by the drain (publish and mapping failures); best-effort |
| `descry_runner_skips_observed_total{kind}` | counter | skips seen by the drain, `kind` `queued` or `in_flight`; best-effort, triage only |
| `descry_runner_targets` | gauge | number of configured targets, counting exact duplicates |
| `descry_build_info{version,commit}` | gauge | constant 1. `version` is the release version without the leading `v` (`0.5.0`), `commit` the full git commit; a local `go build` reports `version="dev"`, `commit="none"`. There is no date label |

The best-effort series undercount when the runner drops `Result`s; the exact
counters are the ones to alert on. The `metrics` package doc has the details,
and [contrib/prometheus](../contrib/prometheus/README.md) has ready-made
alerting rules. [examples/kubernetes](../examples/kubernetes/README.md) has
manifests that run descry with `--listen` in a cluster.

### The `url` label and shared labels

The label is `scheme://host[:port]/path`: userinfo, query and fragment are
dropped, an empty path renders as `/`, and a URL with no safe rendering
(including one with a literal `@` other than the userinfo delimiter; write it
as `%40`) becomes `<unparseable>`. The path is exported as-is, so a fleet whose
URLs carry a secret in the path should run with `--metrics-per-target=false`.

Two targets with the same label would share one series set (counters summed,
gauges last-writer-wins). So the rule is: **only with `--listen` and
per-target series on** does descry refuse to start (exit 2, before the port is
bound or the sink opened); in every other mode a shared label is at most a
startup warning. When it refuses, it names config positions rather than URLs,
and reports only the first shared label in config order, as one of these
lines (fix it and restart to see any other):

```
error: targets 1 and 2 share the metrics label "https://h/p" (labels drop userinfo, query and fragment); give each a distinct scheme, host or path, or run with --metrics-per-target=false
error: targets 2 and 5 have no safe metrics label (each renders as "<unparseable>"); fix the URL (escape a literal "@" as %40) or run with --metrics-per-target=false
```

The label comes from the target URL, not from a `url` label set in the
config, so setting one does not resolve this. A single `<unparseable>` target
is allowed. Without `--listen`, or with
`--metrics-per-target=false`, nothing is refused: exact duplicates get the
duplicate-URL warning, and URLs that differ only in userinfo, query,
fragment or normalisation run as distinct targets and get the shared-subject warning (set a
`url` label on each to tell their events apart).

### Which event fields are clean

Events and metrics are redacted in different places. The metrics `url` label
is rendered by the `metrics` package (`metrics.TargetLabel`), so it is the
same for the `descry` binary and for any program that uses
`metrics.Collector`. The event fields are rendered by the `descry` binary, not
by the engine: it sets each target's event subject to
`metrics.TargetLabel` of the target URL, and renders `data.final_url` the
same way before the event is built. A program that embeds `runner.New`
directly publishes the Observation's `Labels["url"]` as `subject` (no
subject if there is none). The bundled HTTP check copies the target's
`Labels` into its Observation, so with it that is the `url` label the program
sets on the target; a custom `Check` must carry the target's labels through
itself, or the event has no subject or carries whatever `url` label the check
put there. It also publishes the check's `Observation.FinalURL` as it is; for the bundled
HTTP check that is the URL after redirects, userinfo (password in clear) and
query included. To match the binary, set `Labels["url"] =
metrics.TargetLabel(t.URL)` and wrap the check so a non-empty `FinalURL` is
replaced by `metrics.TargetLabel(obs.FinalURL)`. In both cases a `url` label
you set yourself is published verbatim, except that the CloudEvents library
trims surrounding whitespace and omits a subject left empty, so two labels
that differ only in surrounding whitespace share a subject, and the
startup `targets share an event subject` warning does not catch that case. Log lines are a third path, through
`check.RedactURL`, which can still print a password from a misparsed URL; see
[Log lines and their levels](#log-lines-and-their-levels).

The `descry` binary applies the same rendering to the events, whether or not
`--listen` is set. It normalises as well as strips, so a consumer keyed on `subject` sees changed
values even for URLs that carried no secret: a bare host gains a trailing `/`
(`https://example.com` becomes `https://example.com/`), the path is
re-escaped (a literal space becomes `%20`), the scheme is lower-cased, and the
fragment is dropped along with userinfo and query.

| Field | Published by `descry` as |
|---|---|
| `subject` | `scheme://host[:port]/path` of the target URL, or `<unparseable>`. Equal to the metrics `url` label, so events and series join on it |
| `data.final_url` | the same rendering of the URL the response came from (the target URL itself when there was no redirect); omitted when the request failed (no response, or a redirect refused by the SSRF guard or by the redirect limit: the check follows at most 4 redirects, and a 5th fails the run) |
| `subject` from a `url` label you set (`targets[].labels.url`) | verbatim, trimmed of surrounding whitespace; omitted if that leaves it empty |
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
| WARN | `targets share an event subject; set a distinct url label to tell them apart` | startup, once per subject shared by targets with different URLs: URLs that render the same (differing only in userinfo, query, fragment or the normalisation described under [Which event fields are clean](#which-event-fields-are-clean), such as `https://h` and `https://h/`), more than one `<unparseable>` URL with no `url` label, or two targets given the same `url` label. `subject` is printed through `check.RedactURL` |
| WARN | `target URL cannot be shown safely; its event subject is <unparseable>` | startup, once per target whose URL has no safe rendering and that has no `url` label. `target` is its 1-based position in `targets`; this line does not include the URL |
| WARN | `--metrics-per-target has no effect without --listen` | startup, when `--metrics-per-target` is set explicitly and `--listen` is not |
| WARN | `admin server shutdown` | `--listen` only: in-flight requests did not finish within 5s at shutdown, so their connections were closed. The exit code is unchanged |
| WARN | `dropping results; results channel full …` | consumer not draining; rate-limited to once per default interval |
| WARN | `shutdown grace expired; …` | `WithShutdownGrace` only: `Run` returned with `workers` runs still in flight; see [Shutdown](#bounding-the-wait-withshutdowngrace) |
| ERROR | `publish failed after retries` | the sink rejected an event 3 times (linear back-off, 100ms × attempt). The observation is lost; the scheduler continues |

Lines that print a target URL (`url` above, `subject` when it comes from a
`url` label you set, and the per-run diagnostics `descry` writes to stderr)
pass it through `check.RedactURL`. It masks the password (the username
stays) and a bare username with no password, but leaves the query string as
it is, so a `?token=` is printed even in a well-formed URL. It masks nothing
in a URL with no `//` after the scheme (`https:user:pass@host` is printed as
written), nor in a password with a literal `/`, `?` or `#` whose part before
that character is empty or all digits: `url.Parse` reads that part as a port,
which moves the `@` out of the authority (`https://user:12/34@host/p`). Any
other password of that shape fails to parse, and a URL that fails to parse
prints as `<unparseable>`. A literal `/`, `?` or `#` in the username, or in a
bare token with no password (`https://tok/en@host/p`), leaves a valid host
before it, so that URL parses and is printed in full. This is a known gap, to be fixed in
`check.RedactURL`. The `skipping slot`, `publish failed` and `dropping
results` lines come from the engine, so they reach a program that embeds the
runner too; the startup warnings and the per-run stderr diagnostics come from
the `descry` binary. Keep credentials and tokens out of target URLs, or out
of URLs of those shapes at least.

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
4. The `Results()` drain completes, so a scrape still in flight then sees
   every counter. There is no linger: once step 5 begins, a new scrape is
   refused.
5. With `--listen`, the admin server shuts down: it stops accepting and waits
   up to 5s for in-flight requests; any still running then are closed, with a
   WARN, and the exit code is unchanged.
6. The file sink, if any, is closed.

The runner's side is the same for any program: cancel the context passed to
`Run`. The runner stops dispatching, waits for in-flight runs to finish, then
closes `Results()` so a draining consumer's loop exits. Targets that were
queued but not yet started are acked without running, so shutdown produces
no burst of `context.Canceled` results. No `Publish` happens after `Run`
returns — which is what makes a deferred sink `Close()` safe. If that `Close`
fails (the file sink's final flush or the file close itself), the
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
| 1 | `--listen` bind failure (port in use, permission; before the sink opens, so no events file); an admin server error, which prints `error: admin server: …` at once and cancels the run; any `Run` error other than cancellation; a file-sink `Close` failure when the code would otherwise be 0; `metrics.New` failing (also before the sink opens) |
| 2 | flag parse error; missing `--config`; `--listen` not `host:port`, or with a port that is empty, not a number or above 65535; `config.Load` error; two targets sharing a metrics label (only with `--listen` and per-target series on; otherwise at most a startup warning); invalid sink settings; invalid event `source`; the file sink cannot be opened |

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
| scrapes of `/metrics` get 503 | more than 2 scrapes in flight, a scrape over 10s, or the gather of a recent timed-out or abandoned scrape still running | fewer or slower-cadence scrapers; one Prometheus job per instance |
| every scrape of `/metrics` gets 503 for up to 30s, though your scrapers are within the limit | two clients requested `/metrics` and stopped reading; each holds a `/metrics` slot until the 30s write timeout | clears within 30s; bind `--listen` to loopback or a private interface, or firewall the port from untrusted peers |
