# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Pre-1.0, the minor version carries breaking changes.

## [0.3.0] — unreleased

### Added

- **Per-target intervals.** `check.Target.Interval` sets a target's own cadence;
  zero means "use the runner's default interval". Config gained
  `targets[].interval` (a duration string) and `cmd/descry` wires it through.
  An unparseable or negative value is a `config.Load` error. This is the standard
  design for monitoring systems (Prometheus per-job `scrape_interval`, Blackbox
  Exporter, Uptime Kuma, Checkly all expose per-target cadence).
- **Skip observability.** Skipped slots are now reported on `Results()`, not just
  counted:
  - `runner.ErrSkipped` — the prior run had **started** and was still running:
    the check is slower than the target's interval.
  - `runner.ErrSkippedQueued` — the prior run was still **queued** behind a
    saturated worker pool: `concurrency` is too small. It wraps `ErrSkipped`, so
    `errors.Is(err, runner.ErrSkipped)` is true for both; test for
    `ErrSkippedQueued` first when classifying.
- `runner.Runner.Dropped() int64` — the count of `Result`s discarded because
  `Results()` was full (the consumer stopped draining). Mirrors `Skipped()`.
  Together they close the accounting identity: per target,
  `completed + ErrSkipped + ErrSkippedQueued == slots processed` while
  `Dropped() == 0`, and fleet-wide `+ dropped` otherwise (a dropped `Result`
  takes its target with it).
- `docs/OPERATIONS.md` — operator's guide: what each skip kind and counter means,
  how to size `concurrency` from `Σ p99 / interval`, first-fire delay, restart
  gap, the `interval < timeout` warning, and a triage table. Linked from the
  README.
- `cmd/descry` logs one startup warning per target whose effective interval is
  not longer than the check `timeout`, naming the target. Not a config error — a
  fast endpoint on a tight cadence is legitimate — but slow responses on that
  target will surface as `ErrSkipped`. It also warns once per URL that appears
  more than once (duplicates are independent targets to the runner), and now
  waits for the results drain to finish before exiting so the last buffered
  diagnostics reach stderr; a second Ctrl-C after the first terminates the
  process even if a check or sink ignores its context.
- `runner.New` caps `concurrency` at the number of targets (more workers than
  targets can never be busy at once). `Run` is single-use: a second call on the
  same `Runner` returns an error instead of panicking on the closed results
  channel.

### Changed

- **Scheduler rewritten: one goroutine + a fixed worker pool, replacing a ticker
  and a goroutine per target.** Scheduler-owned goroutines are now
  O(`concurrency`) regardless of target count (measured: 10,000 targets went from
  10,007 peak goroutines to 69), and start-lateness p99 dropped from ~3.1s to
  ~158µs at that scale.
- **Breaking (behavioral): the first check of each target now happens at a stable
  per-URL offset within its interval, not immediately at start.** Phase is
  `FNV-1a-64(url) mod interval`, anchored to the wall clock, which spreads a large
  fleet across the interval instead of stampeding, and keeps a target on the same
  slot across restarts. **Health gates should allow ≥ 1 interval — 2× is
  safer — before expecting a target's first result.**
- **Breaking (behavioral): a restart shorter than one interval loses at most one
  slot per target** (gap between two observations ≤ 2× interval). Slots inside
  downtime are not made up. Health gates that already allow ≥ 2× interval are
  unaffected.
- **Breaking: `runner.New` panics on a non-positive default interval.** It
  previously panicked later, inside `Run`, from `time.NewTicker`; the message now
  names the argument. `New`'s signature is unchanged.
- Consumers that count every errored `Result` as a lost observation now count
  skips too. That is correct — a skip *is* a lost observation — but a skip says
  nothing about the target's health. Classify with `errors.Is` before alarming.
- Per-skip logging moved from Warn to Debug. `Results()` is the signal; under
  saturation the log was a flood. The results-channel-full warning stays at Warn,
  rate-limited to once per default interval.
- A wall-clock step backward now re-anchors every stale entry in one pass and logs
  once at Info, bounding the stall to one interval plus at most the step size (one
  interval exactly when the step exceeds the longest interval; see
  `docs/OPERATIONS.md`). A forward step still yields one late run per target and
  an O(1) catch-up, never a skip flood.
- `sink.StdoutSink` buffers its writer internally. The
  "flushed before `Publish` returns" contract is unchanged; the write path is now
  allocation-free (`StdoutSink.Publish` 4 → 3 allocs/op, both sinks now at
  `MarshalJSON`'s own floor). `StdoutSink` now buffers through a `bufio.Writer`
  like `FileSink` (still flushed inside the lock before `Publish` returns), and
  both sinks reset the buffer after a failed write or flush so one transient
  error of the underlying writer no longer wedges the sink for good.

**Unchanged.** `runner.New`'s signature, `Results()`, `Skipped()`, the publish
retry ladder, the `Check` and `EventSink` interfaces, and `EventSink`'s
durability contract ("flushed before `Publish` returns").

## [0.2.1] — 2026-08-14

### Changed

- Dependency bumps: `github.com/oklog/ulid/v2`, `goreleaser/goreleaser-action`,
  `actions/setup-go`, `actions/checkout`.

## [0.2.0] — 2026-08-14

### Added

- Lint, vulnerability-scanning, and coverage hygiene in CI; security policy,
  contributing guide, badges, and Dependabot.

## [0.1.2] — 2026-08-07

### Added

- `httpcheck.WithUserAgent` functional option, seam-boundary docs, and repo
  hygiene for the public release. `WithUserAgent("")` or omitting the option
  leaves net/http's stdlib User-Agent in place — the engine never impersonates a
  browser unless the caller opts in.

## [0.1.1] — 2026-06-05

### Added

- `httpcheck.WithUserAgent` functional option.

## [0.1.0] — 2026-06-03

Initial release: `Check` → `Observation` → CloudEvents 1.0 → `EventSink`
(stdout and append-only file), a bounded worker pool with per-check timeouts, the
two-layer SSRF guard, and the YAML-configured `descry` binary.

<!-- Entries for 0.1.0–0.2.1 were reconstructed from git history when this file
     was added in 0.3.0; the GitHub release notes are authoritative for those tags. -->

[0.3.0]: https://github.com/fairbearlab/descry/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/fairbearlab/descry/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/fairbearlab/descry/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/fairbearlab/descry/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/fairbearlab/descry/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/fairbearlab/descry/releases/tag/v0.1.0
