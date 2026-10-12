# TODOS

Parked work, with enough context to pick up cold. Each entry says what, why, where to
start, and what it depends on. Priorities: P1 next release, P2 when a consumer asks or
the next time the area is touched, P3 nice-to-have, P4 only if a trigger fires.

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

### Per-target metric series must be retired when `SetTargets` removes a target

**What:** When the planned dynamic target set (`Runner.SetTargets`, above) removes a
target, the `metrics/` decorator's per-target series for it must be deleted
(`DeleteLabelValues`, or a removal hook the runner exposes). Otherwise the last values
stay exported forever and `descry_check_up` keeps reporting a target that no longer
exists.

**Why:** Today's CLI loads a static config, so nothing is removed at runtime; a dynamic
target set changes that, and the `SetTargets` design says nothing yet about downstream
state keyed by target.

**Context:** 2026-10-11 — the `metrics/` collector caches one set of per-target children
per raw target URL and never drops them. A plain `DeleteLabelValues` is not enough: the
`url` label is shared (targets that differ only in query or fragment, every
`<unparseable>` URL, and duplicate entries of one URL all map to one label), so the
delete must wait until no remaining target maps to the label, must evict or rebuild the
affected cache entries in the same step, and must not race a removed target's in-flight
run (whose `record` would recreate the series). Add this to the `SetTargets` design when
it is written.

**Effort:** M · **Priority:** P3 · **Depends on:** `Runner.SetTargets`; `metrics/` (landed)

### `TestScale_Saturated` flakes under parallel package load

**What:** Make the accounting-identity assert in `TestScale_Saturated`
(`runner/scale_test.go`) robust when the machine is busy, or keep the test out
of the plain `go test ./...` run.

**Why:** The harness runs on a real clock and allows each target to differ by
one slot from the wall-clock expectation (the boundary slot at each end). On a
loaded machine it has failed with a worst difference of 2 ("|results −
expected slots| = 2, want ≤ 1") while the runner code was untouched, and a
retry passed. CI's test job uses `-race`, which skips the harness, but a local
`go test ./...` without `-race` or `-short` runs it and can then fail for
reasons unrelated to the change under test.

**Context:** 2026-10-12 — seen during a local pre-push run with other test
processes loading the machine; `go test ./...` runs packages in parallel, so
the 2s saturated regime competes for CPU with the other packages. Options:
derive the expected slot window from the run's actual start and cancel
timestamps rather than `t0` and `tEnd`, widen the tolerance only for slots
whose boundary fell inside the measured scheduling delay, or gate the scale
tests behind an explicit opt-in (env var or build tag) so they run only in the
perf workflow's `scale` job and through the `-run 'TestScale'` command in
CONTRIBUTING.md.

**Effort:** S · **Priority:** P3 · **Depends on:** nothing

### A check cancelled at shutdown is published as a `down` event

**What:** When `Run`'s context is cancelled, a check that is already in flight
should not produce an event (or a failed-run metric) for the target. Either
`runOne` drops the observation when `ctx.Err() != nil` after the check returns,
or the outcome is marked as cancelled so the sink and the collector can skip it.

**Why:** On SIGTERM the runner hands the cancelled context to the in-flight
check. The bundled `httpcheck` returns the cancelled request as an
`Observation` with status `down` (class `unknown` in most cases) and a nil
error, the collector records a failed run, and a sink that ignores its context
(the stdout sink does) still publishes it. Every rollout, eviction, node drain
or liveness kill that catches a check mid-run therefore emits a false `down`
event for that target, which downstream alerting cannot tell from a real
outage.

**Context:** 2026-10-12 — found while documenting the Kubernetes example, whose
README now warns about it ("A restart can log a false `down`"; drop that bullet
with the fix). Start at `runner.runOne` and `httpcheck.Run`; the fix must keep
the shutdown invariant that no `Publish` happens after `Run` returns, and
decide whether a custom check's own ctx error should still reach `Results()`.

**Effort:** S · **Priority:** P2 · **Depends on:** nothing


## Event

### Encoder docs cite `regexp.MustCompile`; should be `regexp.Compile`

**What:** Replace "regexp.MustCompile" with "regexp.Compile" in the `Encoder` and
`ToCloudEvent` doc comments (`event/event.go`), `docs/ARCHITECTURE.md`, and the
CHANGELOG Added bullet.

**Why:** `NewEncoder` returns an error and never panics; the MustCompile comparison
suggests a panic and invites skipping the error check.

**Context:** Found in the third /ship review pass on the event Encoder branch;
deferred so the ship would not hit the three-fix-cycle cap. Pure wording.

**Effort:** S · **Priority:** P3 · **Depends on:** none


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

## Metrics

### `target_id` metrics label from a target identity key

**What:** Add a stable `target_id` label to the per-target `descry_check_*` series in
`metrics/`, derived from a target identity key (defined alongside the dynamic target
set, `Runner.SetTargets`), so targets that differ only in query string or fragment stop
sharing one series set.

**Why:** The `url` label is built from scheme, host and path only (credential safety:
the label is persistent and served on an unauthenticated port), so two targets
differing only in dropped parts collide: counters sum and gauges are last-writer-wins.
The identity label must be an opaque key: assigned in config, or random at registration.
It must never be a plain hash of the URL or of its dropped parts, because, as
`metrics.TargetLabel` notes, a fast hash of a URL whose userinfo or query carries a
weak secret can be brute-forced offline. If the key has to be derived, use a keyed MAC
with a per-process secret.

**Context:** 2026-10-11 — documented in `metrics.TargetLabel`; deferred because the
identity key does not exist until the dynamic target set is designed. The CLI now
refuses to start (exit 2) when two targets share a label while it serves per-target
series, so the collision only remains for programs that embed `metrics/`; the entry
stays for them.

**Effort:** M · **Priority:** P3 · **Depends on:** a target identity key (with `Runner.SetTargets`); `metrics/` (landed)

## CLI

### Move `cmd/descry` to its own module when a CLI-only dependency raises a shared floor

**What:** Split `cmd/descry` into a nested module only when a CLI-only dependency forces
a newer version of a library the embedding consumer also pins. `metrics/` is a public
root package, so it cannot move under `cmd/descry` without breaking embedders' imports;
if prometheus/client_golang is the trigger, `metrics/` needs its own nested module
(`metrics/go.mod`, same import path, its own tags) to take it out of the root graph.

**Why:** One root module was chosen: binary-only requires enter the consumer's module
graph but are never imported, so the only cost is a raised floor on a shared
dependency. If that happens, the known escape hatch is a nested module, about a
day of work. Writing the trigger down avoids re-deriving it in a repin PR.

**Context:** 2026-10-11 — trigger = the consumer's repin PR shows a go.mod/go.sum diff
that raises a shared dependency (for example golang.org/x/sys or protobuf) because of a
require such as prometheus/client_golang (which now arrives via the root `metrics/`
package, and which `cmd/descry` also imports directly to serve `--listen`) or a future
CLI-only OTel/grpc. Precedent: the root go.mod already carries
`tool golang.org/x/vuln/cmd/govulncheck`. The move: `cmd/descry/go.mod` requiring the
root at a tagged version, `cmd/descry/vX.Y.Z` tags, and a ci.yml step (or job) that
builds and tests the nested module; no lockstep or go.sum precompute is needed because the CLI pins a published
root tag.

**Effort:** M · **Priority:** P4 · **Depends on:** a repin PR that exhibits the trigger

### No test proves `run()` wraps the production check in `sanitizedCheck`

**What:** Add a run-level test that fails if `run()` stops wrapping `httpcheck.New`
in `sanitizedCheck`, on both the plain and the `--listen` path (where the
collector decorates the wrapped check).

**Why:** The wrapper is what keeps userinfo and the query out of `data.final_url`.
Its tests build the wrapper (and, for the redirect case, the runner) themselves,
and the run-level tests only use loopback targets, which the SSRF guard refuses
before any response, so `FinalURL` is never set. Dropping the wrapper in `run()`
leaves the suite green and reopens the leak.

**Context:** 2026-10-11 — proving it needs a way to get a real `FinalURL` through
`run()`: a seam for the inner check (deliberately not added with `--listen`, to
keep `run()` free of a check seam) or a probe the guard lets through.

**Effort:** S · **Priority:** P3 · **Depends on:** nothing

## Check

### `check.RedactURL` leaves passwords in misparsed URLs

**What:** Make `check.RedactURL` mask credentials in URLs that `url.Parse`
misreads: an opaque URL such as `https:user:pass@host` (no `//` after the
scheme), and a password with a literal `/`, `?` or `#` that moves the `@` out of
the authority (`https://user:12/34@host/p` parses as host `user:12`, path
`/34@host/p`). Then have the CLI rely on it.

**Why:** `url.Parse` accepts both without error and finds no `User`, so
`RedactURL` returns them unchanged and the password is printed. The CLI passes
target URLs through `RedactURL` in its startup warnings (duplicate URL, interval
shorter than timeout, and the shared-subject warning, but only for an
operator-set `url` label of that shape; a derived subject is already safe) and
in the per-run `check failed` / `check skipped` lines on stderr, so a config
typo of either shape puts the password in the logs. The metrics label and the
derived event subject are not affected: `metrics.TargetLabel` renders both
shapes as `<unparseable>`.

**Context:** 2026-10-12 — found while reviewing the `--listen` work; the
unparseable-subject warning already names the target by config position only,
but the older lines still print the URL. Fix it in the engine (`check/check.go`)
by adopting `metrics.TargetLabel`'s rule: an opaque URL, or any literal `@`
beyond the single userinfo delimiter `url.Parse` consumed, yields the
`<unparseable>` placeholder. Keep the function idempotent and add table rows to
its tests. OPERATIONS.md ("Log lines and their levels") documents the gap until
then; drop that caveat with the fix.

**Effort:** S · **Priority:** P1 · **Depends on:** nothing

## Admin server

### Reserved capacity for health probes

**What:** Keep `/healthz` and `/readyz` answerable while the 64 admin
connection slots are taken: a separate probe listener, or a few slots reserved
for the probe paths.

**Why:** A connection holds a slot from accept until it closes, and one that
never sends its headers holds it for up to the 5s header timeout. 64 such
sockets from any peer that can reach the port make HTTP liveness probes time
out, and an orchestrator may then restart a healthy process. OPERATIONS.md
advises loopback binding, a firewall, and exec or TCP probes meanwhile.

**Context:** 2026-10-12 — the cap is enforced by the listener before any
request is read, so reserving slots by path needs a second listener (simplest:
a `--probe-listen` address) or a post-accept handoff. Start at
`cmd/descry/admin.go`.

**Effort:** M · **Priority:** P3 · **Depends on:** nothing

### Admin shutdown waits out its timeout for idle never-requested connections

**What:** At shutdown, close admin connections that are open but have not sent
a request, instead of waiting the full 5s for them and then logging the
`admin server shutdown` WARN.

**Why:** `http.Server.Shutdown` closes only idle connections; a connection
that is accepted but has not sent its headers counts as active for its first
5s, so a single half-open client (or a probe that connects and waits) delays
every shutdown by the full grace and turns a clean stop into a WARN.

**Context:** 2026-10-12 — the server has keep-alives off, so no connection is
legitimately idle after a response. Options: track accepted connections in the
limit listener and close those still in `http.StateNew` when shutdown starts,
or use `ConnState` to do the same. Start at `cmd/descry/admin.go`
(`adminServer.shutdown`).

**Effort:** S · **Priority:** P3 · **Depends on:** nothing

### No warning when `--listen` binds a wildcard or non-loopback address

**What:** Log a WARN at startup when `--listen` resolves to a wildcard (`:9090`,
`0.0.0.0`, `[::]`) or other non-loopback address, naming the bound address.

**Why:** The endpoints are unauthenticated and the docs say to bind loopback or
a private interface, but `:9090` (the shortest form) binds every interface
silently.

**Context:** 2026-10-12 — the bound address is known after `bindAdmin`; check
it with `net.IP.IsLoopback` and `IsUnspecified`. Keep it a warning, not an
error: a private interface or a container network is a legitimate target.

**Effort:** S · **Priority:** P3 · **Depends on:** nothing

### `/healthz` does not reflect scheduler progress

**What:** Let `/healthz` (or a new probe) fail when the scheduler has stopped
making progress, not only when the server stops answering.

**Why:** `/healthz` is 200 whenever the HTTP server answers, so a wedged
scheduler or a run stuck on a sink that ignores its context still looks alive
to an orchestrator.

**Context:** 2026-10-12 — a progress signal needs a heartbeat the runner exposes
(for example the last dispatch time) without coupling the engine to the CLI;
the threshold must allow for long intervals and the first fire, which is up to
one interval after start. Start at `cmd/descry/admin.go` and the runner's
public surface.

**Effort:** M · **Priority:** P3 · **Depends on:** nothing

### Admin slot-release paths that no test pins

**What:** Add tests for two slot-release rules in `cmd/descry/admin.go`:
`limitConn.CloseWrite` must not give back the listener slot (only the first
`Close` does), and `limitInFlight` must free its `/metrics` slot when the
handler panics or the client disconnects mid-response.

**Why:** Both behave correctly today (checked by probe during review), but
nothing fails if they regress. A `CloseWrite` that also released the slot would
let a half-closed connection free its slot early and push the open-connection
count past the cap; a `limitInFlight` that leaked its slot on a panic or a
dropped client would leave `/metrics` answering 503 after two such scrapes.

**Context:** 2026-10-12 — for `CloseWrite`, build a `limitConn` over a real
TCP conn with a counting `release` and assert 0 releases after `CloseWrite`
and 1 after a double `Close`. For `limitInFlight`, wrap a handler that panics
(recovered by the test or by net/http) and one whose client goes away during
the write, then assert a third request still gets 200, not 503.
`TestLimitConn_CloseWriteForwardsHalfClose` and
`TestAdminMux_MetricsSlotHeldThroughWrite` are the nearest existing tests.

**Effort:** S · **Priority:** P3 · **Depends on:** nothing

## Release

### Make the first published image public and verify its signatures

**What:** After the v0.5.0 release workflow runs, set the
`ghcr.io/fairbearlab/descry` package's visibility to public (GHCR creates a
new package private), confirm it is linked to this repository, then run the
`cosign verify` and `cosign verify-blob` commands from README.md's "Verify a
release" against v0.5.0, and `kubectl apply -k examples/kubernetes` on a kind
cluster without the local overlay.

**Why:** The push, the image signature and the checksums signature first run
on the v0.5.0 tag: the CI snapshot builds the image but skips publishing and
signing. A private package makes anonymous pulls, `cosign verify` and the
Kubernetes example fail with an authorization error.

**Context:** 2026-10-12 — if the release fails after the image push (the
image is signed before the GitHub release is created), patch forward with
v0.5.1 rather than retagging; `newTag` and the README examples then move to
the release that worked.

**Effort:** S · **Priority:** P1 · **Depends on:** the v0.5.0 release

### Verify Scorecard credits the signed releases

**What:** After v0.5.0, check the OpenSSF Scorecard result for this
repository and confirm the Signed-Releases check counts the release's
signature (`descry_<version>_checksums.txt.sigstore.json`).

**Why:** Releases are signed with keyless cosign: `docker_signs` on the image
and `signs` on the checksums file, from the release workflow's OIDC identity.
That is the one signing mechanism; build-provenance attestations
(`actions/attest-build-provenance`, SLSA provenance files) are not planned. If
Scorecard does not recognise the `.sigstore.json` bundle name, note that here
rather than adding a second signing mechanism for the score.

**Effort:** S · **Priority:** P3 · **Depends on:** the v0.5.0 release

## Completed

### Publish the container image before v0.5.0 is tagged

**What:** Publish `ghcr.io/fairbearlab/descry` images from the release workflow
(a `kos` or `dockers` section in `.goreleaser.yaml`, plus the registry login and
`packages: write` permission it needs), starting with v0.5.0.

**Why:** `examples/kubernetes/kustomization.yaml` sets `newTag: v0.5.0` for
`ghcr.io/fairbearlab/descry`, but nothing publishes that image today, so
`kubectl apply -k examples/kubernetes` ends in `ImagePullBackOff`. The example
README and the changelog say no image is published yet and build one locally
with ko; once the image exists, drop that caveat from the README, the
kustomization and deployment comments, and the changelog.

**Effort:** S · **Priority:** P1 · **Depends on:** nothing

**Completed:** 2026-10-12 — `.goreleaser.yaml` has a `kos` entry that the
release workflow pushes as `ghcr.io/fairbearlab/descry:{{ .Tag }}`, with the
`v`, so it matches `newTag: v0.5.0`, plus `latest` on final releases; the
first tag that publishes it is v0.5.0. `release.yml` logs in to GHCR with
`packages: write`. cosign signs the image and the checksums file keylessly.
The example README, kustomization and deployment comments and the changelog
now say the release workflow publishes the image from v0.5.0, and keep the
local ko build for before that release and for local changes.
`TestReleaseImageMatchesKustomization` in `cmd/descry` fails if the kos
repository and the kustomization image name drift apart, if the kos tags use
`{{ .Version }}` or lose `{{ .Tag }}`, or if `newTag` loses its `v`. The
release-day steps are tracked under Release.

### Prometheus deny-list fails open on new packages

**What:** The CI step that keeps prometheus out of the engine listed the engine
packages by hand, so a new package was unchecked until someone added it, and a
`go list` failure or an empty list could pass silently.

**Why:** The whole point of the step is that embedders who do not import `metrics/`
never link prometheus; a hand-kept list decays.

**Effort:** S · **Priority:** P2 · **Depends on:** `metrics/` (landed)

**Completed:** 2026-10-11 — the engine set is now `go list ./...` minus `metrics/`
and `cmd/`, listed and checked separately for GOOS linux, darwin and windows (so a
package that builds on one platform only is checked there); the step fails on a
`go list` error or an empty set.

### `SetRunner` typed-nil doc was wrong

**What:** `metrics.Collector.SetRunner` said a typed nil "panics at scrape time",
which read as a process crash.

**Why:** Verified on client_golang v1.25.0: the counter funcs run on the Gather
caller's goroutine, outside the registry's recover. Served through net/http each
scrape's panic is recovered per request and the client sees a dropped connection;
only a direct `Gather` call panics the caller.

**Effort:** S · **Priority:** P3 · **Depends on:** none

**Completed:** 2026-10-11 — the doc now describes that behaviour and says to pass
the value `runner.New` returned; the CLI does exactly that.

### Fuzz shadow model does not mirror the overflow self-heal

**What:** `runner/runner.go` re-derives `next` from the epoch when `k·interval` overflows
(a wall clock centuries ahead saturates `Sub` at the max `Duration`). `FuzzScheduler`'s
`shadow.lap()` did not model that branch. Unreachable at the fuzz's clock excursions
(~12.8 h forward / 25 h back), so assertion (g) exact-`next` still held; if the decoder's
advance range is ever widened past the saturation point, (g) would diverge and look like
a scheduler bug when it is a model gap. Mirrored the guard in the shadow so it stays
correct once that range widens.

**Effort:** S · **Priority:** P3 · **Depends on:** widening the fuzz clock range

**Completed:** PR #25 (2026-09-26) — `shadow.lap()` now re-derives `next` from
`slotAfter` when the saturated-`Duration` add leaves it non-advancing, mirroring
`schedule()`'s guard line-for-line. Because the fuzz cannot reach that branch
(largest advance 64·`fuzzMaxInterval`, largest step 25 h), the guard is pinned
directly by `TestShadow_SaturatedDurationSelfHeals`, the shadow-side twin of
`TestStall_SaturatedDurationSelfHeals`; removing the guard makes it spin and fail.

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

**Completed:** PR #22 (2026-09-25) — `TestForwardStep_OneRunPerTargetNoReanchor`: 1h
forward `fakeClock.Step` across 5 targets; one run each at the monotonic deadline,
phase-aligned `next`, zero skips, no Info log.

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

**Completed:** PR #18 (2026-09-01) — plus review hardening: pipefail on the bench
pipes, awk gate fails closed on zero parsed rows, alloc guards actually run in CI
(non-`-race` step), fuzz crashers uploaded as artifacts, scale harness detects
early `Run` exit and iterates all configured targets.

### `testRunner.stop()` should fail, not panic, on a wedged `Run`

**What:** `runner/helpers_test.go` `(*testRunner).stop` panics after 5 s if `Run` has not
returned. Replace with a `t.Fatalf` (needs the `*testing.T` on the helper) so the test's
own diagnostic — usually the `t.Fatalf` that fired first — is what a reader sees, not a
goroutine dump from the helper.

**Why:** Today a real scheduler hang buries the message that says what actually broke.

**Effort:** S · **Priority:** P3 · **Depends on:** none

**Completed:** PR #21 (2026-09-25) — `stop` fails through the test's `testing.TB`
after `stopWait`; `fakeCheck.wedge` (ignores ctx) and a recording-`TB` self-test.

### Bounded shutdown grace

**What:** `Run`'s shutdown wait for in-flight runs is unbounded by design: a `Check` or
sink that ignores its context holds `Run` (and `Results()` open) until it returns. Add an
optional grace deadline (a functional option or a field on `New`'s successor) after which
`Run` returns `errors.Join(ctx.Err(), ErrShutdownTimeout)`, logs how many workers were
still in flight, and does *not* close `Results()` (a straggler could still publish).

**Why:** The bundled `httpcheck` honours ctx and its own timeout, and `cmd/descry` now
restores default signal handling after the first signal so a second Ctrl-C terminates the
process. A library user with a custom sink and no such escape hatch would want the bound.
It changes the "no `Publish` after `Run` returns" guarantee for that path, so it needs its
own design note and OPERATIONS.md entry, not a drive-by.

**Effort:** S · **Priority:** P2 · **Depends on:** a consumer that needs it

**Completed:** PR #21 (2026-09-25) — `runner.WithShutdownGrace(d)` as a variadic
functional option on `New`; design note in the `runner` package doc (`# Shutdown`),
OPERATIONS.md § Shutdown → "Bounding the wait", CHANGELOG `[Unreleased]`. `Results()` stays
open on the timeout path, per the entry; `cmd/descry` does not set it.

### Cadence floor

**What:** `config.Load` rejects negative per-target intervals and non-positive top-level
ones, but nothing stops `interval: 1ns`. A sub-millisecond interval makes the scheduler
spin (every slot is due before the timer arms) and drives `Skipped()`/`Dropped()` up at
millions per second with one rate-limited warning. Reject, or clamp with a warning,
effective intervals below a documented floor (1 ms is generous for a network probe) in
`config.Load`, and state the floor in OPERATIONS.md next to the sizing formula.
`runner.New` itself stays permissive — the floor is a CLI/config policy. Raised by two
independent pre-merge reviewers (Claude adversarial, Codex): a 1 ns target measured ~800k
slots/s on one pegged core with one rate-limited warning as the only signal.

**Effort:** S · **Priority:** P2 · **Depends on:** none

**Completed:** PR #23 (2026-09-25) — rejected, not clamped: `config.Load` errors on
any effective interval below `config.MinInterval` (1ms), naming the floor; documented in
OPERATIONS.md next to the sizing formula. `runner.New` unchanged.

### Attribute and reduce `ToCloudEvent` allocations

**What:** Profile `event.ToCloudEvent` with `-memprofile`, attribute its 15 allocs/op, and
remove the avoidable ones.

**Why:** Measured 530 ns/op, 888 B/op, 15 allocs/op (darwin/arm64, Go 1.26). Not a
bottleneck at any realistic workload, but it is the largest per-event allocation site
and an isolated win once CI allocation guards exist to lock it in. Both sinks' `Publish`
already sit at `MarshalJSON`'s own 3-alloc floor, so this is the remaining per-event cost.

**Context:** `event/event.go` `ToCloudEvent`; the numbers above came from an ad-hoc benchmark that was not kept — add one at `event/bench_test.go` first. A first
CPU profile was dominated by GC and scheduler noise, indicating allocation pressure rather
than a hot loop; the individual sites were never attributed. Likely candidates:
`Extra`/`Labels` map handling and repeated `SetExtension` calls on the `cloudevents.Event`.
No API change expected. Best done after a CI perf gate exists so the improvement is
guarded.

**Effort:** S · **Priority:** P3 · **Depends on:** none (prefer after the CI perf gate)

**Completed:** event encoder PR (2026-10-05) — `event.NewEncoder(cfg)` validates the
config once and `Encode` builds per-event fields only (no per-event `Validate`);
`runner.New` builds one and `cmd/descry` checks it before opening the sink.
`ToCloudEvent` is now a one-shot wrapper. Pre-built Encoder: 8 allocs/op minimal, 13
full (was 13 / 18 for `ToCloudEvent`), guarded by `TestEncoder_Allocs` in the perf
workflow's alloc step; benchmarks in `event/bench_test.go`, equivalence oracle against
the legacy implementation in `event/equivalence_test.go` and `FuzzToCloudEvent`.

### File-sink close failure exits 0

**What:** When the deferred `fsink.Close()` in `cmd/descry/main.go` `run()` fails
(final bufio flush or `f.Close`), set the exit code to 1 instead of only printing
`error: closing file sink` to stderr.

**Why:** The buffered tail of events is lost, yet the process exits 0. `run()` now
documents 0 ok / 1 runtime failure / 2 usage-config error, so this is the one
data-loss path a supervisor or script cannot see.

**Context:** Pre-existing; raised by the /ship red team on the event Encoder branch
and deferred. Fix with a named return (`func run(...) (code int)`) and have the
deferred closure set `code = 1` when Close fails and code is 0. A test needs a seam to
inject a sink whose Close fails; none exists today.

**Effort:** S · **Priority:** P2 · **Depends on:** none

**Completed:** 2026-10-10 (`fix/cli-close-exit-and-redact-source`, the CLI fixes landed
before v0.4.0) — `run()` has a named return; the deferred file-sink Close sets `code = 1`
when it fails and the code is still 0. The seam is a package-level `newFileSink` typed
`interface{ sink.EventSink; io.Closer }`, so `TestRun_FileSinkCloseFailureExitsOne` injects
a sink whose Close fails. The Why above overstates the loss: `Publish` flushes per event, so
a failed Close loses at most the torn-line terminator; the fix surfaces a sink failure, it
does not recover events.

### Redact `source` in the invalid-event-config error

**What:** `run()` prints `error: invalid event config (source %q): ...` with the raw
configured `source`; pass it through `check.RedactURL` (or drop it) first.

**Why:** If an operator puts credentials in the source URI's userinfo part, they
land in stderr / journald at startup. Low risk: `source` is an identity URI,
not a credential slot.

**Context:** Raised by the /ship adversarial review on the event Encoder branch and
deferred. One line at the `event.NewEncoder` pre-check in `cmd/descry/main.go`;
extend `TestRun_InvalidSourceFailsBeforeSink` with a userinfo case.

**Effort:** S · **Priority:** P3 · **Depends on:** none

**Completed:** 2026-10-10 (`fix/cli-close-exit-and-redact-source`, the CLI fixes landed
before v0.4.0) — `run()` prints `check.RedactURL(cfg.Source)`. `RedactURL` itself now returns the fixed placeholder
`<unparseable>` on parse failure, covering every call site rather than this one, and the
placeholder is a fixed point so the idempotency fuzz still holds.
`TestRun_InvalidSourceFailsBeforeSink` gained a userinfo row.
