# descry

[![ci](https://github.com/fairbearlab/descry/actions/workflows/ci.yml/badge.svg)](https://github.com/fairbearlab/descry/actions/workflows/ci.yml) [![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/fairbearlab/descry/badge)](https://scorecard.dev/viewer/?uri=github.com/fairbearlab/descry) [![codecov](https://codecov.io/gh/fairbearlab/descry/graph/badge.svg)](https://codecov.io/gh/fairbearlab/descry) [![Release](https://img.shields.io/github/v/release/fairbearlab/descry?sort=semver)](https://github.com/fairbearlab/descry/releases) [![pkg.go.dev](https://pkg.go.dev/badge/github.com/fairbearlab/descry.svg)](https://pkg.go.dev/github.com/fairbearlab/descry)

> **descry** *(verb, literary)* — to catch sight of something distant or hard to make out.
>
> That is the whole job: watch remote endpoints from a distance and notice when something is wrong.

A sink-agnostic, event-sourced HTTP uptime engine. A `Check` runs against a
`Target`, produces an `Observation`, mapped to a CloudEvents 1.0 event, handed to
an `EventSink` (stdout / file in v1); tenancy and routing ride along as an opaque
`Labels` map the engine never interprets, so it owes no particular consumer
anything. It runs in production in my homelab, watching real endpoints.

The part that is more than table stakes is the SSRF guard: two layers, the second
of which checks the already-resolved socket address inside
`net.Dialer.ControlContext`, so a rebound DNS answer is blocked before `connect(2)`
rather than after. Threat model and residual risks are in [Security](#security).

## Install

As a binary:

```bash
go install github.com/fairbearlab/descry/cmd/descry@latest
```

As a library:

```bash
go get github.com/fairbearlab/descry
```

As a container image, from v0.5.0: `ghcr.io/fairbearlab/descry:<tag>`, where
`<tag>` is the release tag with its `v` (`v0.5.0`). It is a distroless static
image for linux/amd64 and linux/arm64 that runs as a non-root user, with an
SPDX SBOM attached (the SBOM is not signed; the signature below covers the
image); final releases also move `latest`.
[`examples/kubernetes/`](examples/kubernetes/) runs it in a cluster.

### Verify a release

From v0.5.0, the release workflow signs the image and the checksums file
with [cosign](https://docs.sigstore.dev/) keyless signing, so a signature
proves the file was produced by `.github/workflows/release.yml` running for
that tag in this repository. With cosign v3, for a release `v0.5.0`:

```bash
cosign verify ghcr.io/fairbearlab/descry:v0.5.0 \
  --certificate-identity 'https://github.com/fairbearlab/descry/.github/workflows/release.yml@refs/tags/v0.5.0' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com'
```

A tag can be moved, so deploy by the digest you verified, not by the tag.
Resolve the tag once:

```bash
docker buildx imagetools inspect ghcr.io/fairbearlab/descry:v0.5.0 --format '{{.Manifest.Digest}}'
```

then run the `cosign verify` above on `ghcr.io/fairbearlab/descry@sha256:<digest>`
and deploy that reference (in a kustomization, `digest:` in place of
`newTag`). `cosign verify` also prints the digest it checked, as
`critical.image.docker-manifest-digest`. The index and each per-platform
manifest are signed, so a single platform's digest verifies too.

For the release archives, download `descry_0.5.0_checksums.txt` and
`descry_0.5.0_checksums.txt.sigstore.json` from the release next to the
archives (the file names carry the version without the `v`), verify the
checksums file, then the archives against it:

```bash
cosign verify-blob \
  --certificate-identity 'https://github.com/fairbearlab/descry/.github/workflows/release.yml@refs/tags/v0.5.0' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  --bundle descry_0.5.0_checksums.txt.sigstore.json descry_0.5.0_checksums.txt &&
  shasum -a 256 --check --ignore-missing descry_0.5.0_checksums.txt
```

Look for the `<archive>: OK` line for the archive you downloaded. `shasum`
fails with "no file was verified" if the archive is missing or renamed, where
macOS's `sha256sum` exits 0 and prints nothing.

These are cosign v3 signatures: the image's are stored as OCI referrers and
the checksums file's as a Sigstore bundle, which cosign v2 and older
verifiers may report as missing, so use cosign v3.x. Releases before v0.5.0
are not signed.

## Design

[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) is the producer/consumer seam
contract: what the engine owns and never delegates, what a consumer owns, and the
litmus test for whether a proposed change leaks consumer specifics into the
engine's exported types. Read it before adding a typed field to `Observation`.

[docs/OPERATIONS.md](docs/OPERATIONS.md) is the operator's half: what
`ErrSkipped`, `ErrSkippedQueued`, `Skipped()` and `Dropped()` mean, how to size
`concurrency`, how the scheduler behaves across restarts and clock steps, what
the metrics and the `/healthz` and `/readyz` probes report, and the exit codes.
Read it before wiring up an alarm.

[CHANGELOG.md](CHANGELOG.md) tracks notable changes release over release.

## 60-second demo (stdout)

```bash
git clone https://github.com/fairbearlab/descry
cd descry
go run ./cmd/descry --config example.yaml
# prints CloudEvents 1.0 observations to stdout on the configured interval
```

Each target can override the top-level `interval` with its own `targets[].interval`
(e.g. scrape one site every 30s while the rest run on the default 10s) — see
`example.yaml`:

```yaml
targets:
  - url: https://example.com
  - url: https://status.example.com
    interval: 30s      # this target's own cadence; others use the top-level interval
```

Intervals below 1ms (`config.MinInterval`) are rejected at load: at that
cadence the scheduler can only spin.

If a target's interval is not longer than the check `timeout`, descry
logs one startup warning naming it; a slow response on that target will then
surface as `ErrSkipped` rather than blocking its next slot.

**First fire is not immediate.** Each target starts at a stable per-URL offset
within its interval (`FNV-1a-64(url) mod interval`, anchored to the wall clock),
so a large fleet spreads across the interval instead of stampeding, and a target
keeps its slot across restarts. The practical consequence: the first observation
of a target arrives up to one interval after startup, so a health or freshness
gate should allow at least 2× interval. Details and the rest of the cadence
model are in [docs/OPERATIONS.md](docs/OPERATIONS.md).

## File-sink replay demo

Edit `example.yaml` to switch the sink:

```yaml
sink: file
file_path: events.jsonl
```

Then run:

```bash
go run ./cmd/descry --config example.yaml &
sleep 35
cat events.jsonl   # one valid CloudEvent per line
```

## Metrics and health endpoints

`--listen` serves Prometheus metrics and two probes on one address. It is off by
default; without it descry opens no port.

```bash
descry --config example.yaml --listen 127.0.0.1:9090
curl -s 127.0.0.1:9090/metrics | grep '^descry_check_up'
```

| Endpoint | Answers |
|---|---|
| `GET /metrics` | the `descry_*` series in the Prometheus text format (no Go or process collectors). At most 2 scrapes run at once (a third gets 503, and so can a scrape that follows a timed-out or abandoned one while its gather finishes) and a scrape times out after 10s |
| `GET /healthz` | 200 `ok` whenever the server is serving |
| `GET /readyz` | 503 `not ready` until the runner starts, 200 while it runs, 503 again from the first shutdown signal on. It does **not** wait for a first observation (see "First fire" above) |

`HEAD` works wherever `GET` does; other methods get 405 and other paths 404.

| Flag | Default | Effect |
|---|---|---|
| `--listen host:port` | empty (off) | serve the three endpoints there. `:9090` binds every interface. An address that is not `host:port`, or whose port is empty (`127.0.0.1:`), not a number or above 65535, exits 2; a bind failure (port in use, permission) exits 1 before the sink is opened |
| `--metrics-per-target` | `true` | export the per-target `descry_check_*` series, labelled `url="scheme://host[:port]/path"`. `false` keeps only the fleet-wide series. Only has an effect with `--listen`; set without it, descry logs a WARN |

The endpoints are **unauthenticated**. Bind them to loopback or a private
interface, not a public one.

With `--listen` and per-target series on, descry refuses to start (exit 2)
when two targets share a metrics label: the label keeps only scheme, host and
path, so targets that differ only in userinfo or query, exact duplicates, and
URLs that have no safe rendering (`<unparseable>`) would merge into one series
set. Give each a distinct scheme, host or path, or run with
`--metrics-per-target=false`.

A program that embeds the engine gets the same series from the
[`metrics`](https://pkg.go.dev/github.com/fairbearlab/descry/metrics) package.
The order is fixed, because `runner.New` takes the decorated check:

```go
var failures atomic.Int64 // your Results() drain adds one per non-skip error

c, err := metrics.New(metrics.Options{ResultErrors: failures.Load})
if err != nil {
	return err
}
r := runner.New(c.Check(httpcheck.New(10*time.Second)), s, evCfg, targets, interval, concurrency)
c.SetRunner(r)
c.SetTargetCount(len(targets))
mux.Handle("GET /metrics", promhttp.HandlerFor(c.Registry(), promhttp.HandlerOpts{}))
// In the drain: errors.Is(err, runner.ErrSkippedQueued) first, then
// runner.ErrSkipped, reported with c.ObserveSkip(metrics.SkipQueued) and
// c.ObserveSkip(metrics.SkipInFlight).
```

Series, probe semantics, the shutdown order and the exit codes are in
[docs/OPERATIONS.md](docs/OPERATIONS.md#metrics-and-health-endpoints).
Ready-made alerting rules for these series are in
[contrib/prometheus](contrib/prometheus/README.md), and manifests that run
descry with `--listen` in a Kubernetes cluster are in
[examples/kubernetes](examples/kubernetes/README.md).

## Version

`descry --version` prints version, commit, and commit date (not the build
time, so a rebuild of a tag is identical). Release builds stamp them via
`-ldflags -X main.version=…` (see `.goreleaser.yaml`); a plain `go build`
reports the `dev` defaults.

## HTTP check options

`httpcheck.New` takes a timeout plus optional functional options:

```go
import httpcheck "github.com/fairbearlab/descry/checks/http"

// Default: net/http's stdlib User-Agent ("Go-http-client/1.1"), no impersonation.
c := httpcheck.New(10 * time.Second)

// Override the User-Agent (e.g. to satisfy a WAF that blocks the default UA):
c := httpcheck.New(10*time.Second, httpcheck.WithUserAgent("MyMonitor/1.0 (+https://example.com)"))
```

`WithUserAgent("")` (or omitting the option) leaves the stdlib default in place —
the engine never impersonates a browser unless the caller explicitly opts in.

## Security

The SSRF guard is **best-effort, not a security boundary**. Network egress
isolation is the real control and the deployer's responsibility.

The guard runs two layers:

1. **Parse-time (Layer 1):** A string-only allowlist/blocklist applied before any
   DNS lookup. Blocks private RFC 1918 ranges, loopback, link-local, CGNAT,
   TEST-NET ranges, reserved blocks, and non-standard ports. Literal IPv6
   addresses are decoded for known embedding classes (`::ffff:x.x.x.x`).

2. **Dial-time (Layer 2):** A `net.Dialer.ControlContext` hook runs on the
   already-resolved socket address before `connect(2)` — no TOCTOU window.
   Applies to every dial attempt including Happy Eyeballs and each redirect hop.

Residual risks the guard does **not** cover:

- Kernel/network routing manipulation that maps a public-looking address to an
  internal host *after* the Layer 2 check — e.g. policy routing, NAT/NAT64, or a
  VPN/overlay that redirects an allowed destination IP to an internal endpoint.
  (Classic DNS rebinding is *not* a residual risk here: Layer 2 re-checks the
  resolved socket address on every dial, so a later private DNS answer is blocked
  locally before connect rather than bypassing the guard.)
- IPv6 embedding classes not explicitly decoded (unknown future embedding schemes).
- Non-HTTP protocols tunneled over allowed ports 80/443.

For production use, run `descry` inside a network namespace or egress firewall that
enforces the isolation policy independently of this guard.

What descry publishes about a target URL:

- **The `--listen` port is unauthenticated.** Anyone who can reach it can read
  the metrics. Bind it to loopback or a private interface.
- **The per-target `url` metrics label** is `scheme://host[:port]/path`: userinfo,
  query and fragment are dropped, the path is kept. A target that carries a
  secret in its path (a tokenised webhook or capability URL) needs
  `--metrics-per-target=false`.
- **The event `subject` and `data.final_url`** use the same rendering, so they
  drop userinfo, query and fragment too, and a URL with no safe rendering shows
  as `<unparseable>`.
- **A `url` label you set on a target (`targets[].labels.url`) is published
  verbatim** as the event subject. It is yours to keep clean, and it is how to
  give two targets that share a rendering distinct subjects (it does not change
  the metrics label).
- **`source` and the failure body (`data.extra.body`) are not rewritten.**

## License

Apache-2.0. See [LICENSE](LICENSE).
