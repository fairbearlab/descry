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

## Design

[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) is the producer/consumer seam
contract: what the engine owns and never delegates, what a consumer owns, and the
litmus test for whether a proposed change leaks consumer specifics into the
engine's exported types. Read it before adding a typed field to `Observation`.

## 60-second demo (stdout)

```bash
git clone https://github.com/fairbearlab/descry
cd descry
go run ./cmd/descry --config example.yaml
# prints CloudEvents 1.0 observations to stdout on the configured interval
```

Each target can override the top-level `interval` with its own `targets[].interval`
(e.g. scrape one site every 30s while the rest run on the default 10s) — see
`example.yaml`. If a target's interval is shorter than the check `timeout`, descry
logs one startup warning naming it; a slow response on that target will then
surface as `ErrSkipped` rather than blocking its next slot.

## File-sink replay demo

Edit `example.yaml` to switch the sink:

```yaml
sink: file
file_path: events.jsonl
```

Then run:

```bash
go run ./cmd/descry --config example.yaml &
sleep 15
cat events.jsonl   # one valid CloudEvent per line
```

## Version

`descry --version` prints version, commit, and build date. Release builds stamp
them via `-ldflags -X main.version=…` (see `.goreleaser.yaml`); a plain
`go build` reports the `dev` defaults.

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

## License

Apache-2.0. See [LICENSE](LICENSE).
