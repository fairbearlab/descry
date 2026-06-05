# descry

A sink-agnostic, event-sourced HTTP monitoring engine. A `Check` runs against a
`Target`, produces an `Observation`, mapped to a CloudEvents 1.0 event, handed to
an `EventSink` (stdout / file in v1).

## Install

```bash
go install github.com/fairbearlab/descry/cmd/descry@latest
```

## Use as a private module

`descry` is a **private** Go module. To `require` it from another project and
`go mod download` it reproducibly, the consumer's toolchain needs two things:
bypass the public proxy/checksum DB, and authenticate to GitHub for the fetch.

**1. Tell Go the module is private** (skips proxy.golang.org and sum.golang.org,
which can't see a private repo):

```bash
go env -w GOPRIVATE=github.com/fairbearlab/*
# or per-invocation: GOPRIVATE=github.com/fairbearlab/* go mod download
```

**2. Authenticate the fetch.** Go fetches over HTTPS, so give git a token. Pick one:

- **`.netrc`** (works in CI and Docker without rewriting git config):

  ```
  machine github.com
    login <github-username>
    password <personal-access-token>
  ```
  (`~/.netrc`, mode `0600`; in Docker, mount or `COPY` it then `chmod 600`.)

- **git credential rewrite** (handy on a workstation):

  ```bash
  git config --global url."https://<token>@github.com/".insteadOf "https://github.com/"
  ```

**3. Require the version:**

```
require github.com/fairbearlab/descry v0.1.0
```

```bash
GOPRIVATE=github.com/fairbearlab/* go mod download github.com/fairbearlab/descry
```

### Token / repo access notes

- **No special repo settings are required on `descry`** beyond the token's
  identity having read access to the repo. A classic PAT needs the `repo` scope;
  a fine-grained PAT needs **Contents: read** on `fairbearlab/descry`.
- If the `fairbearlab` org **enforces SAML SSO**, the PAT must be explicitly
  **authorized for the org** (PAT settings → "Configure SSO"), or fetches 404.
- In GitHub Actions, the default `GITHUB_TOKEN` is scoped to the *current* repo
  only and cannot read a sibling private repo. Use a PAT (or a GitHub App token)
  stored as a secret and feed it via `.netrc` / the git rewrite above.

## 60-second demo (stdout)

```bash
git clone https://github.com/fairbearlab/descry
cd descry
go run ./cmd/descry --config example.yaml
# prints CloudEvents 1.0 observations to stdout on the configured interval
```

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

```bash
descry --version
# descry dev (none) unknown
```

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
