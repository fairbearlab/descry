# descry

A sink-agnostic, event-sourced HTTP monitoring engine. A `Check` runs against a
`Target`, produces an `Observation`, mapped to a CloudEvents 1.0 event, handed to
an `EventSink` (stdout / file in v1).

## Install

```bash
go install github.com/fairbearlab/descry/cmd/descry@latest
```

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
