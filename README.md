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

- DNS rebinding and resolver tricks (the resolved address is checked at dial time,
  but a malicious resolver can return a public IP for the first query and a private
  IP for subsequent ones).
- Kernel routing manipulation (e.g. policy routing that redirects public-looking
  traffic to internal hosts).
- IPv6 embedding classes not explicitly decoded (unknown future embedding schemes).
- Non-HTTP protocols tunneled over allowed ports 80/443.

For production use, run `descry` inside a network namespace or egress firewall that
enforces the isolation policy independently of this guard.

## License

Apache-2.0. See [LICENSE](LICENSE).
