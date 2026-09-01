# Security Policy

## Supported versions

descry is pre-1.0. Only the latest minor release line gets fixes.

| Version | Supported |
| ------- | --------- |
| 0.3.x   | yes       |
| < 0.3   | no        |

Once the project reaches v1.0, this table will grow a real deprecation window.
Until then, reproduce against the latest v0.3.x tag if you can — but report
regardless of the version you are on.

## Reporting a vulnerability

Use GitHub's private vulnerability reporting: open **Report a vulnerability**
under this repo's **Security** tab, or go directly to
https://github.com/fairbearlab/descry/security/advisories/new.

Please do not open a public issue for a suspected vulnerability. Private
reporting lets us assess and fix before details are public.

## What's already in place

- Dependencies are monitored by Dependabot; `govulncheck` runs on pushes to
  `main`, on pull requests targeting `main`, and weekly on a schedule.
- CodeQL analyses pushes to `main` and pull requests targeting `main`, plus a
  weekly run to catch newly published rules.
- GitHub Actions are pinned to commit SHAs and run with minimal token permissions.
- Secret scanning and push protection are enabled on the repository.
- The SSRF guard has Go fuzz targets on both layers (`assertSafeURL`,
  `isBlockedIP`) and on `check.RedactURL`. They hunt for panics and for
  disagreements between the two layers; they do **not** make the guard a
  security boundary, and nothing in the threat model above changes because of them.
  The two SSRF-layer targets (plus the scheduler's) get 60s of coverage-guided
  fuzzing on every pull request via the `perf` workflow; all fuzz seed corpora
  also run as regular tests in CI's `go test -race ./...` invocation.
- This repository is scored by [OpenSSF Scorecard](https://scorecard.dev/viewer/?uri=github.com/fairbearlab/descry).

## Response window

descry has one maintainer. Expect an acknowledgment within about a week. There
is no promised fix SLA — turnaround depends on severity and complexity — but
you'll hear back with a plan, not silence.

## Scope

Read the threat model first:
[README § Security](https://github.com/fairbearlab/descry/blob/main/README.md#security).

The SSRF guard is explicitly **best-effort, not a security boundary**. Egress
isolation is the deployer's job, not the library's. That means:

<!-- Editors: the bullets below mirror README.md § Security ("Residual risks").
     When guard behavior or the threat model changes, update both files in the
     same PR — nothing else keeps them in sync. -->
**Out of scope** — the residual risks the README already documents. These are
accepted limitations, not vulnerabilities:

- Kernel- or network-level routing manipulation that maps a public-looking
  address to an internal host *after* Layer 2's dial-time check — policy
  routing, NAT/NAT64, or a VPN/overlay redirecting an allowed IP.
- IPv6 embedding classes the guard does not claim to decode (unknown future
  embedding schemes).
- Non-HTTP protocols tunneled over the allowed ports 80/443.

Classic DNS rebinding is also out of scope, but for the opposite reason: Layer 2
re-checks the resolved socket address on every dial, so it is handled rather than
accepted. A working rebinding bypass would be a guard bug — report it.

**In scope** — any gap between what the threat model promises and what the code
does:

- An IPv6 embedding class the guard documents as decoded (`::ffff:x.x.x.x`) but
  does not actually decode.
- A Layer 1 or Layer 2 check that fails to apply where the README says it
  applies — including a dial path the `ControlContext` hook misses, such as a
  redirect hop or a Happy Eyeballs attempt.

If the guard is wrong about its own documented behavior, that's a bug we want to
know about. When in doubt, report it privately and let us triage rather than
guessing.
