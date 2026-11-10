# risk-zones.md: descry

> DRAFT (agent-drafted 2026-10-13 from git history at main@20511645; user-reviewed 2026-10-19; zones recalibrated 2026-10-30 (red = release, secrets/egress, guardrails); the user edits, agents never do). Read by L0 triage, the /ship prefix, A6 scope judge, A7 evidence check and L7. Schema version 1.

## How to use
1. List the changed paths (`git diff --name-only <base>...`).
2. Each path takes the HIGHEST zone of every row whose glob matches it. A path no row matches is **yellow** (round up). A path is prod-touching if ANY matching row says Prod yes; its evidence classes are all classes named by the matching rows. Prod is yes only when the change itself acts on prod (deploy or release paths, live data or DB writes and migrations, prod secrets, prod config the running system reads directly, or files that are live when edited); code that reaches prod later through the normal reviewed path is no, and its Zone carries the risk.
3. The change's zone is the highest zone of any changed path. One-way changes (data migration, deleted history, published release, public post) are red whatever this file says.
4. Prefix /ship with the row from "Ship prefix" below. If any changed path is prod-touching, add L7.

## Zones
| Glob | Zone | Prod | Evidence class | Why (evidence) |
|---|---|---|---|---|
| `.github/workflows/release.yml`, `.goreleaser.yaml`, `examples/kubernetes/kustomization.yaml` | red | yes | orchestration | Release: a `v*` tag publishes binaries and the signed ghcr.io image; needs the user's personal OK on top of red review (user-confirmed). |
| `.github/workflows/**` | yellow | no | orchestration | Non-release CI and scans (dependabot.yml and .golangci.yml stay red in the guardrails row); review gate removals (coverage floor, import guard, fuzz, tidy) as routine-plus. |
| `.github/dependabot.yml`, `.golangci.yml`, `cmd/descry/release_test.go`, `.claude/**`, `CLAUDE.md`, `AGENTS.md`, `.githooks/**`, `.husky/**`, `.pre-commit-config.yaml`, `risk-zones.md`, `.loops/**`, `bin/env-check` | red | no | config | CI gate config and guards: dependabot opens the bumps of SHA-pinned actions and go.mod; .golangci.yml enables gosec over the SSRF and file code; release_test.go (652 lines) is the only automated guard on the signing pipeline (TestReleaseSigningContract, image-vs-kustomization tag, dirty-stamp check; 2051164). Agent and hook config is always red: none tracked today or in any history (.claude/ is gitignored); defensive. risk-zones.md is this triage policy: one edit can downgrade every path, so it is red. `.loops/**` and `bin/env-check` are the contributor and agent env-check harness (the shared loops engine's config, its local check scripts and the wrapper); agents run it before every edit, so it is guardrail config like the agent rows. |
| `**/.env*`, `**/*secret*`, `**/*credential*`, `**/*migrat*` | red | yes | config | Always red: secrets and migrations (none tracked today). |
| `contrib/prometheus/**`, `examples/kubernetes/**` | yellow | no | config | Example ops manifests operators copy; the user deploys none; the kustomization tag is Prod via the release row. |
| `check/check.go`, `metrics/{label,metrics}.go`, `runner/runner.go`, `cmd/descry/main.go`, `checks/http/{ssrf,http}.go`, `cmd/descry/admin.go`, `docs/internal/**` | red | no | none | Credential redaction, SSRF guard and unauthenticated endpoints; red on merit for good (user-confirmed). |
| `config/config.go`, `event/event.go`, `sink/file.go`, `metrics/doc.go` | yellow | no | none | Shipped behaviour at the next tag; event.go is a pinned downstream contract (user-confirmed). |
| `go.mod`, `go.sum`, `.tool-versions`, `.gitignore`, `codecov.yml` | yellow | no | config | Toolchain, deps and repo hygiene; escalate a major bump or a Go security fix by hand. |
| `README.md`, `SECURITY.md`, `example.yaml` | yellow | no | none | Public on merge: cosign verify steps, vuln-report channel and the shipped sample config; no running system reads them. |
| `cmd/descry/{examples,eventurl,admin,main,collision}_test.go`, `metrics/{rules,label,metrics}_test.go`, `check/*_test.go`, `checks/http/*_test.go`, `runner/runner_test.go` | yellow | no | none | Tests that guard red contracts; weakening one loosens a red file. |
| `sink/sink.go` | green | no | none | Small and well tested. |
| `**/*_test.go`, `**/*.md`, `docs/**`, `LICENSE` | green | no | none | Other tests and docs; rows above win. |

Note: `{a,b}` expands as in bash. No tracked path on main falls to the yellow default today. All 37 commits on origin/main (2026-05-26 to 2026-10-13) fall inside 180 days; the stale branch claude/perf-t8-ci-gate (pre-squash PR #18) was ignored.

## Ship prefix
- red: "/ship — red zone (<category>): full review + <one extra review chosen at L0>"
- yellow: "/ship — yellow zone: routine review, red-team focus on <area>"
- green: "/ship" (routine)
- docs-only: "/ship — docs only: minimal review"
- trivial S0: "Minimal ship. Skip the in-depth review."

## CLAUDE.md line (to add to the repo)
"Before /ship, look up changed paths in risk-zones.md and prefix the invocation with the row."

## Open questions for the user
None open. All 7 answered or settled from the repo on 2026-10-19.
