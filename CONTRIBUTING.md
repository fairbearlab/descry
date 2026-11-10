# Contributing

## Before you touch engine types

If your change adds a field to `Observation`, a method to `Check`, or a hook to
the runner — stop and read
[docs/ARCHITECTURE.md § Premise #4 — the seam litmus test](docs/ARCHITECTURE.md#premise-4--the-seam-litmus-test)
first. Consumer-specific needs belong in the `Labels`/`Extra` maps or in a
consumer-side `EventSink`/`Check` implementation, never in the engine's exported
types. PRs that widen the engine surface without a Premise #4 justification will
be asked to rework before review continues.

## Transport and check changes

Standing rule: a new check type, or a change to a check's transport (dialer,
TLS, keep-alives, HTTP/2, timeouts, redirects, error classification), adds its
fault cases to `checks/<dir>/verdict_matrix_test.go`, or it does not merge. A
fault case states what the check reports when the target, the path or this
host fails in that way: the verdict, the error class, and whether it is a
verdict on the target, a check that could not run (`check.ErrNotRun`), or a
run that is dropped. Where the connection mode changes the outcome, the case
runs in both modes with its own expectation. The table is the contract in
[docs/OPERATIONS.md § Verdict contract](docs/OPERATIONS.md#verdict-contract);
a change that moves a row changes the contract, and the docs move with it.

What the tests enforce, and what they do not:

- `TestEveryCheckHasVerdictMatrix` (`checks/standing_rule_test.go`) fails if a
  directory under `checks/` that holds non-test Go files has no
  `verdict_matrix_test.go`, or that file has no `func TestVerdictMatrix`. It
  names the directory and the file to create.
- `TestSafeTransportWiring` (`checks/http`) asserts the transport fields the
  verdicts depend on, by name, in fresh and reuse mode: `DisableKeepAlives`,
  `Proxy`, `TLSHandshakeTimeout`, `HTTP2`, the dialer's `Timeout`, the SSRF
  refusal in `DialContext`, and no `ClientSessionCache`. New stdlib fields do
  not break it; a changed value does, and its failure points here.
- Neither test knows whether the fault case for *your* change exists. Adding
  it is a review duty: a reviewer asks which row covers the change, and the PR
  waits until one does.

A skeleton for a new check's `verdict_matrix_test.go`:

```go
// TestVerdictMatrix is the fault table for this check; see the standing rule
// in CONTRIBUTING.md#transport-and-check-changes.
func TestVerdictMatrix(t *testing.T) {
	t.Run("target_refuses_connection", func(t *testing.T) {
		t.Parallel()
		for _, reuse := range []bool{false, true} {
			t.Run(map[bool]string{false: "fresh", true: "reuse"}[reuse], func(t *testing.T) {
				t.Parallel()
				addr := closedPort(t) // a listener opened, then closed
				obs, err := newCheck(reuse).Run(context.Background(), check.Target{URL: addr})
				if err != nil {
					t.Fatalf("err = %v, want a verdict on the target", err)
				}
				if obs.Status != check.StatusDown || obs.ErrorClass != check.ErrConnectionRefused {
					t.Fatalf("got %s/%s, want down/connection_refused", obs.Status, obs.ErrorClass)
				}
			})
		}
	})
}
```

`checks/http/verdict_matrix_test.go` is the full example, including real-time
cases (keep those `t.Parallel()`) and the shared fixtures in
`checks/http/deadconn_test.go`.

A transport change that alters what a check costs (connections, handshakes,
timeouts) also reruns the cold-connection capacity harness, which is outside
the default `go test` run, and updates
[Sizing `concurrency`](docs/OPERATIONS.md#sizing-concurrency) if the numbers
move:

```
DESCRY_CAPACITY=1 go test -run TestColdConnectionCapacity -v -timeout 15m ./checks/http/
go test -run '^$' -bench BenchmarkCheckRun -benchmem ./checks/http/
```

## Dev setup

Toolchain versions are pinned in `.tool-versions`; `asdf install` or
`mise install` gets you both. Install them by hand if you prefer — CI runs
exactly those versions.

```
git clone https://github.com/fairbearlab/descry && cd descry
go build ./...
```

No code generation, no external services needed to build or run the tests.

## bin/env-check (optional)

`bin/env-check` checks a checkout before you build or test in it: that the Go,
golangci-lint and goreleaser pins in `go.mod`, `.tool-versions` and the
workflows agree, that your local Go and golangci-lint match them, that the
module cache already has every module the tests and the `govulncheck` tool
need (the scan itself still downloads the vulnerability database), that the
files CI needs are present, and that you are working in a git worktree
(`git worktree add ../descry-<topic> -b <branch> origin/main`) rather than the
main checkout. It is a thin wrapper around the shared `loops env-check` engine
(the `loops` plugin in
[agentic-tools](https://github.com/adambware/agentic-tools)), configured in
`.loops/env-check.toml`.

- Exit 0: no FAIL. Exit 1: at least one FAIL; every FAIL and WARN line ends
  with a fix. Exit 2: a usage or config error, for example an engine older
  than the config needs; update the engine.
- **Exit 3 (`env-check: not run`) means the engine (or node) is not
  installed. You can ignore it**; the commands under
  [Before you push](#before-you-push), and CI, stay the gate.
- `bin/env-check --static` reads repo files only. In a single plain clone,
  run `ENV_CHECK_ALLOW_MAIN=1 bin/env-check`.

## Before you push

Every command below runs in CI on your PR. Run them locally first:

```
go vet ./...
go mod tidy -diff
go test -race ./...
golangci-lint run ./...
go tool govulncheck ./...
```

`go mod tidy -diff` fails on any go.mod/go.sum drift, so commit a tidy module
file set. CI also runs a `release-check` job that validates `.goreleaser.yaml`
(`goreleaser check`) and does a snapshot build with publishing and signing
skipped, so a broken release config fails the PR instead of the tag. The job
also fails if the snapshot's linux/amd64 binary is stamped as a dirty build
(anything but `vcs.modified=false` in `go version -m`). The
snapshot still builds the container image with ko and loads it into the local
Docker daemon as `goreleaser.ko.local`, so it needs a running Docker daemon;
nothing is pushed or signed. If you change `.goreleaser.yaml`, `cmd/descry` or
the build flags, run it locally (goreleaser v2.18.3, the version `ci.yml` and
`release.yml` pin; it writes `dist/`, which is gitignored; remove the
`goreleaser.ko.local` images when done):

```
goreleaser check
goreleaser release --snapshot --clean --skip=publish,sign
```

CI also runs a `rules` job, which checks and unit-tests the alerting rules in
`contrib/prometheus/` with `promtool`, and a `manifests` job, which renders
`examples/kubernetes/` with `kubectl kustomize` and validates it with
`kubeconform`. If you change either directory, run the commands in its README
([Tests](contrib/prometheus/README.md#tests),
[Validate the manifests](examples/kubernetes/README.md#validate-the-manifests)).
`go test ./metrics/` fails if a series or label the rules read is renamed, and
`go test ./cmd/descry/` fails if the example ConfigMap no longer loads.

PRs also run a perf gate (`.github/workflows/perf.yml`): a same-job `benchstat`
comparison against the merge base that fails on a ≥ +10 % ns/op regression
(p < 0.05) in the runner and sink benchmarks, the scale harness
(`runner/scale_test.go`) with its structural criteria asserted, allocation-count
guards (they skip under `-race`, so `go test -race` never exercises them), and
count-budgeted coverage-guided fuzz runs. If your change touches the runner,
sink, event encoder, metrics url label, or the SSRF guard, run the gated tests
locally before pushing:

```
go test -run 'TestScale' -v ./runner/
go test -run 'Allocs' -v ./...
go test -run '^$' -fuzz 'FuzzScheduler' -fuzztime=400000x ./runner/
go test -run '^$' -fuzz 'FuzzAssertSafeURL' -fuzztime=1200000x ./checks/http/
go test -run '^$' -fuzz 'FuzzIsBlockedIP' -fuzztime=1000000x ./checks/http/
go test -run '^$' -fuzz 'FuzzToCloudEvent' -fuzztime=1000000x ./event/
go test -run '^$' -fuzz 'FuzzTargetLabel' -fuzztime=1000000x ./metrics/
go test -run '^$' -bench . -count=10 ./runner/ ./sink/
```

The fuzz commands use an execution-count budget (`Nx`), not a wall-clock
duration (`60s`) — on the Go 1.26.x in `go.mod`, a plain duration races a
stdlib coordinator bug ([golang/go#75804](https://github.com/golang/go/issues/75804))
that can fail the run with a bare `context deadline exceeded` right at the
deadline (see the `fuzz` job comment in `.github/workflows/perf.yml`). Feel
free to use a duration for local exploration; just keep CI on the count form.

The benchmark command above measures your head only; the CI gate benchmarks the
merge base and your head on the same runner and compares them with `benchstat`,
so a local run is a sanity check, not the gate itself.

## PRs

- Branch from `main`, PR back to `main` — no direct pushes to `main`.
- Keep PRs small and focused — one concern per PR.
- Before /ship, look up changed paths in risk-zones.md and prefix the
  invocation with the row.
- Commit subjects follow the repo's existing style: `feat:`, `fix:`, `chore:`,
  `docs:` prefixes (see `git log`).
- Vulnerabilities do not go in a PR — see [SECURITY.md](SECURITY.md) for private
  reporting.
