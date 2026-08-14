# Contributing

## Before you touch engine types

If your change adds a field to `Observation`, a method to `Check`, or a hook to
the runner — stop and read
[docs/ARCHITECTURE.md § Premise #4 — the seam litmus test](docs/ARCHITECTURE.md#premise-4--the-seam-litmus-test)
first. Consumer-specific needs belong in the `Labels`/`Extra` maps or in a
consumer-side `EventSink`/`Check` implementation, never in the engine's exported
types. PRs that widen the engine surface without a Premise #4 justification will
be asked to rework before review continues.

## Dev setup

Toolchain versions are pinned in `.tool-versions`; `asdf install` or
`mise install` gets you both. Install them by hand if you prefer — CI runs
exactly those versions.

```
git clone https://github.com/fairbearlab/descry && cd descry
go build ./...
```

No code generation, no external services needed to build or run the tests.

## Before you push

Every command below runs in CI on your PR. Run them locally first:

```
go vet ./...
go test -race ./...
golangci-lint run ./...
go tool govulncheck ./...
```

## PRs

- Branch from `main`, PR back to `main` — no direct pushes to `main`.
- Keep PRs small and focused — one concern per PR.
- Commit subjects follow the repo's existing style: `feat:`, `fix:`, `chore:`,
  `docs:` prefixes (see `git log`).
- Vulnerabilities do not go in a PR — see [SECURITY.md](SECURITY.md) for private
  reporting.
