#!/bin/sh
# d.govulncheck: `go tool govulncheck ./...` can run, i.e. the govulncheck tool that go.mod pins
# (tool directive) builds from the module cache. Read-only and offline: env-check sets GOPROXY=off
# and GOTOOLCHAIN=local.
if ! command -v go >/dev/null 2>&1; then
  echo "SKIP d.govulncheck go not on PATH (see d.tools)"
  exit 0
fi
if out=$(GOFLAGS=-mod=readonly go tool -n govulncheck 2>&1 >/dev/null); then
  echo "PASS d.govulncheck go tool govulncheck resolves from the module cache"
  exit 0
fi
# go prints "go: downloading" progress lines before the error; report the error.
err=$(printf '%s\n' "$out" | grep -v '^go: downloading' | head -n 1)
[ -n "$err" ] || err=$(printf '%s\n' "$out" | head -n 1)
case $err in
  *"requires go >="*) fix="install the Go version go.mod pins (see a.local.go)" ;;
  *) fix="go mod download (needs network)" ;;
esac
echo "WARN d.govulncheck go tool govulncheck failed offline: $err — fix: $fix"
