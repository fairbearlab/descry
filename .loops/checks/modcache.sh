#!/bin/sh
# d.modcache: every module the tests need is already in the module cache, so `make test` runs offline.
# Read-only and offline: env-check sets GOPROXY=off and GOTOOLCHAIN=local.
if ! command -v go >/dev/null 2>&1; then
  echo "SKIP d.modcache go not on PATH (see d.tools)"
  exit 0
fi
if out=$(GOFLAGS=-mod=readonly go list -deps -test ./... 2>&1 >/dev/null); then
  echo "PASS d.modcache module cache has every dependency"
  exit 0
fi
# go prints "go: downloading" progress lines before the error; report the error.
err=$(printf '%s\n' "$out" | grep -v '^go: downloading' | head -n 1)
[ -n "$err" ] || err=$(printf '%s\n' "$out" | head -n 1)
case $err in
  *"requires go >="*) fix="install the Go version go.mod pins (see a.local.go)" ;;
  *) fix="go mod download (needs network)" ;;
esac
echo "WARN d.modcache go list failed offline: $err — fix: $fix"
