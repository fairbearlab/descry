#!/bin/sh
# d.govulncheck: `go tool govulncheck ./...` can run, i.e. the govulncheck tool that go.mod pins
# (tool directive) builds from the module cache. Offline even when run by hand: GOPROXY=off, GONOPROXY=none
# and GOVCS=*:off stop every module download (GOPRIVATE paths included), and GOTOOLCHAIN=local stops a
# toolchain download.
if ! command -v go >/dev/null 2>&1; then
  echo "SKIP d.govulncheck go not on PATH (see d.tools)"
  exit 0
fi
if out=$(GOPROXY=off GONOPROXY=none GOVCS='*:off' GOTOOLCHAIN=local GOFLAGS=-mod=readonly \
  go tool -n govulncheck 2>&1 >/dev/null); then
  echo "PASS d.govulncheck go tool govulncheck resolves from the module cache"
  exit 0
fi
# go prints "go: downloading" progress lines before the error; report the error.
err=$(printf '%s\n' "$out" | grep -v '^go: downloading' | head -n 1)
[ -n "$err" ] || err=$(printf '%s\n' "$out" | head -n 1)
# One line of plain text: no control characters, and no fix separator go's output could use to set the fix.
err=$(printf '%s' "$err" | LC_ALL=C tr '\011' ' ' | LC_ALL=C tr -d '\000-\037\177' | sed -e 's/ -- fix:/ - fix:/g' -e 's/ — fix:/ - fix:/g')
case $err in
  *"requires go >="*) fix="install the Go version go.mod pins (see a.local.go)" ;;
  *) fix="go mod download (needs network)" ;;
esac
printf '%s\n' "WARN d.govulncheck go tool govulncheck failed offline: $err — fix: $fix"
