#!/bin/sh
# d.goreleaser: the goreleaser on PATH is the one CI pins (the goreleaser-action version in release.yml).
# Only needed when you change .goreleaser.yaml; CI runs `goreleaser check` and a snapshot release either way.
# The env-check config gates this row with needs = ["goreleaser"], so it SKIPs where goreleaser is not
# installed (or is an asdf/mise shim with no version set) and this script only runs when it is runnable.
pin=$(sed -n 's/^[[:space:]]*version:[[:space:]]*v\{0,1\}\([0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\).*/\1/p' \
  .github/workflows/release.yml | head -n 1)
if [ -z "$pin" ]; then
  echo "WARN d.goreleaser cannot parse the goreleaser pin in release.yml — fix: check the goreleaser-action step's version"
  exit 0
fi
out=$(goreleaser --version 2>/dev/null)
rc=$?
ver=$(printf '%s\n' "$out" | grep 'GitVersion' | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n 1)
[ -n "$ver" ] || ver=$(printf '%s\n' "$out" | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n 1)
if [ "$rc" -ne 0 ]; then
  echo "SKIP d.goreleaser goreleaser is on PATH but not runnable here (only needed for release-config changes)"
elif [ -z "$ver" ]; then
  echo "WARN d.goreleaser goreleaser on PATH but its version is unreadable; CI pins $pin — fix: reinstall goreleaser $pin"
elif [ "$ver" = "$pin" ]; then
  echo "PASS d.goreleaser goreleaser $ver matches CI's pin"
else
  echo "WARN d.goreleaser goreleaser $ver differs from CI's pin $pin — fix: install goreleaser $pin (only matters for .goreleaser.yaml changes)"
fi
exit 0
