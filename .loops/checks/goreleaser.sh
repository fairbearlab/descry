#!/bin/sh
# d.goreleaser: the goreleaser on PATH is the one CI pins (the goreleaser-action step's version in release.yml).
# Only needed when you change .goreleaser.yaml; CI runs `goreleaser check` and a snapshot release either way.
# The env-check config gates this row with needs = ["goreleaser"], so it SKIPs where goreleaser is not
# installed (or is an asdf/mise shim with no version set); the SKIP branch below also covers running this
# script by hand without goreleaser. The pin is the first block-style `version:` inside the goreleaser-action
# step (an optional leading v, quotes, a trailing comment and CRLF are tolerated), never another step's.
# The installed GitVersion must equal the pin exactly, prerelease suffix included.
pin=$(awk -v q="'" '
  $0 ~ ("uses:[[:space:]]*[\"" q "]?goreleaser/goreleaser-action") { ga = 1; next }
  ga && /^[[:space:]]*-[[:space:]]/ { exit }
  ga && /uses:/ { exit }
  ga && /^[[:space:]]*version:/ {
    v = $0
    gsub(/\r/, "", v)
    sub(/^[[:space:]]*version:[[:space:]]*/, "", v)
    sub("^[\"" q "]", "", v)
    sub(/^v/, "", v)
    if (match(v, /^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?/)) print substr(v, 1, RLENGTH)
    exit
  }
' .github/workflows/release.yml 2>/dev/null)
if [ -z "$pin" ]; then
  echo "WARN d.goreleaser cannot parse the goreleaser pin in release.yml — fix: check the goreleaser-action step's version"
  exit 0
fi
out=$(goreleaser --version 2>/dev/null)
rc=$?
tok='[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?'
gvline=$(printf '%s\n' "$out" | grep 'GitVersion' | head -n 1)
if [ -n "$gvline" ]; then
  ver=$(printf '%s\n' "$gvline" | grep -oE "$tok" | head -n 1)
else
  ver=$(printf '%s\n' "$out" | grep -oE "$tok" | head -n 1)
fi
if [ "$rc" -ne 0 ]; then
  echo "SKIP d.goreleaser goreleaser is not runnable here (only needed for release-config changes)"
elif [ -z "$ver" ]; then
  echo "WARN d.goreleaser goreleaser on PATH but its version is unreadable; CI pins $pin — fix: reinstall goreleaser $pin"
elif [ "$ver" = "$pin" ]; then
  echo "PASS d.goreleaser goreleaser $ver matches CI's pin"
else
  echo "WARN d.goreleaser goreleaser $ver differs from CI's pin $pin — fix: install goreleaser $pin (only matters for .goreleaser.yaml changes)"
fi
exit 0
