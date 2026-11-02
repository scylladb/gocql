#!/bin/sh
# Build only the current v2 worktree. Preserve the parked comparison binaries.
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
output=${1:?usage: build-profile.sh OUTPUT_BINARY}
case "$output" in
    /*) ;;
    *) output="$(pwd)/$output" ;;
esac
GOWORK=off go -C "$repo/tests/driver-comparison" build -trimpath -buildvcs=false -o "$output" .
go version -m "$output"
sha256sum "$output"
