#!/bin/sh
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cache="$repo/.codex-cache/driver-comparison"
old=1ceaac8172bda26e8b8d701ac356982d33729b27
v2=5a07badd5f3a567f3990e0c70aba6c8d28e7fbc0
mkdir -p "$cache"
test "$(git -C "$repo" rev-parse HEAD)" = "$v2"
if ! git -C "$repo" cat-file -e "$old^{commit}"; then git -C "$repo" fetch origin "$old"; fi
if ! test -d "$cache/old-source"; then git -C "$repo" worktree add --detach "$cache/old-source" "$old"; fi
test "$(git -C "$cache/old-source" rev-parse HEAD)" = "$old"
git -C "$cache/old-source" diff --exit-code HEAD -- .
git -C "$repo" diff --exit-code HEAD -- '*.go' go.mod go.sum internal
GOWORK=off go -C "$repo/tests/driver-comparison" build -trimpath -buildvcs=false -o "$cache/v2" .
GOWORK=off go -C "$repo/tests/driver-comparison" build -trimpath -buildvcs=false -tags old -o "$cache/old" .
go version > "$cache/build.log"
go version -m "$cache/old" >> "$cache/build.log"
go version -m "$cache/v2" >> "$cache/build.log"
git -C "$repo" ls-remote origin refs/heads/master > "$cache/remote-master.txt"
