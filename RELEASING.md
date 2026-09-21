# Releasing gocql

Releases are published only by the manually dispatched **Release** workflow. Do not create release tags or GitHub Releases by hand. A Go module becomes public when its tag is pushed, so an incorrectly tagged version cannot be unpublished.

## One-time repository setup

Create GitHub App `gocql-release`, install it only on this repository, and grant repository permissions:

- Metadata: read
- Contents: read and write

Create GitHub Actions environment `release`. Configure no required reviewer. Limit deployment branches and tags to `master`. Add:

- Variable `RELEASE_APP_ID`: `gocql-release` App ID
- Secret `RELEASE_APP_PRIVATE_KEY`: App private key
- Secret `GPG_PRIVATE_KEY`: armored private key matching `ci/release/release-signing-key.asc`
- Secret `GPG_PASSPHRASE`: promoter-key passphrase

Committed trusted fingerprint: `DC4D ED58 7433 F319 EEE1 EB74 5BD1 EAD2 57F2 1B89`. Key rotation must update public-key file and fingerprint in reviewed PR before environment secret changes.

After App and workflow installation, create active tag ruleset targeting `refs/tags/v*` and `refs/tags/lz4/v*`. Restrict creation, update, deletion. Give permanent "Always allow" bypass only to `gocql-release`; no role, team, admin, or other App bypass.

Check ruleset and probe both patterns as normal maintainer:

```sh
gh api repos/scylladb/gocql/rulesets
git push origin "$(git rev-parse HEAD):refs/tags/v-invalid-ruleset-probe"
git push origin "$(git rev-parse HEAD):refs/tags/lz4/v-invalid-ruleset-probe"
```

Both pushes must fail. Commands create no local tags. If either succeeds, stop and remove probe only through audited break-glass process.

## Prepare candidate

1. Complete content-readiness checklist [#1068](https://github.com/scylladb/gocql/issues/1068). Workflow does not replace it.
2. Resolve every open `release-blocker`. Workflow checks before CI and immediately before publication. API/parsing errors stop release.
3. Merge release changes to `master`.
4. Root release: update concrete root replacement in README.md to candidate `v1.x.y`; workflow requires match.
5. Choose target `master` or a full 40-character SHA reachable from `master`. `master` is fetched and resolved once during preflight; every later job uses that immutable SHA. Other branches, abbreviated SHAs, and non-ancestors are rejected.

Release-control code and trusted public-key material come from the workflow revision on `master`, not from the candidate commit. The controller is built once and passed to later jobs as a short-lived workflow artifact. This permits releasing an older reachable commit without trusting or requiring release scripts in that commit.

Version input: bare canonical v1 SemVer, e.g. `1.20.0` or `1.20.0-rc.1`. No leading `v`. v2+, build metadata, leading zeroes, unsafe tag characters rejected. Both modules remain v1 paths without `/v2`; major release needs separate path/workflow change.

Published Go versions and source commits are immutable. Proxies/checksum databases cache tags immediately. Never move, replace, delete published tag. Correct with new version; add `retract` later if needed.

## Validate

Open **Actions → Release → Run workflow**, select `master`, enter:

- `module`: `root` or `lz4`
- `version`: bare candidate
- `target`: `master` or a full SHA
- `mode`: `validate`

Validation performs target, module, README, both blocker, recovery-state, and full Build gates (amd64, arm64, ScyllaDB, Cassandra). It never enters `release` environment, receives no App/GPG credentials, creates no tag/Release. Run summary shows requested target, resolved SHA, computed tag, release type, Latest behavior, and recovery action. Confirm resolved SHA appears in every checkout.

Mappings:

- `root`: module `github.com/gocql/gocql`, tag/title `v<version>`.
- `lz4`: module `github.com/scylladb/gocql/lz4`, tag `lz4/v<version>`, title `lz4 v<version>`.

Gate test: temporary open `release-blocker` issue must stop validation. Remove label/close issue afterward; never bypass.

## Publish

Dispatch again from `master` with same module/version and set `mode: publish`. To reproduce a validated candidate after `master` moves, copy resolved SHA from validation summary into `target`; do not enter `master`. Serialized workflow reruns every check and full Build matrix before entering `release` environment.

Actions run names include mode, module, version, and requested target, making validation and publication runs distinguishable in history.

Equivalent CLI dispatches reduce form-entry mistakes:

```sh
gh workflow run release.yml --ref master \
  -f module=root -f version=1.20.0 -f target=master \
  -f mode=validate

# Copy resolved SHA from validation summary.
TARGET_SHA=0123456789abcdef0123456789abcdef01234567
gh workflow run release.yml --ref master \
  -f module=root -f version=1.20.0 -f target="$TARGET_SHA" \
  -f mode=publish
```

Production job mints short-lived repository-scoped token (metadata-read, contents-write), imports promoter key, checks primary fingerprint, creates signed annotated tag explicitly at validated SHA, then creates Release with generated notes from selected module's preceding tag and `--verify-tag`. Stable root releases become Latest. Root prereleases and all LZ4 releases use `latest=false`.

Verify:

```sh
git fetch --tags origin
git cat-file -t v1.20.0
git rev-list -n 1 v1.20.0
git tag --verify v1.20.0
```

For LZ4 use `lz4/v1.20.0`. Object type must be `tag`; resolved commit must match requested SHA; signature must identify committed fingerprint.

## Retries and partial publication

Rerun identical inputs after transient failure:

- No tag/Release: create signed tag, push, create Release.
- Correct signed tag at exact SHA, no Release: create Release only.
- Correct tag and matching Release: verify and succeed without mutation.

Workflow fails closed for Release without tag, wrong target, lightweight/untrusted/unverified tag, conflicting title/prerelease metadata, a new release with wrong Latest behavior, or Git/GitHub/parsing failure. A historical stable root release remains valid after a newer stable release supersedes it as Latest. Never repair by moving/deleting tag. Investigate; if public state may exist, issue new version.

Release-control jobs time out after 20 minutes, build jobs after 45 minutes, and integration jobs after 120 minutes. A stuck run therefore cannot hold the globally serialized release queue indefinitely.

## LZ4 follow-up

LZ4 versioning independent. Tag must exist before parent pin changes: tag first, then pin. After successful LZ4 release, open PR updating root `go.mod` requirement and every LZ4 README version. Run `make fix-go-mod-drift` and `make check`. Never merge pin first; consumers cannot resolve untagged version.

## Break glass

Ruleset change/disable is production-impacting break glass, never release shortcut. Require explicit repository-owner approval in public tracking issue recording reason, actor, tag patterns, time window. Preserve repository/organization audit logs. Restore ruleset and rerun both maintainer probes immediately. Document every remote mutation and final tag/Release state. Blocker gates, immutable-tag rule, signature and SHA checks still apply.
