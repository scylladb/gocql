# Release controller

The release controller implements the validation and publication policy used by the manually dispatched [Release workflow](../../.github/workflows/release.yml). It publishes the root `github.com/gocql/gocql` module and the independently versioned `github.com/scylladb/gocql/lz4` module.

This program is internal release tooling. It is not included in the gocql library or installed as a service. The workflow builds one binary from its revision on `master`, stores it as a short-lived workflow artifact, and uses that same binary for every release-control job. The candidate commit therefore supplies the code being released, but not the policy used to approve or publish it.

For maintainer instructions and repository setup, see [RELEASING.md](../../RELEASING.md).

## High-level flow

```text
workflow inputs
      |
      v
  preflight -----> full Build matrix at resolved commit
      |                         |
      |                         v
      +---------------------> gate
                                |
                     validate --+--> summary only
                                |
                      publish --+--> publish --> signed tag + GitHub Release
```

The controller exposes three commands. The workflow supplies their environment and runs them in this order:

1. `preflight`
   - Requires dispatch from `master` and mode `validate` or `publish`.
   - Parses the selected module and canonical v1 SemVer.
   - Fails if an open issue has the `release-blocker` label.
   - Fetches `origin/master`, resolves `master` or an exact 40-character commit SHA, requires that commit to be reachable from `master`, and checks it out detached.
   - Validates the module path, root README version when releasing the root module, `go mod tidy -diff`, and `go mod verify`.
   - Loads the committed public signing key and verifies its committed fingerprint.
   - Inspects existing tag and GitHub Release state and chooses a recovery action.
   - Emits the immutable commit SHA and release metadata for later jobs.

2. `gate`
   - Runs after the full Build matrix at the resolved SHA.
   - Requires the resolved target to remain a full commit SHA.
   - Rechecks release blockers, the trusted public key, and remote tag/Release state.
   - Performs no mutation.

3. `publish`
   - Runs only for `publish` mode, inside the protected `release` environment.
   - Checks release blockers again immediately before mutation.
   - Rechecks remote state and succeeds without mutation if the release is already complete.
   - When no tag exists, imports the private key, creates a signed annotated tag at the exact resolved SHA, verifies it locally against the committed fingerprint, and pushes it.
   - Creates the GitHub Release with generated notes starting at the highest preceding version tag reachable from the target.
   - Polls and verifies the final tag, Release metadata, signature, target, and Latest state.

`validate` mode runs `preflight`, the full Build matrix, and `gate`. It never runs `publish`, enters the `release` environment, or receives publication credentials.

## Candidate mapping

| Input module | Required module path | Directory | Tag | Release title |
| --- | --- | --- | --- | --- |
| `root` | `github.com/gocql/gocql` | repository root | `v<version>` | `v<version>` |
| `lz4` | `github.com/scylladb/gocql/lz4` | `lz4` | `lz4/v<version>` | `lz4 v<version>` |

Versions must be bare canonical v1 SemVer, such as `1.20.0` or `1.20.0-rc.1`. A leading `v`, build metadata, missing components, leading zeroes, and major versions other than 1 are rejected.

Stable root releases are created as Latest. Root prereleases and all LZ4 releases are created with `latest=false`.

## Remote-state machine

Before and after mutation, `inspectReleaseState` reduces remote state to one of three actions:

| Tag state | Release state | Result |
| --- | --- | --- |
| Missing | Missing | `create-tag-and-release` |
| Valid | Missing | `create-release` |
| Valid | Valid | `already-complete` |
| Missing | Present | Error |
| Conflicting or unverifiable | Any | Error |

An existing tag is valid only when it is annotated, GitHub reports a verified signature, it targets the exact candidate SHA, and local GPG verification identifies the committed trusted fingerprint. An existing Release must have the expected tag, title, draft state, prerelease state, and Latest policy.

For retrying an older stable root release, it may no longer be Latest if a higher stable root release has superseded it. A newly created stable root release must become Latest before publication succeeds.

This state machine makes identical retries safe:

- Failure before tag push: retry creates tag and Release.
- Failure after tag push: retry verifies the tag and creates only the Release.
- Failure after Release creation: retry verifies completed state without mutation.
- Conflicting public state: stop; never move or replace the tag.

## Trust and credentials

- `release-signing-key.asc` and `release-signing-key.fingerprint` are embedded into the controller binary and form its signing trust anchor.
- Read-only workflow tokens query blockers, tags, and Releases during validation.
- The private signing key, its passphrase, and the repository-scoped GitHub App token are available only to the `publish` job.
- The private key must match the committed fingerprint. Tag creation uses the identity from the trusted key and a temporary isolated GnuPG home.
- The GitHub App token pushes the tag and creates the Release. Repository tag rules are expected to reject those mutations from ordinary maintainer credentials.

Any Git, GitHub API, parsing, signature, or state-verification error fails closed.

## Source layout

| File | Responsibility |
| --- | --- |
| `candidate.go` | Module mapping, SemVer parsing/comparison, module and README validation |
| `git.go` | Command execution, target resolution, GPG setup and verification, preceding-tag selection |
| `github.go` | Read-only GitHub API client, blocker check, remote-state machine, Latest policy |
| `main.go` | Command orchestration, workflow outputs/summaries, signed-tag and Release publication |
| `signing_key.go` | Embeds the committed public key and fingerprint |
| `release_test.go` | Unit and integration-style tests using fake command/API implementations |

## Local validation

Run from repository root:

```sh
go test -race ./ci/release
go test ./...
go test -C lz4 ./...
make check
```

The commands are designed for GitHub Actions and require workflow-provided environment variables, a checked-out repository, GitHub access, and GPG. Use the Release workflow rather than invoking `preflight`, `gate`, or `publish` locally against production.
