<!-- Copyright (C) 2026 ScyllaDB -->

# Documentation workflows

[Docs / Build](../.github/workflows/docs.yml) is the shared read-only workflow for checking documentation and preparing the Pages artifact. Set `publish: false` to validate without uploading a deployment artifact and `publish: true` to build the complete site and upload its deployment artifact. Separate deployment jobs in [Docs / Publish](../.github/workflows/docs-pages.yml) and [Release](../.github/workflows/release.yml) deploy that artifact after their builds pass; only those deployment jobs receive Pages and OIDC write permissions. The release deployment runs only in publish mode, after the release and its documentation build succeed. PR builds require only read access to repository contents.

| `target` | Check (`publish: false`) | Publish (`publish: true`) |
| --- | --- | --- |
| Commit SHA | Build the source at that commit, including an untagged release candidate. | Invalid. |
| Root release tag | Build the source at that tag. | Require a published root GitHub Release, then include it in the Pages site. |
| `all` | Build all versions currently selected for the site. | Rebuild and deploy the selected published versions and `/master`. |
| `master` | Build the current development source. | Refresh `/master` while keeping the published versions. |

[Docs / Publish](../.github/workflows/docs-pages.yml) accepts `all`, `master`, or a published root release tag as its manual target. On pushes to `master`, changes limited to documentation source pages select `master`; changes to docs configuration or tooling select `all`. [Docs / Build PR](../.github/workflows/docs-pr.yml) checks the pull request commit with `publish: false`.

GitHub Pages deployments replace the whole site artifact. Each publish target therefore builds a complete site, even when the requested update concerns one version or `/master`.
