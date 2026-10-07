<!-- Copyright (C) 2026 ScyllaDB -->

# Documentation workflows

[Docs / Build and publish](../.github/workflows/docs.yml) is the shared workflow for checking and publishing documentation. Set `publish: false` to build without deploying and `publish: true` to deploy the Pages site after the build passes.

| `target` | Check (`publish: false`) | Publish (`publish: true`) |
| --- | --- | --- |
| Commit SHA | Build the source at that commit, including an untagged release candidate. | Invalid. |
| Root release tag | Build the source at that tag. | Require a published root GitHub Release, then include it in the Pages site. |
| `all` | Build all versions currently selected for the site. | Rebuild and deploy the selected published versions and `/master`. |
| `master` | Build the current development source. | Refresh `/master` while keeping the published versions. |

[Docs / Publish](../.github/workflows/docs-pages.yml) accepts `all`, `master`, or a published root release tag as its manual target. On pushes to `master`, changes limited to documentation source pages select `master`; changes to docs configuration or tooling select `all`. [Docs / Build PR](../.github/workflows/docs-pr.yml) checks the pull request commit with `publish: false`.

GitHub Pages deployments replace the whole site artifact. Each publish target therefore builds a complete site, even when the requested update concerns one version or `/master`.
