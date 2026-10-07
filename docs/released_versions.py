# Copyright (C) 2026 ScyllaDB
"""Select published root releases for the Pages version menu."""

import json
import re
import sys
from pathlib import Path


ROOT_VERSION = re.compile(r"v1\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-[0-9A-Za-z.-]+)?\Z")


def select_versions(releases, required_tag=None):
    published = [
        release
        for release in releases
        if not release["draft"]
        and release["published_at"]
        and ROOT_VERSION.fullmatch(release["tag_name"])
    ]
    published.sort(key=lambda release: release["published_at"], reverse=True)
    stable = [release for release in published if "-" not in release["tag_name"]]
    if not stable:
        raise ValueError("no published stable root release is available for docs")

    latest = max(
        stable,
        key=lambda release: tuple(map(int, ROOT_VERSION.fullmatch(release["tag_name"]).groups()[:2])),
    )["tag_name"]
    selected = published[:5]
    if latest not in {release["tag_name"] for release in selected}:
        selected.append(next(release for release in stable if release["tag_name"] == latest))
    if required_tag:
        matching = next((release for release in published if release["tag_name"] == required_tag), None)
        if matching is None:
            raise ValueError(f"{required_tag} is not a published root release")
        if required_tag not in {release["tag_name"] for release in selected}:
            selected.append(matching)
    return {
        "tags": [release["tag_name"] for release in selected],
        "latest": latest,
        "unstable": [release["tag_name"] for release in selected if "-" in release["tag_name"]],
    }


if __name__ == "__main__":
    releases = json.loads(Path(sys.argv[1]).read_text())
    required_tag = sys.argv[2] if len(sys.argv) > 2 else None
    print("DOCS_RELEASE_VERSIONS=" + json.dumps(select_versions(releases, required_tag), separators=(",", ":")))
