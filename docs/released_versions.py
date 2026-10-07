"""Select published root releases for the Pages version menu."""

import json
import re
import sys
from pathlib import Path


ROOT_VERSION = re.compile(r"v1\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-[0-9A-Za-z.-]+)?\Z")


def select_versions(releases):
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
    return {
        "tags": [release["tag_name"] for release in selected],
        "latest": latest,
        "unstable": [release["tag_name"] for release in selected if "-" in release["tag_name"]],
    }


if __name__ == "__main__":
    releases = json.loads(Path(sys.argv[1]).read_text())
    print("DOCS_RELEASE_VERSIONS=" + json.dumps(select_versions(releases), separators=(",", ":")))
