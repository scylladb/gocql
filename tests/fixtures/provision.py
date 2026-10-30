#!/usr/bin/env python3
"""Verify and unpack an explicitly pinned native server distribution archive.

Only a new destination is accepted. No package manager, container storage,
service, existing database directory or global CCM cluster is modified.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import tarfile
import tempfile
import urllib.request
import zipfile


def provision(archive, destination, expected):
    archive, destination = Path(archive), Path(destination)
    with archive.open("rb") as source:
        actual = hashlib.file_digest(source, "sha256").hexdigest()
    if actual != expected:
        raise ValueError("archive SHA-256 mismatch")
    if destination.exists():
        raise ValueError("destination already exists")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".gocql-unpack-", dir=destination.parent) as temporary:
        staging = Path(temporary) / "install"
        staging.mkdir()
        if zipfile.is_zipfile(archive):
            with zipfile.ZipFile(archive) as bundle:
                for entry in bundle.infolist():
                    target = (staging / entry.filename).resolve()
                    if not target.is_relative_to(staging.resolve()) or (entry.external_attr >> 16) & 0o170000 == 0o120000:
                        raise ValueError("unsafe archive path or symlink")
                bundle.extractall(staging)
        else:
            with tarfile.open(archive) as bundle:
                bundle.extractall(staging, filter="data")
        (staging / "gocql-provision.json").write_text(json.dumps({"archive": str(archive.resolve()), "sha256": actual}, indent=2) + "\n")
        staging.rename(destination)
    return actual


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--archive", type=Path)
    source.add_argument("--url", help="explicit immutable archive URL; content digest is always required")
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--destination", required=True, type=Path)
    args = parser.parse_args()
    if args.archive:
        provision(args.archive, args.destination, args.sha256)
    else:
        with tempfile.TemporaryDirectory(prefix="gocql-download-") as temporary:
            archive = Path(temporary) / "distribution"
            with urllib.request.urlopen(args.url, timeout=60) as response, archive.open("wb") as target:
                shutil.copyfileobj(response, target)
            provision(archive, args.destination, args.sha256)


if __name__ == "__main__":
    main()
