#!/usr/bin/env python3
"""Check source pinning, evidence links, inventories, and review artifacts.

This checks traceability, not semantic fidelity or successful execution of tests.
Run without --allow-pending for the final documentation gate.
"""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rust-source", type=Path,
                        default=Path("/extra/scylladb/gocql-rust-reference-f8b0ee9"))
    parser.add_argument("--allow-pending", action="store_true")
    args = parser.parse_args()
    directory = Path(__file__).resolve().parent
    catalog = json.loads((directory / "rust-source-catalog.json").read_text())
    revision = catalog["revision"]
    errors, pending = [], []
    actual_revision = subprocess.check_output(
        ["git", "-C", str(args.rust_source), "rev-parse", "HEAD"], text=True).strip()
    if actual_revision != revision:
        errors.append(f"Rust checkout revision {actual_revision} differs from {revision}")
    actual_files = {
        str(path.relative_to(args.rust_source))
        for root in catalog["roots"]
        for path in (args.rust_source / root).rglob("*.rs")
    }
    declared_files = {entry["path"] for entry in catalog["modules"]}
    for path in sorted(actual_files - declared_files):
        errors.append(f"Uncatalogued Rust module: {path}")
    for path in sorted(declared_files - actual_files):
        errors.append(f"Missing Rust module: {path}")
    if len(declared_files) != len(catalog["modules"]):
        errors.append("Duplicate Rust module entries")

    sections = sorted({s for entry in catalog["modules"] for s in entry["sections"]})
    link_pattern = re.compile(
        r"https://github\.com/scylladb/scylla-rust-driver/blob/"
        r"([^/]+)/([^\s)#]+)(?:#L(\d+)(?:-L(\d+))?)?")
    links_checked = 0
    reports_present = 0
    reviewer_ids = set()
    for section in sections:
        preceding_after = None
        spec = directory / f"{section}.md"
        if not spec.exists():
            pending.append(f"Missing specification: {section}")
        else:
            text = spec.read_text()
            links = list(link_pattern.finditer(text))
            if not links:
                errors.append(f"{section}: no pinned Rust implementation links")
            for match in links:
                pin, path, start, end = match.groups()
                if pin != revision:
                    errors.append(f"{section}: source link not pinned to revision: {match[0]}")
                target = args.rust_source / path
                if not target.is_file():
                    errors.append(f"{section}: missing link target {path}")
                    continue
                count = len(target.read_text().splitlines())
                if start and not (1 <= int(start) <= int(end or start) <= count):
                    errors.append(f"{section}: invalid line anchor {match[0]}")
                links_checked += 1
            for title in ("Summary", "Invariants", "Test Places"):
                if not re.search(rf"^## {title}\s*$", text, re.M | re.I):
                    errors.append(f"{section}: missing {title} heading")
            if not re.search(r"^## .*language[- ](?:agnostic|independent)", text, re.M | re.I):
                errors.append(f"{section}: missing language-independent behavior section")
            if not re.search(r"^## .*Rust", text, re.M | re.I):
                errors.append(f"{section}: missing separate Rust evidence section")
        for round_number in range(1, 11):
            report = directory / "reviews" / f"{section}-round-{round_number:02d}.md"
            if not report.exists():
                pending.append(f"Review pending: {section} round {round_number:02d}")
                continue
            reports_present += 1
            content = report.read_text()
            if revision not in content:
                errors.append(f"{report.name}: missing source revision")
            hashes = re.findall(r"\b[0-9a-f]{64}\b", content)
            if len(hashes) < 2:
                errors.append(f"{report.name}: missing before/after SHA-256 evidence")
            else:
                if preceding_after is not None and hashes[0] != preceding_after:
                    errors.append(f"{report.name}: before hash does not match prior review")
                preceding_after = hashes[1]
                if round_number == 10 and spec.exists():
                    actual_hash = hashlib.sha256(spec.read_bytes()).hexdigest()
                    if actual_hash != hashes[1]:
                        errors.append(f"{report.name}: final spec differs from reviewed content")
            identity = re.search(r"/root/[a-z0-9_]+", content)
            if identity is None:
                errors.append(f"{report.name}: missing reviewer agent identity")
            elif identity[0] in reviewer_ids:
                errors.append(f"{report.name}: reviewer was already used in another round")
            else:
                reviewer_ids.add(identity[0])

    inventory = json.loads((directory / "integration-baseline.json").read_text())
    test_count = sum(len(row["tests"]) for row in inventory["files"])
    if test_count != inventory["test_count"]:
        errors.append("Integration inventory count mismatch")
    print(f"Catalog: {len(actual_files)} Rust modules; {len(sections)} sections")
    print(f"Checked: {links_checked} source links; {reports_present}/60 review reports")
    print(f"Baseline: {test_count} integration/CCM tests in {len(inventory['files'])} files")
    for error in errors:
        print(f"ERROR: {error}")
    if pending:
        print(f"Pending documentation artifacts: {len(pending)}")
        if not args.allow_pending:
            for item in pending:
                print(f"PENDING: {item}")
    print("Semantic completeness and runtime verification require separate audits.")
    return 1 if errors or (pending and not args.allow_pending) else 0


if __name__ == "__main__":
    sys.exit(main())
