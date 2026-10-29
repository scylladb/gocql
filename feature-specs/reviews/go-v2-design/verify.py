#!/usr/bin/env python3
"""Verify the ten Go design review reports; this does not verify driver behavior."""
from pathlib import Path
import hashlib
import json
import re


def main():
    directory = Path(__file__).resolve().parent
    root = directory.parents[2]
    docs = ["plan.md", "feature-specs/gocql-v2-api.md",
            "feature-specs/gocql-v2-implementation.md"]
    previous = None
    identities = []
    total = 0
    for number in range(1, 11):
        report = (directory / f"round-{number:02d}.md").read_text()
        identities.append(re.search(r"Reviewer: `([^`]+)`", report).group(1))
        total += int(re.search(r"(\d+) finding", report).group(1))
        assert "0 unresolved" in report
        rows = re.findall(
            r"^\| `([^`]+)` \| `([0-9a-f]{64})` \| `([0-9a-f]{64})` \|$",
            report, re.M)
        assert len(rows) == 3, number
        before = {path: digest for path, digest, _ in rows}
        after = {path: digest for path, _, digest in rows}
        if previous is not None:
            assert before == previous, number
        previous = after
    assert len(set(identities)) == 10
    assert total == 33
    assert previous == {path: hashlib.sha256((root / path).read_bytes()).hexdigest()
                        for path in docs}
    plan = (root / "plan.md").read_text()
    tasks = re.findall(r"^\| (\d+) \| `v2-(\d+)-([^`]+)`", plan, re.M)
    assert len(tasks) == 17
    assert [int(task[0]) for task in tasks] == list(range(1, 18))
    assert all(int(order) == int(branch) for order, branch, _ in tasks)
    assert "v2-01-design-contract" not in plan
    inventory = json.loads((root / "feature-specs/integration-baseline.json").read_text())
    assert inventory["test_count"] == sum(len(row["tests"]) for row in inventory["files"]) == 174
    for path in [root / name for name in docs] + list(directory.glob("*.md")):
        content = path.read_text()
        assert sum(line.startswith("```") for line in content.splitlines()) % 2 == 0, path
        prose = re.sub(r"^```[^\n]*\n.*?^```[^\n]*(?:\n|$)", "", content,
                       flags=re.M | re.S)
        for raw in re.findall(r"\]\(([^)]+)\)", prose):
            if "://" in raw or raw.startswith("#"):
                continue
            target = raw.split("#")[0]
            if target:
                assert (path.parent / target).exists(), (path, target)
    print("PASS: 10 unique reviewers; 33 findings/fixes; 0 unresolved; "
          "SHA-256 chain/final hashes; 17 contiguous branches; "
          "174 baseline identities; local links/code fences.")


if __name__ == "__main__":
    main()
