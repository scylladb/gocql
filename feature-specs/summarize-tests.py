#!/usr/bin/env python3
"""Summarize complete go test -json runs without treating skips as passes.

This is an evidence checker, not proof that a test exercises the v2 API. The
migration manifest and source review establish that separate requirement.
"""

import argparse
from collections import Counter
import json
from pathlib import Path
import sys


def read_run(path):
    results = {}
    packages = {}
    started = set()
    race = False
    failures = []
    for number, line in enumerate(path.read_text().splitlines(), 1):
        try:
            event = json.loads(line)
        except ValueError:
            # Compiler and go-command diagnostics are sometimes plain text.
            failures.append(f"line {number}: non-JSON diagnostic: {line[:240]}")
            continue
        action, package, test = event.get("Action"), event.get("Package"), event.get("Test")
        if action == "start":
            started.add(package)
        if "WARNING: DATA RACE" in event.get("Output", ""):
            race = True
        if action in ("pass", "fail", "skip"):
            if test:
                results[(package, test)] = action
            else:
                packages[package] = action
    for package in started - packages.keys():
        failures.append(f"package has no terminal result: {package}")
    if not started:
        failures.append("no package start events")
    for package, result in packages.items():
        if result != "pass":
            failures.append(f"package {package}: {result}")
    for (package, test), result in results.items():
        if result == "fail":
            failures.append(f"{package}:{test}: fail")
    if race:
        failures.append("race detector reported a data race")
    return {
        "path": str(path),
        "successful": not failures,
        "counts_including_subtests": dict(Counter(results.values())),
        "package_results": packages,
        "diagnostics": failures,
        "tests": [{"package": package, "test": test, "result": result}
                  for (package, test), result in sorted(results.items())],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runs", nargs="+", type=Path)
    parser.add_argument("--inventory", type=Path,
                        default=Path(__file__).with_name("integration-baseline.json"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--require-all", action="store_true",
                        help="fail unless every inventoried top-level test passed in a successful run")
    args = parser.parse_args()
    inventory = json.loads(args.inventory.read_text())
    evidence = {name: [] for file in inventory["files"] for name in file["tests"]}
    runs = [read_run(path) for path in args.runs]
    for run in runs:
        if not run["successful"]:
            continue
        for test in run["tests"]:
            if test["result"] == "pass" and test["test"] in evidence:
                evidence[test["test"]].append(run["path"])
    missing = [name for name, paths in evidence.items() if not paths]
    report = {
        "scope": "Execution outcomes only; v2 migration and assertion fidelity need source audits.",
        "inventory_revision": inventory["revision"],
        "runs": runs,
        "test_count": len(evidence),
        "tests_with_pass": len(evidence) - len(missing),
        "missing_pass": missing,
        "passing_evidence": evidence,
    }
    if args.output:
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    for run in runs:
        print(f"{run['path']}: {'PASS' if run['successful'] else 'INCOMPLETE/FAIL'} "
              f"{run['counts_including_subtests']}")
        for diagnostic in run["diagnostics"]:
            print(f"  {diagnostic}")
    print(f"Original top-level tests with pass evidence: {report['tests_with_pass']}/{len(evidence)}")
    if args.require_all and missing:
        print("Missing: " + ", ".join(missing))
    return int(any(not run["successful"] for run in runs) or (args.require_all and bool(missing)))


if __name__ == "__main__":
    sys.exit(main())
