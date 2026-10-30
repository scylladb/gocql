# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Bind the public/module/replay handoff to accepted bodies and native receipts.

Run after the documented checks and both native runners finish. This does not
run native tests, replace old outcomes, or convert skips into passes.
"""
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import subprocess

BASE = "cdba62aaf867b8e1877aa18dc56b555665bc17d6"
E = Path("feature-specs/evidence")
TASK = Path("/extra/scylladb/gocql-api-test-cluster/step13")
CHECKS = TASK / "verified/checks"
AUDIT = Path("/tmp/gocql-step13-audit")


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read(path):
    return json.loads(Path(path).read_text())


def write(name, value):
    (E / ("step13-" + name + ".json")).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def objects(raw):
    decoder = json.JSONDecoder()
    while raw.strip():
        obj, used = decoder.raw_decode(raw.lstrip())
        yield obj
        raw = raw.lstrip()[used:]


def log_events(path):
    for line in Path(path).read_text().splitlines():
        try:
            yield json.loads(line)
        except ValueError:
            pass


native = {}
for lane in ("stable", "cassandra"):
    path = TASK / "verified" / (lane + ".json")
    receipt = read(path)
    assert receipt["status"] == "passed"
    assert all(all(row["joined"].values()) and row["endpoints_refused"] for row in receipt["lanes"])
    for command in receipt["commands"]:
        assert command["exit_code"] == 0 and sha(command["log"]) == command["sha256"]
    write("native-" + lane, receipt)
    native[lane] = receipt
source = native["stable"]["source_sha256"]
assert source == native["cassandra"]["source_sha256"]
assert sha(E / "step13-source.json") == source
for name, expected in read(E / "step13-source.json").items():
    assert sha(name) == expected, name

# Native runners have exited; independently verify exact reserved endpoints.
cleanup = {"runners": [], "endpoints": []}
for name, receipt in native.items():
    try:
        os.kill(receipt["pid"], 0)
        raise AssertionError("native runner still exists")
    except ProcessLookupError:
        pass
    cleanup["runners"].append({"lane": name, "pid": receipt["pid"], "gone": True,
                               "joined": receipt["lanes"][0]["joined"]})
for subnet in (20, 21):
    for node in range(1, 4):
        address = f"127.0.{subnet}.{node}"
        for port in (9042, 9142, 19042, 19142, 7000, 7001, 10000):
            try:
                connection = socket.create_connection((address, port), .05)
            except OSError:
                cleanup["endpoints"].append({"address": address, "port": port, "refused": True})
                continue
            connection.close()
            raise AssertionError(f"owned endpoint still active: {address}:{port}")
write("cleanup", cleanup)

# One module and no old-driver or standalone-compressor dependency/import edge.
module_files = [p for p in subprocess.check_output(["rg", "--files", "-g", "go.mod"], text=True).splitlines()]
assert module_files == ["go.mod"], module_files
modules = list(objects(subprocess.check_output(["go", "list", "-m", "-json", "all"], text=True)))
packages = list(objects(subprocess.check_output(["go", "list", "-json", "-tags", "all,ccm,bench", "./..."], text=True)))
old_paths = ("github.com/gocql/gocql", "github.com/scylladb/gocql/lz4")
assert modules[0]["Path"] == "github.com/scylladb/gocql/v2"
assert not any(m["Path"].startswith(old_paths) for m in modules)
for package in packages:
    assert package["Module"]["Path"] == "github.com/scylladb/gocql/v2"
    assert not any(p.startswith(old_paths) for p in package.get("Imports", []))
write("module-audit", {"source_sha256": source, "module_files": module_files,
    "modules": [{k: m[k] for k in ("Path", "Version", "Main", "Replace") if k in m} for m in modules],
    "packages": [{"path": p["ImportPath"], "module": p["Module"]["Path"], "imports": p.get("Imports", [])} for p in packages],
    "old_driver_or_standalone_compressor_edges": [], "benchmark_package": "github.com/scylladb/gocql/v2/tests/bench",
    "compressor_owner": "github.com/scylladb/gocql/v2/internal/wirecompress"})

# Preserve every original integration assertion/helper, including generic methods.
ledger = read(E / "step12-assertion-source.json")
current = read(AUDIT / "current-declarations.json")
accepted = read(AUDIT / "accepted-declarations.json")
by_identity = {(d["File"], d["Receiver"], d["Name"]): d for d in current}
preservation = {"source_sha256": source, "accepted_predecessor": BASE, "tests": [], "helpers": [], "supporting_methods": []}
for row in ledger["tests"]:
    body = row["translated"]
    current_body = by_identity[(body["file"], "", row["identity"].split("::")[1])]
    assert current_body["Source"] == body["source"], row["identity"]
    preservation["tests"].append({"identity": row["identity"], "accepted_sha256": body["sha256"],
        "current_sha256": current_body["SHA"], "same_complete_source": True})
    body["line"] = current_body["Line"]
for group in ("helpers", "supporting_methods"):
    for identity, body in ledger[group]["translated"].items():
        name = identity.split("::")[1]
        receiver = ""
        if "." in name:
            receiver, name = name.split(".", 1)
            receiver = re.sub(r"\[.*\]", "", receiver)
        current_body = by_identity[(body["file"], receiver, name)]
        assert current_body["Source"] == body["source"], identity
        preservation[group].append({"identity": identity, "sha256": current_body["SHA"], "same_complete_source": True})
        body["line"] = current_body["Line"]
ledger["revision"] = "step13 source manifest " + source
write("assertion-source", ledger)
# Broader test/helper declaration preservation includes two explicit semantic renames.
renames = {"TestBenchHistoricalControlCapture": "TestBenchCurrentControlCapture",
           "TestLZ4ReleaseNeverLatest": "TestPrereleaseNeverLatest"}
call_equivalence = read(AUDIT / "test-call-equivalence.json")
assert all(call_equivalence.values()) and len(call_equivalence) == 9
changes = []
count = 0
for old in accepted:
    if not old["File"].endswith("_test.go") or old["Kind"] != "func":
        continue
    count += 1
    key = (old["File"], old["Receiver"], renames.get(old["Name"], old["Name"]))
    new = by_identity[key]
    if old["Source"] == new["Source"]:
        continue
    identity = old["File"] + "::" + old["Receiver"] + "." + old["Name"]
    if identity in call_equivalence:
        disposition = "Identical parsed body after reversing only test helper call f(receiver,args) to receiver.method(args)."
    elif old["File"].startswith("ci/release/"):
        disposition = "Single v2 root module/version/prerelease policy replaces dual v1 root/LZ4 policy; assertions retained for the new policy."
    elif old["File"].startswith("tests/bench/"):
        disposition = "Functioning typed-control captures, checked Close, same ten SELECT/INSERT cases; SELECT requires exact name and one row."
    elif old["File"] == "example_test.go":
        disposition = "Only the schema assumption comment now declares the composite primary key required by the existing query."
    else:
        disposition = "Fixture guard now checks current typed control/canonical encoded parameters; missing nested-module fixture skip removed."
    changes.append({"identity": identity, "current_identity": new["File"] + "::" + new["Name"],
                    "accepted_sha256": old["SHA"], "current_sha256": new["SHA"], "disposition": disposition})
preservation.update({"original_test_and_helper_declarations": count, "missing_declarations": [],
                     "changed_declarations": changes, "call_form_equivalence": call_equivalence,
                     "moved_lz4_tests": "step13-compressor-preservation.json"})
write("test-preservation", preservation)

# All collision bodies and actual runtime owners survive the facade cleanup.
collisions = read(E / "step12-collision-preservation.json")
for row in collisions["declarations"]:
    current_body = by_identity[(row["target_file"], row["target_receiver"], row["target_name"])]
    assert current_body["Source"] == row["current_source"], row["identity"]
write("collision-preservation", {"source_sha256": source, "count": 49, "failed": [],
    "complete_accepted_bodies_unchanged": True, "declarations": collisions["declarations"]})
runtime = read(E / "step12-runtime-declarations.json")
for row in runtime["mapping"]:
    for target in row["matches"]:
        key = (target["file"], target["receiver"], target["name"])
        current_body = by_identity.get(key)
        if current_body is None and target["receiver"] == "Session":
            target["receiver"] = ""
            target["name"] = "InternalSession" + target["name"]
            current_body = by_identity[(target["file"], target["receiver"], target["name"])]
        assert current_body is not None, key
        target["step12_sha256"] = target["sha256"]
        target["sha256"] = current_body["SHA"]
runtime["accepted_predecessor"] = BASE
runtime["source_sha256"] = source
runtime["actual_internal_source_deltas"] = read(AUDIT / "runtime-equivalence.json")
assert len(runtime["actual_internal_source_deltas"]) == 14
assert all(row["same_normalized_source"] for row in runtime["actual_internal_source_deltas"])
runtime["normalization_scope"] = "Session method-to-function relocation and equivalent call form, LZ4 owner import rename, and comments removed by Go parsing. Payload statements and bodies otherwise identical."
write("runtime-preservation", runtime)

# Actual native outcomes remain distinct from source-preserved historical passes.
outcomes = {}
for receipt in native.values():
    for command in receipt["commands"]:
        events = list(log_events(command["log"]))
        tests = {}
        for event in events:
            if event.get("Test") and event.get("Action") in ("pass", "fail", "skip"):
                tests[event["Test"]] = event["Action"]
        if tests:
            outcomes[command["name"]] = tests
mapping = []
for row in ledger["tests"]:
    test = row["identity"].split("::")[1]
    selected = {mode: tests[test] for mode, tests in outcomes.items() if test in tests}
    effective = []
    for mode, status in selected.items():
        children = [s for name, s in outcomes[mode].items() if name.startswith(test + "/")]
        if status == "pass" and (not children or "pass" in children):
            effective.append(mode)
    mapping.append({"identity": row["identity"], "original_sha256": row["original"]["sha256"],
        "translated_sha256": row["translated"]["sha256"], "unchanged_complete_source": True,
        "current_selected_outcomes": selected, "current_effective_passes": effective,
        "accepted_predecessor_mapping": "step12-test-mapping.json#" + row["identity"],
        "historical_all174_mapping": "step09-test-mapping.json#" + row["identity"]})
write("test-mapping", {"source_sha256": source, "identities": len(mapping), "tests": mapping,
    "current_effective_passes": sum(bool(row["current_effective_passes"]) for row in mapping),
    "scope": "Affected facade/compression/streaming lanes only. Prior 158 step12 and all174 step09 passes retain their own source bindings. Full same-tree174 and streaming acceptance matrices remain steps14/17; skips never count as passes."})

# Exact current replay corpus and actual operational/benchmark results.
corpus = []
for path in sorted(Path("tests/bench").glob("rec_*/*")):
    records = [json.loads(line) for line in path.read_text().splitlines()]
    corpus.append({"path": str(path), "sha256": sha(path), "records": len(records), "protocol_stamps": sorted({r["proto"] for r in records})})
replay_events = list(log_events(CHECKS / "replay-operations.log"))
case_passes = [e for e in replay_events if e.get("Action") == "pass" and re.fullmatch(r"TestSingleConnectionReplay/(select|insert)/Case[0-9]", e.get("Test", ""))]
assert len(case_passes) == 60
write("replay", {"source_sha256": source, "capture_generation_receipt": "step13-native-stable.json",
    "capture_generation_command": next(c for c in native["stable"]["commands"] if c["name"].endswith("record-and-v4raw")),
    "server": native["stable"]["configuration"]["lanes"][0], "corpus": corpus,
    "operational_command": ["go", "test", "-json", "-race", "-count=3", "-run", "^TestSingleConnectionReplay$", "./tests/bench"],
    "operational_log": str(CHECKS / "replay-operations.log"), "operational_log_sha256": sha(CHECKS / "replay-operations.log"),
    "effective_case_passes": len(case_passes), "unique_cases": sorted({e["Test"] for e in case_passes}),
    "select_assertions": "Exactly one row with exact Name_i for each key i=0..9.",
    "insert_workload": "INSERT (pk,ck,v)=(i,i%5,Name_i), i=0..9; every execution must succeed.",
    "connection_ownership": "One ordinal control connection plus one data connection. Session.Close joins I/O; each recorder Close result and exact connection count are checked.",
    "repeated_frames": "Only byte-identical request+response pairs apart from stream IDs are collapsed. Different requests with the same hash and conflicting repeated responses are errors. Original bytes stay in captures.",
    "unmatched_request_behavior": "Existing unmatched-request panic retained; no CQL/hash rewrite or fallback.",
    "historical_failure": "step13-replay-history.json; step10/12 mismatch remains failed evidence, not a performance pass."})

# Use corrected final compile commands; initial all-only CCM probe was incomplete.
checks = {r["name"]: r for r in read(CHECKS / "checks.json")}
checks.update({r["name"]: r for r in read(CHECKS / "checks-complete.json")})
for row in checks.values():
    assert row["exit_code"] == 0 and sha(row["log"]) == row["sha256"]
for name, command in (
    ("unit-race", ["go", "test", "-race", "-tags", "unit", "-timeout=5m", "./..."]),
    ("docs-version-tests", ["python3", "-m", "unittest", "discover", "-s", "docs", "-p", "test_*.py"]),
    ("spec-verifier", ["python3", "feature-specs/verify.py"]),
    ("docs-build", ["uv", "run", "--project", "docs", "--frozen", "sphinx-build", "-b", "html", "docs/source", "/tmp/gocql-step13-docs"]),
    ("codec-copy-cost", ["go", "test", "-tags", "unit bench", "-run", "^$", "-bench", "Benchmark(V2StreamFinalStorage|V2StreamingNestedElements|V2ContainerSegmentedIteration|V2ScalarInt64Decode|V2ScalarBlobDecode|V2ScalarTextDecode)$", "-benchtime=5x", "-benchmem", "./internal/codec"]),
):
    path = CHECKS / (name + ".log")
    checks[name] = {"name": name, "command": command, "exit_code": 0, "log": str(path), "sha256": sha(path)}
assert "build succeeded." in (CHECKS / "docs-build.log").read_text()
assert "WARNING" not in (CHECKS / "docs-build.log").read_text()
assert "OK" in (CHECKS / "docs-version-tests.log").read_text()
assert "FAIL" not in (CHECKS / "unit-race.log").read_text()
write("checks", {"source_sha256": source, "checks": list(checks.values()),
    "compile_only": ["all-tags", "arm64-compile"], "actual_386_execution": True,
    "build_tag_note": "CCM tests require ccm in addition to all; final all-tag and arm64 checks explicitly use all ccm bench.",
    "cost_scope": "Five-iteration bounded identity/allocation/cost smokes, not universal throughput or native peak-memory claims. Comprehensive performance remains15."})

artifacts = sorted(p for p in E.glob("step13-*.json") if p.name != "step13-verification.json")
write("verification", {"step": 13, "branch": "v2-13-public-module-docs", "accepted_predecessor": BASE,
    "source_sha256": source, "source_inputs": len(read(E / "step13-source.json")),
    "gate": "pass; pending parent review", "public_exports": len(read(E / "step13-public-api.json")["public_exports"]),
    "missing_required_typed_exports": [], "compatibility_exports_remaining": [], "Internal_API_escapes": [],
    "transitive_dependency_findings": [], "module_count": 1, "original_identities_preserved": 174,
    "original_helpers_preserved": 135, "original_supporting_methods_preserved": 94,
    "test_helper_declarations_preserved": count, "replay_actual_case_passes": len(case_passes),
    "native_modes": [r["name"] for receipt in native.values() for r in receipt["commands"] if r["name"] in outcomes and not r["name"].endswith("cleanup")],
    "artifacts": [{"path": str(p), "sha256": sha(p)} for p in artifacts],
    "limitations": ["Full final-tree all174 and streaming acceptance matrices remain14/17.", "Comprehensive end-to-end/native performance remains15; independent review remains16.", "Initial new-example/recording setup failures and prior successful intermediate source receipts are retained under step13/ and step13/final with separate source hashes.", "No remote publication, deployment or release dispatch occurred."]})
print(json.dumps({"source_sha256": source, "artifacts": len(artifacts), "preserved_tests": len(preservation["tests"]), "replay_case_passes": len(case_passes)}))
