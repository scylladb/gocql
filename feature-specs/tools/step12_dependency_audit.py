# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.

"""Audit actual production imports, including prohibited transitive owners.

Run from the module root; the JSON report goes to stdout and violations exit 1.
The graph includes optional integration/debug production files, but excludes tests.
"""
import collections
import json
import subprocess
import sys

MODULE = "github.com/scylladb/gocql/v2"
COMMAND = ["go", "list", "-json", "-tags", "integration,gocql_debug", "./..."]
raw = subprocess.check_output(COMMAND, text=True)
decoder = json.JSONDecoder()
packages = []
while raw.strip():
    package, length = decoder.raw_decode(raw.lstrip())
    packages.append(package)
    raw = raw.lstrip()[length:]
graph = {p["ImportPath"]: p.get("Imports", []) for p in packages}
# Each set names implementation owners which cannot occur anywhere below owner.
# Checking paths, rather than only immediate edges, catches full-settings aliases.
prohibited = {
    "connectivity": {"settings", "routing", "metadata", "cluster", "execution", "pool", "transport"},
    "transport": {"settings", "routing", "metadata", "cluster", "execution", "pool"},
    "pool": {"settings", "routing", "metadata", "cluster", "execution"},
    "protocol": {"settings", "routing", "metadata", "cluster", "execution", "pool", "transport"},
}
findings = []
reachability = {}
for owner, denied in prohibited.items():
    start = MODULE + "/internal/" + owner
    paths = {start: [start]}
    queue = collections.deque([start])
    while queue:
        current = queue.popleft()
        for dependency in graph.get(current, []):
            if dependency in paths or not (dependency == MODULE or dependency.startswith(MODULE + "/")):
                continue
            paths[dependency] = paths[current] + [dependency]
            queue.append(dependency)
    reachability[owner] = sorted(set(paths) - {start})
    for dependency in sorted({MODULE} | {MODULE + "/internal/" + name for name in denied}):
        if dependency in paths:
            findings.append({"owner": owner, "prohibited_owner": dependency, "path": paths[dependency]})
report = {
    "command": COMMAND,
    "rule": "No prohibited runtime owner may be reachable by any production import path, including settings indirection.",
    "prohibited": {k: sorted(v | {"public root"}) for k, v in prohibited.items()},
    "findings": findings,
    "reachability": reachability,
    "graph": [{"package": p["ImportPath"], "imports": p.get("Imports", []), "sources": p.get("GoFiles", [])} for p in packages],
    "root_responsibilities": "aliases, typed thin function facades, consistency parsing and private-core connection I/O facade",
}
json.dump(report, sys.stdout, indent=2)
print()
sys.exit(bool(findings))
