# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Reject falsified evidence in memory; never modify source, receipts or logs."""
import copy,hashlib,importlib.util,json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];TASK=Path('/extra/scylladb/gocql-api-test-cluster/step15')
spec=importlib.util.spec_from_file_location('evidence',ROOT/'feature-specs/tools/step15_evidence.py');e=importlib.util.module_from_spec(spec);spec.loader.exec_module(e)
real_read=e.read;real_sha=e.sha;results=[]
def probe(name,mutate_read=None,mutate_sha=None):
 writes=[]
 def read(path):
  value=real_read(path)
  return mutate_read(str(path),copy.deepcopy(value))if mutate_read else value
 def sha(path):return mutate_sha(str(path),real_sha(path))if mutate_sha else real_sha(path)
 e.read=read;e.sha=sha;e.write=lambda name,value:writes.append(name)
 try:e.main()
 except (AssertionError,KeyError)as error:
  assert not writes,(name,'wrote passing evidence before rejection',writes)
  results.append(dict(name=name,rejected=True,error=str(error),writes=[]))
 else:raise AssertionError((name,'false pass'))
 finally:e.read=real_read;e.sha=real_sha
probe('source-drift',mutate_sha=lambda p,v:'0'*64 if p==str(ROOT/'internal/codec/dynamic.go')else v)
probe('missing-required-check',mutate_read=lambda p,v:v[1:]if p.endswith('/final-checks/checks.json')else v)
probe('matched-original-adapter-hash-drift',mutate_sha=lambda p,v:'0'*64 if p==str(TASK/'baseline-matched/step15_performance_integration_test.go')else v)
probe('missing-result-shape',mutate_read=lambda p,v:v[1:]if p.endswith('/final-policy/v4-no-compression/results.json')else v)
def negotiation(path,value):
 if path.endswith('/final-policy/v4-no-compression/results.json'):value[0]['Negotiated'][0]['host_connections']+=1
 return value
probe('actual-negotiated-capacity-mismatch',mutate_read=negotiation)
p=ROOT/'feature-specs/evidence/step15-negative-controls.json';p.write_text(json.dumps(results,sort_keys=True,indent=2)+'\n');print(json.dumps(dict(probes=len(results),all_rejected=True,source_and_logs_mutated=False)))
