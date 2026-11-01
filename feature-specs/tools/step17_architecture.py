# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Audit final normal/all-tag production closure and public/runtime boundaries."""
import hashlib,json,re,subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
OUT=Path('/extra/scylladb/gocql-api-test-cluster/step17/release');MODULE='github.com/scylladb/gocql/v2'
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
def documents(path):
    raw=Path(path).read_text();decoder=json.JSONDecoder();out=[]
    while raw.strip():
        value,end=decoder.raw_decode(raw.lstrip());out.append(value);raw=raw.lstrip()[end:]
    return out
denied={
 'wire':set(), 'crc':set(), 'murmur':set(),
 'cqltype':{'settings','routing','metadata','cluster','execution','protocol','transport','pool'},
 'codec':{'settings','routing','metadata','cluster','execution','protocol','transport','pool'},
 'connectivity':{'settings','routing','metadata','cluster','execution','transport','pool'},
 'model':{'settings','cluster','execution','transport','pool'},
 'protocol':{'settings','routing','metadata','cluster','execution','transport','pool'},
 'transport':{'settings','routing','metadata','cluster','execution','pool'},
 'pool':{'settings','routing','metadata','cluster','execution'},
 'metadata':{'cluster','execution','transport','pool'},
 'routing':{'cluster','execution','transport','pool'},
}
def build():
    findings=[];closures={}
    graphs=[('normal',OUT/'checks/dependency-catalog.log'),('all-tags',OUT/'checks/api-audit/packages-all.json')]
    if (OUT/'complete-tag-production-graph.log').exists():graphs.append(('complete-tag-union',OUT/'complete-tag-production-graph.log'))
    for mode,path in graphs:
        packages=documents(path);graph={p['ImportPath']:p.get('Imports',[]) for p in packages};closures[mode]={}
        for owner,prohibited in denied.items():
            start=MODULE+'/internal/'+owner
            if start not in graph:
                # crc is intentionally a root-independent primitive package.
                start=MODULE+'/'+owner
            todo=[start];seen=set()
            while todo:
                name=todo.pop()
                if name in seen:continue
                seen.add(name);todo.extend(p for p in graph.get(name,[]) if p==MODULE or p.startswith(MODULE+'/'))
            if MODULE in seen:findings.append((mode,owner,'public root reachable'))
            illegal=seen.intersection(MODULE+'/internal/'+x for x in prohibited)
            if illegal:findings.append((mode,owner,sorted(illegal)))
            closures[mode][owner]=sorted(seen-{start})
        if MODULE+'/scyllacloud' in graph.get(MODULE,[]):findings.append((mode,'root imports cloud'))
    api=json.loads((OUT/'checks/api-audit/public-audit.json').read_text())
    assert not api['issues'] and not api['unnameable_required_interface_types']
    assert len(api['public_exports'])==507 and len(api['thin_facade_functions'])==130 and len(api['canonical_session_methods'])==9
    assert not api['remaining_compatibility_exports'] and not api['missing_required_exports']
    assert api['external_implementation_consumer']['exit_code']==0
    modules=subprocess.check_output(['rg','--files','--hidden','-g','go.mod','-g','!.git','-g','!docs/.venv'],cwd=ROOT,text=True).splitlines()
    assert modules==['go.mod'],modules
    assert re.search(r'(?m)^module '+re.escape(MODULE)+r'$',(ROOT/'go.mod').read_text())
    assert not re.search(r'(?m)^\s*github.com/scylladb/gocql/lz4\s',(ROOT/'go.mod').read_text())
    # Every production runtime/codec file is independently bound here. Allowed
    # cold metadata report/recreation representations are not execution values.
    sources={};forbidden=[]
    for owner in ['execution','codec','transport','pool','routing']:
        for path in sorted((ROOT/'internal'/owner).glob('*.go')):
            if path.name.endswith('_test.go'):continue
            name=str(path.relative_to(ROOT));raw=path.read_text();sources[name]=sha(path)
            if re.search(r'(?m)^\s*"reflect"',raw):forbidden.append(name+': reflection import')
            if re.search(r'(?m)^\s*"unsafe"',raw):forbidden.append(name+': unsafe import')
            if re.search(r'\b(?:Marshal|Unmarshal|MapScan|SliceMap)\s*\(',raw):forbidden.append(name+': legacy serializer call')
    findings+=forbidden
    report=dict(runtime_source_sha256=sha(OUT/'checks/source.json'),normal_and_all_tag_closures=closures,prohibited={k:sorted(v)for k,v in denied.items()},findings=findings,one_module=modules,public_exports=507,thin_forwarders=130,session_methods=9,normal_external_consumer=api['external_implementation_consumer'],runtime_sources=sources,allowed_nonexecution_boundaries='Cold metadata options/recreation and effective configuration reporting retain value-only maps/reflection/JSON. wire.BorrowString is the sole read-only unsafe string view; murmur primitive compatibility and opt-in counters are separately bounded. No reflective or unsafe output decode path is allowed.',gate='open' if findings else 'passed')
    (ROOT/'feature-specs/evidence/step17-architecture.json').write_text(json.dumps(report,sort_keys=True,indent=2)+'\n')
    print(json.dumps(dict(gate=report['gate'],findings=findings,closures=len(closures),owners=len(denied))))
    assert not findings
    return report
if __name__=='__main__':build()
