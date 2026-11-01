# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Lossless line-bound requirement inventory; absence of proof stays open.

Each paragraph, list item, table row and code declaration/field is retained.
The exact document bytes and line ranges are independently verifiable. Historical
Rust behavior and staging narratives are distinguished from Go obligations, but
remain in the inventory so a disposition cannot silently erase a named feature.
No keyword match or passing broad suite changes an item's proof status.
"""
import collections,hashlib,json,re
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
OUT=Path('/extra/scylladb/gocql-api-test-cluster/step17/release')
E=ROOT/'feature-specs/evidence'
DOCS=['plan.md','feature-specs/gocql-v2-api.md','feature-specs/gocql-v2-implementation.md','feature-specs/gocql-v2-streaming-receive.md']+['feature-specs/'+n+'.md' for n in ['routing','connections','metadata-control-events','execution','serialization-protocol','configuration-security-observability']]
sha=lambda data:hashlib.sha256(data).hexdigest()

def extract(name):
    raw=(ROOT/name).read_bytes();lines=raw.decode().splitlines(keepends=True)
    sections=[];items=[];pending=[];start=0;fence=None;code_owner=None
    def emit(kind):
        nonlocal pending
        if not pending:return
        body=''.join(pending)
        items.append(dict(id=name+':'+str(start),document=name,start_line=start,end_line=start+len(pending)-1,section=list(sections),kind=kind,code_owner=code_owner if kind=='code' else None,text=body,text_sha256=sha(body.encode()),status='open',proof=[]))
        pending=[]
    for number,line in enumerate(lines,1):
        stripped=line.strip()
        heading=re.match(r'^(#{1,6}) (.*)',line)
        if heading and fence is None:
            emit('paragraph');depth=len(heading[1]);sections[:]=sections[:depth-1];sections.append(heading[2]);continue
        if stripped.startswith('```'):
            emit('paragraph' if fence is None else 'code')
            fence=None if fence else stripped[3:];code_owner=None;continue
        if not stripped:
            emit('code' if fence else 'paragraph');continue
        if fence is not None:
            # Keep multiline declarations intact; retain field lines as named
            # leaves so every Config/Options/limit field remains auditable.
            emit('code')
            declared=re.match(r'(?:type|var|const)\s+(\w+)',stripped)
            if declared:code_owner=declared[1]
            elif stripped.startswith('func '):code_owner=None
            start=number;pending=[line];emit('code');continue
        kind='table' if stripped.startswith('|') else 'list' if re.match(r'^(?:[-*]|\d+[.)]) ',stripped) else 'paragraph'
        if kind=='table' and re.fullmatch(r'[| :\-]+',stripped):continue
        if kind in ('table','list'):
            emit('paragraph');start=number;pending=[line]
            if kind=='table':emit(kind)
        else:
            if not pending:start=number
            pending.append(line)
    emit('paragraph')
    for item in items:
        heading=' / '.join(item['section']);reference=name not in DOCS[:4]
        if reference:
            item['scope']='reference-source' if any(x in heading for x in ['Rust implementation','Rust-specific','Rust unit','Rust integration']) else 'reference-feature-disposition'
        elif name=='plan.md' and any(x in heading for x in ['Checkpoint: built versus verified','Design reconciliation results','Current gaps to address first']):item['scope']='historical-checkpoint-with-current-obligation'
        else:item['scope']='go-requirement'
        if item['kind']=='code' and item['text'].strip() in ('{','}',')','};'):
            item['scope']='structural-delimiter'
    return dict(document=name,sha256=sha(raw),lines=len(lines),items=items)

def main():
    documents=[extract(p) for p in DOCS]
    disposition=OUT/'inventory-dispositions.json'
    decisions=json.loads(disposition.read_text()) if disposition.exists() else {}
    for doc in documents:
        for item in doc['items']:
            decision=decisions.get(item['id'])
            if decision:
                assert decision['text_sha256']==item['text_sha256'],'changed requirement '+item['id']
                item.update({k:v for k,v in decision.items() if k!='text_sha256'})
    counts=collections.Counter(i['status'] for d in documents for i in d['items'])
    result=dict(method='Complete paragraph/list/table/code-field inventory; exact text/line/document hashes. No inherited pass or keyword matching establishes achievement.',documents=documents,counts=dict(counts),gate='open' if counts['open'] else 'pending-final-audit')
    (E/'step17-requirement-inventory.json').write_text(json.dumps(result,sort_keys=True,indent=2)+'\n')
    print(json.dumps(dict(documents=len(documents),items=sum(len(d['items']) for d in documents),counts=dict(counts))))
if __name__=='__main__':main()
