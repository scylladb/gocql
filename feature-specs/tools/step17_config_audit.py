# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Enumerate every Config field's implementation and current behavioral witnesses.

Direct field references select source witnesses, not automatic achievement.
The contract inventory supplies scope decisions; the timestamp/socket additive
oracles close the two directly observed test gaps without changing native input.
"""
import hashlib,json,re
from pathlib import Path
import step17_matrix as matrix
ROOT=matrix.ROOT;OUT=matrix.TASK;E=matrix.E
def build():
    declarations=matrix.read(OUT/'current-declarations.json')
    c=next(d for d in declarations if d['File']=='internal/settings/v2_config.go' and d['Name']=='Config')
    owners=['Config','PoolOptions','MetadataOptions','ProtocolLimits','IdentityOptions','ClientRoutesOptions','Profile','Options','PrepareOptions','StreamOptions','StreamLimits','RoutingOptions','LatencyOptions']
    fields=[];owner_declarations={}
    for owner in owners:
        candidates=[d for d in declarations if d['Kind']=='type'and d['Name']==owner and re.search(r'type '+owner+r' struct\s*\{',d['Source'])]
        if owner=='StreamLimits':candidates=[d for d in candidates if d['File']=='internal/transport/v2_stream_transport.go']
        declaration=candidates[0];owner_declarations[owner]=dict(file=declaration['File'],line=declaration['Line'],sha256=declaration['SHA'])
        body=re.search(r'type '+owner+r' struct\s*\{(.*?)\}',declaration['Source'],re.S)[1]
        for line in re.split(r'[\n;]',body):
            match=re.match(r'\s*((?:[A-Z][A-Za-z0-9_]*\s*,\s*)*[A-Z][A-Za-z0-9_]*)\s+',line)
            if match:fields.extend((owner,field)for field in re.split(r'\s*,\s*',match[1]))
    commands={}
    for command in matrix.read(OUT/'checks/checks.json'):commands['checks/'+command['name']]=command
    for path in OUT.glob('*.json'):
        receipt=matrix.read(path)
        if isinstance(receipt,dict) and receipt.get('status')=='passed':
            for command in receipt['commands']:commands[path.stem+'/'+command['name']]=command
    outcomes={key:matrix.package_outcomes(command)for key,command in commands.items()}
    witnesses={};rows=[]
    for owner,field in fields:
        implementations=[];tests=[]
        for d in declarations:
            if not re.search(r'\b'+field+r'\b',d['Source']):continue
            identity=d['File']+'::'+(d['Receiver']+'.' if d['Receiver'] else '')+d['Name']
            if d['Kind']=='func' and d['File'].endswith('_test.go') and d['Name'].startswith('Test'):
                package='github.com/scylladb/gocql/v2'+('/'+str(Path(d['File']).parent) if Path(d['File']).parent!=Path('.') else '')
                passes=[key for key,values in outcomes.items() if matrix.effective({name:action for (pkg,name),action in values.items() if pkg==package},d['Name'])]
                if not passes:continue
                tests.append(dict(identity=identity,current_effective_passes=passes))
                witnesses[identity]=dict(file=d['File'],line=d['Line'],sha256=d['SHA'],complete_source=d['Source'])
            elif d['Kind']=='func' and not d['File'].endswith('_test.go') and re.search(r'\.'+field+r'\b',d['Source']):
                implementations.append(identity);witnesses[identity]=dict(file=d['File'],line=d['Line'],sha256=d['SHA'],complete_source=d['Source'])
        rows.append(dict(owner=owner,field=field,implementation_symbols=implementations,current_behavioral_test_witnesses=tests,status='evidence-attached' if implementations and tests else 'open'))
    result=dict(runtime_source_sha256=matrix.sha(OUT/'checks/source.json'),config_declaration=dict(file=c['File'],line=c['Line'],sha256=c['SHA']),owner_declarations=owner_declarations,fields=rows,witnesses=witnesses,additional_current_tests=['step17_config_behavior_test.go','step17_tablet_oracle_test.go'],normal_api_shape_receipt=str(OUT/'checks/api-audit/public-audit.json'),gate='open' if any(r['status']=='open'for r in rows)else 'current-source-witnesses-complete',caution='Symbol occurrence is not achievement. Every field remains subject to its line-bound normative contract and the complete native/unit/copy/architecture gates.')
    matrix.write(E/'step17-config-audit.json',result)
    print(json.dumps(dict(fields=len(rows),open=[r['owner']+'.'+r['field']for r in rows if r['status']=='open'])))
    return result
if __name__=='__main__':build()
