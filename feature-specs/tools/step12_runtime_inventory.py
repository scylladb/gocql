# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.

import json,collections,pathlib,os
AUDIT=pathlib.Path(os.environ.get("GOCQL_STEP12_AUDIT_DIR","/tmp/gocql-step12-audit"))
BASE='github.com/scylladb/gocql/v2/'
old=json.load(open((AUDIT/'original-declarations.json')));cur=json.load(open((AUDIT/'current-declarations.json')))
exclude={'v2_foundation_facade.go','v2_protocol_facade.go','v2_framer_facade.go','v2_metadata_cursor_facade.go','compressor.go','frame.go'}
original=[x for x in old if '/' not in x['File'] and not x['File'].endswith('_test.go') and x['File'] not in exclude and x['Name']!='_']
types={x['Name'] for x in original if x['Kind']=='type'};names={x['Name'] for x in original};methodsets=collections.defaultdict(set)
for x in original:
 if x['Receiver']:methodsets[x['Receiver']].add(x['Name'])
def canon(s,values):
 if s in values:return s
 if s.startswith('Internal'):
  suffix=s[8:];low=suffix[:1].lower()+suffix[1:]
  if low in values:return low
  if suffix in values:return suffix
 return s
lift=set('prepareTyped prepareOnConn prepareOnConnExpected prepareHost hostPreparedInfo executePage executeTypedAttempt registerExecution stopExecutions prepareBatchAttempt repairBatch'.split())
free={}
for path in ['private-method-relocations.json','private-legacy-method-relocations.json']:
 for x in json.load(open((AUDIT/path))):
  owner=canon(x['owner'],types);name=canon(x['method'],methodsets[owner]);free[(x['package'].replace(BASE,''),x['function'])]=(owner,name)
index=collections.defaultdict(list)
for x in cur:
 if not x['File'].startswith('internal/') or x['File'].endswith('_test.go') or '/aliases_' in x['File']:continue
 owner=canon(x['Receiver'],types);name=canon(x['Name'],methodsets[owner]|methodsets['Session'] if owner=='sessionRuntime' else methodsets[owner] if owner else names)
 if owner=='sessionRuntime' and name in lift:owner='Session'
 if not owner:
  mapped=free.get((str(pathlib.Path(x['File']).parent),x['Name']))
  if mapped:owner,name=mapped
 if x['Name']=='InternalRuntimeFor' and x['File'].startswith('internal/execution/'):owner,name='Session','typedRuntime'
 index[(x['Kind'],owner,name)].append(x)
explicit={
 ('func','','cloneV2TLSConfig'):('internal/connectivity/dial.go','CloneTLSConfig','The unchanged trust/certificate snapshot implementation belongs below transport; settings and root retain thin delegating facades.'),
 ('func','','unixContact'):('internal/connectivity/dial.go','UnixContact','The unchanged physical Unix endpoint validation consumes only lower settings; higher settings projects its validated descriptors.'),
 ('func','','newSessionCommon'):('internal/cluster/session.go','newSessionCommonWithRuntime','The shared constructor captures typed configuration and attaches execution before publishing workers/pool services; the legacy facade passes unset configuration.'),
 ('func','Session','streamIDGenerator'):('internal/transport/dial.go','newStreamIDs','Captured stream cap is passed to the same limited stream generator during transport opening.'),
 ('func','Conn','awaitSchemaAgreement'):('internal/cluster/v2_metadata.go','InternalSessionAwaitV2SchemaAgreementOn','Agreement is coordinator-owned; the sole direct deadline assertion now invokes this existing operation.'),
 ('func','HostInfo','IsBusy'):('internal/cluster/policies.go','InternalHostBusy','Pool occupancy belongs to the coordinator; preserve the same threshold check.'),
 ('func','Session','MissingConnections'):('internal/testkit/cluster/pool_integration.go','MissingConnections','Move the original configured-pool fault/count hook off public Session.'),
 ('func','Session','CloseAllConnections'):('internal/testkit/cluster/pool_integration.go','CloseAllConnections','Move the original configured-pool fault hook off public Session.'),
 ('type','','ConnPickerIntegration'):(None,None,'Unused test-only interface had no references; remove it while retaining all actual fault assertions/hooks.'),
 ('func','Session','stopPreparedWorkers'):('internal/execution/v2_runtime_attachment.go','StopPrepared','Execution lifecycle attachment owns the same prepare admission close and worker WaitGroup drain.'),
 ('type','','typedResultMetadata'):('internal/model/result_metadata.go','ResultSchemaIdentity','Move unchanged columns/fence/tracker identity to a lower nongeneric response contract.'),
}
prefer={**{name:'internal/connectivity/settings.go' for name in ['PoolOptions','ReconnectPolicy','ProtocolLimits','ClientRoutesOptions','AuthProvider','AuthExchange','ConvictionPolicy']},'Config':'internal/settings/v2_config.go','StdLogger':'internal/logging/logger.go','UnsupportedSettingError':'internal/runtimeerrors/settings.go','HostSnapshot':'internal/model/host.go','Target':'internal/model/target.go','StreamLimits':'internal/transport/v2_stream_transport.go','executionContext':'internal/cluster/context.go'}
records=[];missing=[];ambiguous=[]
for x in original:
 key=(x['Kind'],x['Receiver'],x['Name']);matches=index[key];reason=None
 if key in explicit:
  path,name,reason=explicit[key];matches=[m for m in cur if m['File']==path and m['Name']==name] if path else []
 elif x['Name'] in prefer:matches=[m for m in matches if m['File']==prefer[x['Name']]]
 elif x['Name']=='init' and not x['Receiver']:
  path='internal/settings/version.go' if x['File']=='version.go' else 'internal/cluster/control.go'
  matches=[m for m in matches if m['File']==path]
 elif x['Name']=='waitCallReqDone':matches=[m for m in matches if pathlib.Path(m['File']).name==x['File']]
 if not matches and not reason:missing.append(key)
 if len(matches)>1:ambiguous.append((key,[(m['File'],m['Name']) for m in matches]))
 records.append({'original_file':x['File'],'original_name':x['Name'],'original_receiver':x['Receiver'],'kind':x['Kind'],'original_sha256':x['SHA'],'disposition':reason or 'Actual implementation declaration relocated once','matches':[{'file':m['File'],'name':m['Name'],'receiver':m['Receiver'],'sha256':m['SHA']} for m in matches]})
result={'accepted_predecessor':'c3505d578d530ad89e043a539d1be43bb85ff295','count':len(records),'missing':missing,'ambiguous':ambiguous,'mapping':records}
pathlib.Path((AUDIT/'runtime-declaration-mapping.json')).write_text(json.dumps(result,indent=2)+'\n')
print('runtime declarations',len(records),'missing',missing,'ambiguous',ambiguous)
assert not missing and not ambiguous
