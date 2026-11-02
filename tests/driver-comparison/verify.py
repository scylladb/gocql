#!/usr/bin/env python3
"""Audit the complete retained matrix, source identities and raw histogram counts."""
import gzip, hashlib, importlib.util, json, math, sys
from pathlib import Path
HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('runner',HERE/'run.py');r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)
def verify(out):
 state=json.loads((out/'state.json').read_text());assert state['status']=='complete',state.get('status')
 assert len(state['chosen_sets'])==12,state['chosen_sets']
 report={'base_rounds':0,'total_recorded_rounds':0,'histograms_checked':0,'continued_instability':{},'processes_checked':0,'sources_checked':0}
 for key,paths in state['sets'].items():
  assert len(paths)==3,(key,paths)
  if key.endswith('-initial'):report['base_rounds']+=3
  rows=[]
  for relative in paths:
   p=out/relative;row=json.loads(p.read_text());rows.append(row);report['total_recorded_rounds']+=1
   m=row['measurement'];assert m['duration_seconds']==120
   if row['driver']=='v2':assert all(x['Token']['Set'] for x in row['evidence']['routing_samples'])
   else:assert row['evidence']['routing_key_hex']=='00000000'
   assert row['warmup']['duration_seconds']==30
   assert m['admitted']==m['completed'];assert m['in_window']+m['drained']+m['errors']==m['completed']
   assert m['throughput']==m['in_window']/120
   assert m['offered']==m['admitted']+m['rejected']
   if m['target_rate']>0:assert m['offered']==math.ceil(m['target_rate']*120)
   assert not m['affinity_error'];assert row['gomaxprocs']==1
   with gzip.open(p.with_suffix('.histograms.json.gz'),'rt') as f:raw=json.load(f)
   for phase in ('measurement','warmup'):
    for name in ('latency','dispatch_latency','queue_latency','schedule_lag'):
     h=row[phase][name];snapshot=raw[phase+'-'+name]
     assert snapshot['SignificantFigures']==3
     assert snapshot['LowestTrackableValue']==1 and snapshot['HighestTrackableValue']==60000000
     assert sum(snapshot['Counts'])==h['count'];assert h['overflow']==0
     if name!='schedule_lag':assert h['count']==row[phase]['completed']
     report['histograms_checked']+=1
  phase='sustainable' if '-sustainable-' in key else 'maximum'
  issues=r.stability(rows,phase);assert issues==state['instability'][key],(key,issues,state['instability'][key])
 assert report['base_rounds']==36,report
 for key,chosen in state['chosen_sets'].items():
  if state['instability'][chosen]:report['continued_instability'][key]=state['instability'][chosen]
  if '-sustainable-' in key:
   maximum=state['chosen_sets'][key.replace('-sustainable-','-maximum-')]
   target=.7*r.statistics.median(json.loads((out/p).read_text())['measurement']['throughput'] for p in state['sets'][maximum])
   for p in state['sets'][chosen]:assert json.loads((out/p).read_text())['measurement']['target_rate']==target
 for p in out.glob('*.process.json'):
  item=json.loads(p.read_text());assert item['masks'] in ([],['8']);assert item['affinity_error'] is None
  assert item['args'][:5]==['taskset','-c','8','env','GOMAXPROCS=1']
  log=p.with_name(p.name.replace('.process.json','.log')).read_text()
  assert 'AFFINITY initial:' in log and 'AFFINITY final:' in log,p
  report['processes_checked']+=1
 env=json.loads((out/'environment.json').read_text())
 assert env['git']=={'old':r.OLD,'v2':r.V2}
 for name,digest in env['source_files'].items():
  assert r.sha(out/'sources'/name)==digest,name;report['sources_checked']+=1
 wire=[]
 for name in ('old-buffered','v2-buffered','v2-stream'):
  item=json.loads((out/'probes'/(name+'-wire.json')).read_text())
  params=sorted((q['cql'],q['consistency'],q['flags'],q['page_size'],tuple(v['sha256'] for v in q['values'])) for q in item['requests'])
  wire.append(params)
 assert wire[0]==wire[1]==wire[2],'wire parameters differ'
 shutdown=json.loads((out/'shutdown.json').read_text());assert len(shutdown['nodes'])==3
 assert all(not n['state']['Running'] and all(not p['open'] for p in n['endpoints'].values()) for n in shutdown['nodes'])
 r.write(out/'verification.json',report)
 r.summarize(out,state)
 return report
if __name__=='__main__':print(json.dumps(verify(Path(sys.argv[1]).resolve()),indent=2))
