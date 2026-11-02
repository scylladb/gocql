#!/usr/bin/env python3
"""Retain readable pprof reports and CPU-per-operation attribution."""
import json
from pathlib import Path
import re
import subprocess
import sys

out=Path(sys.argv[1]).resolve()
records=[]
for d in sorted(out.iterdir()):
    if not d.is_dir() or not (d/'measurement.json').exists():continue
    result=json.loads((d/'measurement.json').read_text());m=result['measurement']
    entries=[]
    specs=[('cpu-top',['-top','-nodecount=80'],d/'measurement.cpu.pprof'),
           ('cpu-cumulative',['-top','-cum','-nodecount=100'],d/'measurement.cpu.pprof'),
           ('alloc-space',['-top','-alloc_space','-base',str(d/'measurement.heap.before.pprof'),'-nodecount=60'],d/'measurement.heap.after.pprof'),
           ('emulator-cpu-top',['-top','-nodecount=40'],d/'emulator.cpu.pprof')]
    for name,args,source in specs:
        res=subprocess.run(['go','tool','pprof',*args,str(source)],text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,check=True)
        (d/(name+'.txt')).write_text(res.stdout)
    raw=subprocess.check_output(['go','tool','pprof','-top','-nodefraction=0','-nodecount=2000','-unit=ms',str(d/'measurement.cpu.pprof')],text=True)
    # Cumulative values overlap. Preserve rows separately; never sum them as
    # independent causes of the performance difference.
    for line in raw.splitlines():
        match=re.match(r'\s*([\d.]+)(ms)?\s+[\d.]+%\s+[\d.]+%\s+([\d.]+)(ms)?\s+[\d.]+%\s+(.+)',line)
        if not match:continue
        flat,_,cum,_,name=match.groups()
        if name in ('runtime.mallocgc','runtime.gcBgMarkWorker','runtime.newproc','runtime.duffcopy','internal/runtime/syscall.Syscall6','runtime.scanobject','runtime.selectgo') or 'InternalConnReader).Read' in name or 'executePage' in name or 'InternalRoutingPolicy).Plan' in name:
            entries.append({'function':name,'flat_cpu_us_per_op':float(flat)*1000/m['completed'],'cumulative_cpu_us_per_op':float(cum)*1000/m['completed']})
    records.append({'case':d.name,'completed':m['completed'],'cpu_seconds':m['cpu_seconds_including_drain'],'functions':entries})
(out/'profile-attribution.json').write_text(json.dumps({'note':'CPU sampling estimates normalized by completed operations; cumulative rows overlap; client CPU includes SHA256 validation and harness accounting.','cases':records},indent=2)+'\n')
print(f'Analyzed {len(records)} observations')
