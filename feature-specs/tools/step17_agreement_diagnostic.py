# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Bounded MV recreation diagnostic; logging-only overlays, no release credit."""
import hashlib,json,os,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
full=os.environ.get('GOCQL_STEP17_AGREEMENT_DIAGNOSTIC')=='full'
OUT=Path('/extra/scylladb/gocql-api-test-cluster/step17/cassandra-agreement-diagnostic')/('run-full'if full else'run');OUT.mkdir(parents=True,exist_ok=True)
changes={}
original=ROOT/'internal/cluster/v2_metadata.go';body=original.read_text()
needle='v, err := InternalReadV2AgreementHost(ctx, job.id, job.conns)'
assert needle in body
body=body.replace(needle,needle+'\n fmt.Printf("D17 agreement required=%s host=%s connections=%d version=%s error=%v ctx=%v\\n",required,job.id,len(job.conns),v,err,ctx.Err())',1)
needle='return nil\n\t\t}\n\t\ttimer := time.NewTimer(interval)'
assert needle in body
body=body.replace(needle,'fmt.Printf("D17 agreement SUCCESS required=%s versions=%v ctx=%v\\n",required,versions,ctx.Err())\n '+needle,1)
replacement=OUT/'metadata.overlay.go';replacement.write_text(body);changes[str(original)]=str(replacement)
original=ROOT/'internal/execution/v2_execute.go';body=original.read_text()
needle='if config.AutomaticAgreement {'
assert needle in body
body=body.replace(needle,'fmt.Printf("D17 DDL POST statement=%q keyspace=%q host=%s automatic=%t refresh=%t ctx=%v\\n",request.InternalStatement,response.Keyspace,implEndpoint.InternalHostInfoHostUUID(conn.InternalHost),config.AutomaticAgreement,config.RefreshAfterAgreement,ctx.Err())\n '+needle,1)
needle='return response, err\n\t\t\t\t}'
# Restrict replacement to the actual agreement block.
start=body.index('if config.AutomaticAgreement {');before,after=body[:start],body[start:]
after=after.replace(needle,'fmt.Printf("D17 agreement FAILED statement=%q error=%v ctx=%v\\n",request.InternalStatement,err,ctx.Err())\n '+needle,1)
needle='if config.AutomaticAgreement && config.RefreshAfterAgreement {'
after=after.replace(needle,'fmt.Printf("D17 agreement DONE statement=%q ctx=%v\\n",request.InternalStatement,ctx.Err())\n '+needle,1)
needle='if refreshErr != nil && !errors.Is(refreshErr, ErrKeyspaceDoesNotExist) {'
after=after.replace(needle,'fmt.Printf("D17 refresh DONE statement=%q error=%v ctx=%v\\n",request.InternalStatement,refreshErr,ctx.Err())\n '+needle,1)
body=before+after;replacement=OUT/'execution.overlay.go';replacement.write_text(body);changes[str(original)]=str(replacement)
overlay=OUT/'overlay.json';overlay.write_text(json.dumps({'Replace':changes},indent=2)+'\n')
if full:
    original=ROOT/'recreate_test.go';body=original.read_text().replace('"regexp"','"regexp"\n"runtime/pprof"')
    needle='func TestRecreateSchema(t *testing.T) {';assert needle in body
    start=body.index(needle);at=body.index('t.Parallel()',start)+len('t.Parallel()')
    body=body[:at]+'\n diagnosticDump:=time.AfterFunc(45*time.Second,func(){os.Stdout.WriteString("D17 RECREATE GOROUTINES\\n");pprof.Lookup("goroutine").WriteTo(os.Stdout,1)});defer diagnosticDump.Stop()'+body[at:]
    replacement=OUT/'recreate.overlay.go';replacement.write_text(body);changes[str(original)]=str(replacement)
    overlay.write_text(json.dumps({'Replace':changes},indent=2)+'\n')
config=json.loads((OUT.parents[1]/'release/cassandra-config.json').read_text());lane=config['lanes'][0]
lane['modes']=[dict(name='mv-v5-raw-tls',tests='^Test'if full else'^TestRecreateSchema$/^Materialized_Views$',args=['-proto=5','-compressor=no-compression','-runssl','-test.count='+('1'if full else'3')])]
path=OUT/'config.json';path.write_text(json.dumps(config,indent=2)+'\n')
sys.path.insert(0,str(ROOT/'tests/fixtures'));import native
Original=native.Runner
class DiagnosticRunner(Original):
    def command(self,name,command,env=None,check=True):
        if command[:2]==['go','test']:command=command[:2]+['-overlay',str(overlay)]+command[2:]
        self.receipt['logging_only_overlay']={str(p):hashlib.sha256(Path(p).read_bytes()).hexdigest()for p in [overlay]+[Path(p)for p in changes.values()]}
        return super().command(name,command,env,check)
native.Runner=DiagnosticRunner
if __name__=='__main__':
    assert not(OUT/'native.json').exists(),'inspect exact retained diagnostic owner instead of restarting'
    sys.argv=[sys.argv[0],'--config',str(path),'--state','/extra/scylladb/gocql-api-test-cluster/step08','--output',str(OUT/'native.json')]
    native.main()
