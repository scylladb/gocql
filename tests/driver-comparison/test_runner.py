import copy, importlib.util, json, tempfile, unittest
from pathlib import Path
spec=importlib.util.spec_from_file_location('runner',Path(__file__).with_name('run.py'))
r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)
def result(rate=100):
 return {'concurrency':4,'measurement':{'errors':0,'affinity_error':'','admitted':100,'completed':100,'in_window':100,'latency':{'overflow':0,'p90_us':10,'p95_us':11,'p99_us':12},'throughput':rate,'duration_seconds':120,'target_rate':100,'rejected':0,'backlog_samples':[0]*30}}
class RunnerTest(unittest.TestCase):
 def test_stability(self):
  rows=[result() for _ in range(3)];self.assertEqual(r.stability(rows,'sustainable'),[])
  rows[1]['measurement']['latency']['p99_us']=24;self.assertTrue(any('p99_us' in x for x in r.stability(rows,'maximum')))
 def test_incomplete(self):self.assertTrue(r.stability([result()],'maximum'))
 def test_capacity_is_not_target(self):self.assertTrue(r.stability([result(98) for _ in range(3)],'sustainable'))
 def test_rejections_invalidate_sustainable(self):
  rows=[result() for _ in range(3)];rows[1]['measurement']['rejected']=1;self.assertTrue(r.stability(rows,'sustainable'))
 def test_raw_histograms_survive_compaction(self):
  with tempfile.TemporaryDirectory() as tmp:
   p=Path(tmp)/'round.json';r.write(p,{'measurement':{'latency':{'snapshot':{'Counts':[0,1,7]},'count':8}}});res=r.compact(p)
   with r.gzip.open(p.with_suffix('.histograms.json.gz'),'rt') as f:raw=json.load(f)
   self.assertEqual(raw['measurement-latency']['Counts'],[0,1,7]);self.assertEqual(r.compact(p),res)
if __name__=='__main__':unittest.main()
