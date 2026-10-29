# Copyright 2026 The Apache Software Foundation.
# Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE
# in the repository root, or https://www.apache.org/licenses/LICENSE-2.0.

import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from matrix import case_label, cases_for_phase, validate_matrix


def fixture(phase='all', repeats=6, legacy=False):
    cases = cases_for_phase(phase, legacy=legacy)
    env = {'arguments': {'phase': phase, 'repeats': repeats, 'cpus': '0,1,2,3'},
           'runs': len(cases)*repeats}
    if not legacy:
        env.update(schema_version=2, expected_cases=[case_label(c) for c in cases],
                   latency_sampling='stratified-random-reservoir-v1')
    rows = []
    for repetition in range(1, repeats+1):
        for case in cases:
            mode, mib, updates, skew, variant = case
            config = {'Mode': mode, 'MiB': mib, 'Updates': updates, 'Skew': skew,
                      'Cache': variant[1], 'Pin': variant[2], 'SocketCPU': variant[3],
                      'Batch': 32, 'CPUs': '0,1,2,3', 'SampleSeed': 20261029+repetition-1}
            workers = [{'Worker': i, 'TargetCPU': i, 'Operations': 800, 'CPUObservations': {str(i): 2},
                        'SampledCPUChanges': 0, 'IncomingBefore': i+4, 'IncomingAfter': i+4,
                        'SocketOtherSamples': 2, 'NAPIID': 0, 'LatencySamplesRetained': 2} for i in range(4)]
            result = {'Config': config, 'OperationsPerSecond': 32000, 'Operations': 3200,
                      'ProcessCPUSeconds': 0.1, 'Seconds': 0.1, 'CPUSecondsPerMillionOps': 31.25,
                      'WorkerOperationsCV': 0, 'BatchP99NS': 1000, 'Allocations': 0, 'Workers': workers}
            rows.append({'label': case_label(case), 'repetition': repetition, 'returncode': 0,
                         'result': result, 'wall_seconds': 0.2,
                         'server_cpu_seconds_including_client_setup_and_warmup': 0.05})
    return rows, env


class MatrixTests(unittest.TestCase):
    def test_all_supported_phases_use_exact_unique_repetitions(self):
        for phase, count in [('all',24),('cpu',16),('tcp',5),('skew',3),('smoke',5)]:
            for legacy in (False,True):
                with self.subTest(phase=phase,legacy=legacy):
                    rows, env = fixture(phase,2,legacy)
                    groups, missing = validate_matrix(rows,env)
                    self.assertEqual(len(groups), count)
                    self.assertEqual(missing, [])

    def test_duplicate_replacing_missing_repetition_is_rejected(self):
        rows, env = fixture()
        rows[-1] = copy.deepcopy(rows[23])
        with self.assertRaisesRegex(ValueError,'duplicate'):
            validate_matrix(rows,env)

    def test_missing_case_and_repetition_is_not_complete(self):
        rows, env = fixture()
        removed = rows.pop()
        _, missing = validate_matrix(rows,env)
        self.assertEqual(missing,[(removed['label'],removed['repetition'])])

    def test_unknown_case_and_out_of_range_repetition_are_rejected(self):
        for key,value in [('label','unknown'),('repetition',0),('repetition',7),('repetition',True)]:
            rows, env = fixture()
            rows[0][key] = value
            with self.subTest(key=key,value=value), self.assertRaises(ValueError):
                validate_matrix(rows,env)

    def test_mislabeled_config_failed_result_and_wrong_seed_are_rejected(self):
        for field,value in [('Cache','sharded'),('SampleSeed',0)]:
            rows, env = fixture()
            rows[0]['result']['Config'][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_matrix(rows,env)
        rows, env = fixture()
        rows[0]['returncode'] = 1
        with self.assertRaisesRegex(ValueError,'failed'):
            validate_matrix(rows,env)

    def summarize(self, rows, env, *flags):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root/'environment.json').write_text(json.dumps(env))
            (root/'runs.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in rows))
            run = subprocess.run([sys.executable,'-O',str(Path(__file__).with_name('summarize.py')),str(root),*flags],
                                 text=True,capture_output=True)
            summary = json.loads((root/'summary.json').read_text()) if (root/'summary.json').exists() else None
            return run, summary

    def test_cli_rejects_incomplete_even_under_python_optimization(self):
        rows, env = fixture('smoke',1)
        rows.pop()
        run, summary = self.summarize(rows,env)
        self.assertNotEqual(run.returncode,0)
        self.assertIsNone(summary)
        self.assertIn('incomplete matrix',run.stderr)
        run, summary = self.summarize(rows,env,'--allow-incomplete')
        self.assertEqual(run.returncode,0,run.stderr)
        self.assertFalse(summary['complete'])

    def test_old_latency_withheld_and_socket_affinity_comparison_not_emitted(self):
        rows, env = fixture('tcp',1,legacy=True)
        run, summary = self.summarize(rows,env)
        self.assertEqual(run.returncode,0,run.stderr)
        self.assertTrue(summary['complete'])
        self.assertFalse(summary['latency_valid'])
        self.assertTrue(all(s['batch_p99_ms_median'] is None for s in summary['summaries']))
        self.assertTrue(all(c['paired_median_batch_p99_ratio'] is None for c in summary['comparisons']))
        self.assertTrue(all('socket' not in c['comparison'] for c in summary['comparisons']))
        self.assertEqual(summary['socket_affinity_status'],'unsupported-observed-mismatch')

    def test_corrected_latency_remains_available(self):
        rows, env = fixture('tcp',1)
        run, summary = self.summarize(rows,env)
        self.assertEqual(run.returncode,0,run.stderr)
        self.assertTrue(summary['latency_valid'])
        self.assertTrue(all(s['batch_p99_ms_median'] == 0.001 for s in summary['summaries']))


if __name__ == '__main__':
    unittest.main()
