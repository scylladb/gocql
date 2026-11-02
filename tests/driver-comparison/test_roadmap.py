import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('roadmap', Path(__file__).with_name('roadmap_run.py'))
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
audit_spec = importlib.util.spec_from_file_location('roadmap_audit', Path(__file__).with_name('roadmap_audit.py'))
auditor = importlib.util.module_from_spec(audit_spec)
audit_spec.loader.exec_module(auditor)


def result(rate=0):
    return {'driver': 'v2', 'scenario': 'paged', 'stream': False,
            'concurrency': 4, 'gomaxprocs': 1,
            'evidence': {'protocol': 4, 'consistency': 'LOCAL_QUORUM',
                         'compression': 'none', 'tls': False, 'coalesce_wait_ns': 0,
                         'page_size': 32, 'prefetch': 0, 'retries': 0,
                         'metadata_refresh_ns': 0},
            'measurement': {'errors': 0, 'timeouts': 0, 'rejected': 0,
                            'affinity_error': '', 'affinity_checks': 30,
                            'duration_seconds': 30, 'target_rate': rate,
                            'queue_capacity': 1024, 'admitted': 100,
                            'completed': 100, 'in_window': 100,
                            'throughput': rate if rate else 100,
                            'backlog_samples': [0] * 30,
                            'latency': {'overflow': 0, 'p99_us': 100},
                            'dispatch_latency': {'overflow': 0},
                            'queue_latency': {'overflow': 0},
                            'schedule_lag': {'overflow': 0},
                            'cpu_seconds_including_drain': 1,
                            'alloc_bytes': 1000, 'mallocs': 100}}


class RoadmapTest(unittest.TestCase):
    def test_audit_preserves_input_module_hash_and_relocation(self):
        import hashlib
        with tempfile.TemporaryDirectory() as tmp:
            harness = Path(tmp)
            original = ('module test\nreplace github.com/gocql/gocql => ../../.codex-cache/driver-comparison/old-source\n'
                        'replace github.com/scylladb/gocql/v2 => ../..\n')
            relocated = original.replace('../../.codex-cache/driver-comparison/old-source', '/frozen/old').replace('=> ../..', '=> /frozen/source')
            module = harness / 'go.mod'
            module.write_text(relocated)
            expected = {'go.mod': hashlib.sha256(original.encode()).hexdigest()}
            result = auditor.harness_provenance(harness, expected, Path('/frozen/source'), Path('/frozen/old'))
            self.assertEqual(result['issues'], [])
            self.assertEqual(module.read_text(), relocated)
            self.assertEqual(result['go_mod_input_relocation']['manifest_hash_scope'], 'input harness go.mod before path relocation')
            module.write_text(relocated + 'require unexpected v1.0.0\n')
            self.assertTrue(auditor.harness_provenance(harness, expected, Path('/frozen/source'), Path('/frozen/old'))['issues'])

    def test_audit_atomic_output_never_overwrites(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'audit.json'
            auditor.atomic_new_json(path, {'count': 1})
            with self.assertRaises(FileExistsError):
                auditor.atomic_new_json(path, {'count': 2})
            self.assertEqual(json.loads(path.read_text()), {'count': 1})

    def test_audit_detects_corrupt_profile_and_histogram_count(self):
        import gzip
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'profile.gz'
            path.write_bytes(gzip.compress(b'profile bytes')[:-4])
            with self.assertRaises((EOFError, OSError)):
                auditor.gzip_integrity(path)
            raw = {'LowestTrackableValue': 1, 'HighestTrackableValue': 60000000,
                   'SignificantFigures': 3, 'Counts': [0, 2]}
            data = result()
            for name in ('latency', 'dispatch_latency', 'queue_latency', 'schedule_lag'):
                data['measurement'][name].update(count=2, snapshot=copy.deepcopy(raw))
            (Path(tmp) / 'measurement.json').write_text(json.dumps(data))
            auditor.audit_histograms(Path(tmp), data)
            data['measurement']['latency']['count'] = 3
            with self.assertRaisesRegex(ValueError, 'raw counts differ'):
                auditor.audit_histograms(Path(tmp), data)

    def test_balanced_order(self):
        self.assertEqual(sum((list(x) for x in r.order('screen')), []), list('ABBA'))
        self.assertEqual(r.order('confirm').count(('A', 'B')), 3)
        self.assertEqual(r.order('confirm').count(('B', 'A')), 3)

    def test_baseline_only_a_and_no_comparison(self):
        self.assertEqual(r.order('baseline'), [('A',)])
        rows = [{'case': 'paged-buffered', 'rate': rate, 'pair': 1, 'variant': 'A',
                 'metrics': r.metrics(result(rate)), 'failures': []}
                for rate in (0, r.CASES['paged-buffered'][3])]
        summary = r.summarize(rows, 'baseline')
        self.assertFalse(summary['accepted'])
        for case in summary['cases'].values():
            self.assertEqual(case['completed_pairs'], 0)
            self.assertEqual(case['expected_pairs'], 0)
            self.assertFalse(case['promising'])
            self.assertFalse(case['confirmed_primary_threshold'])
            self.assertEqual(set(case['variant_medians']), {'A'})
            self.assertEqual(case['variant_medians']['A']['bytes_per_op'], 10)

    def test_failed_client_retains_observation_and_counters(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            data = result()
            data['measurement']['errors'] = 64
            (directory / 'measurement.json').write_text(json.dumps(data))
            (directory / 'measurement.json.process.json').write_text(json.dumps({'masks': ['8'], 'checks': 30}))
            for suffix in ('cpu.pprof', 'heap.before.pprof', 'heap.after.pprof'):
                (directory / ('measurement.' + suffix)).write_bytes(b'profile')
            record = r.read_observation(directory, 'paged-buffered', 0, 1, 'A', 1)
            self.assertEqual(record['counters']['errors'], 64)
            self.assertEqual(record['counters']['completed'], 100)
            self.assertEqual(record['returncode'], 1)
            self.assertIn('client returned 1', record['failures'])
            summary = r.summarize([record], 'baseline')['cases']['paged-buffered:0']
            self.assertFalse(summary['valid'])
            self.assertFalse(summary['promising'])
            self.assertEqual(summary['variant_medians']['A']['bytes_per_op'], 10)

    def test_failed_client_without_result_and_without_completions(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RuntimeError, 'no measurement result'):
                r.read_observation(Path(tmp), 'paged-buffered', 0, 1, 'A', 1)
        data = result()
        data['measurement']['completed'] = 0
        self.assertIsNone(r.metrics(data)['cpu_per_op'])
        record = {'case': 'paged-buffered', 'rate': 0, 'pair': 1, 'variant': 'A',
                  'metrics': r.metrics(data), 'failures': ['no completions']}
        summary = r.summarize([record], 'baseline')['cases']['paged-buffered:0']
        self.assertIsNone(summary['variant_medians']['A']['cpu_per_op'])
        self.assertFalse(summary['promising'])

    def test_deterministic_paired_bootstrap(self):
        values = [3, 4, 5, 6, 7, 8]
        a = r.bootstrap(values)
        self.assertEqual(a, r.bootstrap(values))
        self.assertGreater(a['ci95_percent'][0], 0)
        self.assertEqual(a['mean_percent'], 5.5)
        self.assertLessEqual(r.bootstrap([-10, 10])['ci95_percent'][0], 0)

    def test_failures_are_strict(self):
        self.assertEqual(r.failures(result(), 'paged-buffered', 0), [])
        for key, value in [('errors', 1), ('timeouts', 1), ('rejected', 1),
                           ('queue_capacity', 2048), ('duration_seconds', 29),
                           ('affinity_checks', 0), ('completed', 99)]:
            bad = result()
            bad['measurement'][key] = value
            self.assertTrue(r.failures(bad, 'paged-buffered', 0), key)
        bad = result()
        bad['evidence']['retries'] = 1
        self.assertTrue(r.failures(bad, 'paged-buffered', 0))

    def test_fixed_rate_and_backlog(self):
        target = r.CASES['paged-buffered'][3]
        good = result(target)
        self.assertFalse(r.failures(good, 'paged-buffered', target))
        good['measurement']['throughput'] = .98 * target
        self.assertTrue(r.failures(good, 'paged-buffered', target))
        good = result(target)
        good['measurement']['backlog_samples'] = [0] * 15 + [20] * 15
        self.assertTrue(r.failures(good, 'paged-buffered', target))

    def test_incomplete_pairs_cannot_pass(self):
        rows = [{'case': 'paged-buffered', 'rate': 0, 'pair': 1, 'variant': 'A',
                 'metrics': r.metrics(result()), 'failures': []}]
        summary = r.summarize(rows, 'confirm')
        self.assertFalse(summary['accepted'])
        self.assertFalse(summary['cases']['paged-buffered:0']['confirmed_primary_threshold'])

    def test_threshold_and_p99_do_not_grant_acceptance(self):
        rows = []
        for pair in range(1, 7):
            for variant in ('A', 'B'):
                m = r.metrics(result())
                if variant == 'B':
                    m['cpu_per_op'] *= .95
                    m['p99_us'] *= 1.06
                rows.append({'case': 'paged-buffered', 'rate': 0, 'pair': pair,
                             'variant': variant, 'metrics': m, 'failures': []})
        summary = r.summarize(rows, 'confirm')
        case = summary['cases']['paged-buffered:0']
        self.assertTrue(case['confirmed_primary_threshold'])
        self.assertFalse(case['p99_pass'])
        self.assertFalse(summary['accepted'])
        rows[0]['failures'] = ['rejected arrival']
        self.assertFalse(r.summarize(rows, 'confirm')['cases']['paged-buffered:0']['confirmed_primary_threshold'])

    def test_manifest_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            binary = Path(tmp) / 'binary'
            binary.write_bytes(b'frozen')
            path = Path(tmp) / 'manifest.json'
            data = {'git_head': 'a' * 40, 'binary_sha256': r.r.sha(binary),
                    'driver_source_hashes': {'a.go': 'b' * 64}, 'build': 'build settings'}
            path.write_text(json.dumps(data))
            with mock.patch.object(r.subprocess, 'check_output', return_value='gocql-driver-comparison github.com/scylladb/gocql/v2'):
                self.assertEqual(r.read_manifest(path, binary)['git_head'], 'a' * 40)
                binary.write_bytes(b'changed')
                with self.assertRaisesRegex(ValueError, 'binary hash mismatch'):
                    r.read_manifest(path, binary)


if __name__ == '__main__':
    unittest.main()
