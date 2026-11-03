import importlib.util
from pathlib import Path
import tempfile
import unittest
import json
import os
import io
from unittest import mock

spec = importlib.util.spec_from_file_location('convergence', Path(__file__).with_name('redesign_compare.py'))
c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)
fixture = importlib.util.spec_from_file_location('fixtures', Path(__file__).with_name('test_roadmap.py'))
f = importlib.util.module_from_spec(fixture)
fixture.loader.exec_module(f)


def rows(stage='confirm'):
    result = []
    for case in c.roadmap.CASES:
        workers = c.roadmap.CASES[case][2]
        for load in c.LOADS:
            for pair in range(1, 7 if stage == 'confirm' else 3):
                for variant in ('A', 'B'):
                    metrics = dict(throughput=100, p99_us=100, cpu_per_op=10, bytes_per_op=10, allocs_per_op=10)
                    if variant == 'B':
                        metrics.update(throughput=105 if load == 'maximum' else 100, cpu_per_op=9)
                    result.append(dict(case=case, workers=workers, load=load, pair=pair,
                                       variant=variant, metrics=metrics, failures=[], rate=0))
    return result


def required():
    return [(case, values[2], load) for case, values in c.roadmap.CASES.items() for load in c.LOADS]


class ConvergenceTest(unittest.TestCase):
    def test_capacity_reference_preserves_rates_and_checks_workload_equivalence(self):
        with tempfile.TemporaryDirectory() as tmp:
            before, after = Path(tmp) / 'before', Path(tmp) / 'after'
            for directory in (before, after):
                directory.mkdir()
                (directory / 'main.go').write_text(c.HERE.joinpath('main.go').read_text())
                (directory / 'old.go').write_text('unchanged master adapter')
            previous = dict(binary_sha256='frozen', verified_build='old: go1.25.0', verified_harness=str(before))
            current = dict(verified_build='new: go1.25.0', verified_harness=str(after))
            rates = dict(master_binary_sha256='frozen', rates={'case:w4': {'70': 70, '85': 85}})
            frozen = json.dumps(rates)
            with mock.patch.object(c, 'identity', return_value=previous):
                proof = c.capacity_reference(Path('manifest'), Path('binary'), current, rates)
                self.assertEqual(len(proof['workload_hashes']), 13)
                self.assertEqual(json.dumps(rates), frozen)
                source = (after / 'main.go').read_text()
                self.assertIn('const keys = 100000', source)
                (after / 'main.go').write_text(source.replace('const keys = 100000', 'const keys = 99999'))
                with self.assertRaisesRegex(ValueError, 'canonical workload differs'):
                    c.capacity_reference(Path('manifest'), Path('binary'), current, rates)
                (after / 'main.go').write_text(source)
                (after / 'old.go').write_text('changed master adapter')
                with self.assertRaisesRegex(ValueError, 'adapter differs'):
                    c.capacity_reference(Path('manifest'), Path('binary'), current, rates)

    def test_cpu_sets_use_kernel_format(self):
        self.assertEqual(c.cpu_list('9,8,12,10,18'), '8-10,12,18')
        self.assertEqual(c.cpu_set('0-1,4-5'), {0, 1, 4, 5})
        for invalid in ('-1', '4-2', '', '1-2-3'):
            with self.assertRaises(ValueError):
                c.cpu_list(invalid)

    def test_scylla_requires_qualified_dedicated_host_before_load(self):
        args = ['compare', '--baseline', 'unused', '--baseline-manifest', 'unused', '--candidate', 'unused',
                '--candidate-manifest', 'unused', '--output', 'unused', '--backend', 'scylla']
        with mock.patch('sys.argv', args), mock.patch('sys.stderr', new_callable=io.StringIO) as stderr, mock.patch.object(c.r, 'client') as client:
            with self.assertRaises(SystemExit) as error:
                c.main()
            self.assertEqual(error.exception.code, 2)
            self.assertIn('dedicated-host qualification', stderr.getvalue())
            client.assert_not_called()

    def test_local_receipt_requires_explicit_scope_and_one_cpu(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            evidence = directory / 'evidence.json'
            evidence.write_text('{}')
            names = ('compiler', 'server_image', 'rf3_topology', 'cpu_topology', 'storage',
                     'original_write_maxima', 'write_controls_52503_01', 'wire_dataset_equivalence', 'network_delay')
            receipt = dict(qualified=True, dedicated_host=False, local_cluster=True,
                           hostname=c.socket.gethostname(), hosts='a,b,c', references=c.REFERENCES,
                           boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(), gomaxprocs=1,
                           cpu_roles=dict(client=[8], server=[0, 1, 4, 5, 6, 7],
                                          monitoring=[2, 18], development=[11, 12, 13, 14, 15]),
                           evidence={name: dict(path=evidence.name, sha256=c.r.sha(evidence)) for name in names})
            path = directory / 'receipt.json'
            path.write_text(json.dumps(receipt))
            with self.assertRaises(ValueError):
                c.qualification(path, 'a,b,c')
            self.assertFalse(c.qualification(path, 'a,b,c', local_cluster=True)['dedicated_host'])
            receipt['blocked_controls'] = ['historical control']
            path.write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, 'overload policy'):
                c.qualification(path, 'a,b,c', local_cluster=True)
            c.qualification(path, 'a,b,c', local_cluster=True, allow_reference_overload=True)
            receipt['gomaxprocs'] = 2
            path.write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, 'exactly one CPU'):
                c.qualification(path, 'a,b,c', local_cluster=True, allow_reference_overload=True)
            receipt['gomaxprocs'] = 1
            receipt['boot_id'] = 'old boot'
            path.write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, 'another boot'):
                c.qualification(path, 'a,b,c', local_cluster=True, allow_reference_overload=True)

    def test_resume_keeps_completed_failures_and_archives_unfinished_attempts(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp)
            context = dict(stage='confirm', runner_hashes={'run.py': 'r', 'roadmap_run.py': 'm', 'redesign_compare.py': 'old'},
                           identities={v: dict(binary_sha256=v) for v in ('A', 'B')})
            (source / 'context.json').write_text(json.dumps(context))
            (source / 'state.json').write_text(json.dumps(dict(status='interrupted')))
            completed = dict(case='write', workers=64, load='maximum', pair=1, variant='A',
                             directory='complete', metrics={'throughput': 1}, failures=['unfavorable failure'])
            unfinished = dict(completed, variant='B', directory='unfinished', metrics={}, failures=['observation incomplete'])
            (source / 'observations.json').write_text(json.dumps([completed, unfinished]))
            current = dict(context, runner_hashes=dict(context['runner_hashes'], **{'redesign_compare.py': 'new'}))
            rows, interrupted, provenance = c.resume_records(source, current)
            self.assertEqual(rows[0]['failures'], completed['failures'])
            self.assertEqual(len(rows), 1)
            self.assertEqual(len(interrupted), 1)
            self.assertTrue(provenance['orchestration_changed'])
            (source / 'unfinished').mkdir()
            (source / 'unfinished/measurement.json').write_text('{}')
            with self.assertRaisesRegex(ValueError, 'recovery before resume'):
                c.resume_records(source, current)
            current['runner_hashes']['run.py'] = 'changed measurement harness'
            with self.assertRaisesRegex(ValueError, 'measurement harness differs'):
                c.resume_records(source, current)

    def test_resume_reorders_only_at_complete_pair_boundaries(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp)
            matrix = [['write', 64, 'maximum'], ['paged-stream', 4, '70']]
            before = dict(stage='confirm', matrix=matrix,
                          runner_hashes={'run.py': 'r', 'roadmap_run.py': 'm', 'redesign_compare.py': 'orchestration'},
                          identities={v: dict(binary_sha256=v) for v in ('A', 'B')})
            current = dict(before, matrix=list(reversed(matrix)))
            (source / 'context.json').write_text(json.dumps(before))
            (source / 'state.json').write_text(json.dumps(dict(status='interrupted')))
            rows = [dict(case='write', workers=64, load='maximum', pair=1, variant=v,
                         directory=v, metrics={'throughput': 1}, failures=[]) for v in ('A', 'B')]
            path = source / 'observations.json'
            path.write_text(json.dumps(rows))
            completed, _, provenance = c.resume_records(source, current)
            self.assertEqual(len(completed), 2)
            self.assertTrue(provenance['execution_order_changed'])
            path.write_text(json.dumps(rows[:1]))
            with self.assertRaisesRegex(ValueError, 'incomplete pair'):
                c.resume_records(source, current)
            path.write_text(json.dumps(rows))
            with self.assertRaisesRegex(ValueError, 'matrix'):
                c.resume_records(source, dict(current, matrix=matrix[:1]))

    def test_dirty_source_cannot_impersonate_a_pinned_reference(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            source = directory / 'source'
            source.mkdir()
            code = source / 'driver.go'
            code.write_text('original production source')
            expected = {'driver.go': c.r.sha(code)}
            code.write_text('modified production source')
            harness = directory / 'harness'
            harness.mkdir()
            (harness / 'main.go').write_text('harness')
            binary = directory / 'binary'
            binary.write_text('binary')
            manifest = directory / 'manifest.json'
            data = dict(git_head=c.REFERENCES['parked'], build='go1.25.0', frozen_source=str(source),
                        driver_source_hashes={'driver.go': c.r.sha(code)}, binary_sha256=c.r.sha(binary),
                        harness_source_hashes={'main.go': c.r.sha(harness / 'main.go')})
            manifest.write_text(json.dumps(data))
            with mock.patch.object(c, 'pinned_hashes', return_value=expected):
                with self.assertRaisesRegex(ValueError, 'differs from Git revision'):
                    c.identity(manifest, binary, 'parked')

    def test_process_audit_survives_interruption(self):
        class Process:
            pid = os.getpid()
            returncode = None
            def poll(self):
                return self.returncode
            def terminate(self):
                self.returncode = -15
            def wait(self, timeout):
                return self.returncode
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'measurement.json'
            with mock.patch.object(c.r.subprocess, 'Popen', return_value=Process()), mock.patch.object(c.r.time, 'sleep', side_effect=KeyboardInterrupt):
                with self.assertRaises(KeyboardInterrupt):
                    c.r.client(path, 'unused', 'nonmatching-test-process', 'run')
            process = json.loads(path.with_suffix('.json.process.json').read_text())
            self.assertEqual(process['returncode'], -15)
            self.assertIn('args', process)

    def test_confirmation_alternates(self):
        self.assertEqual(c.roadmap.order('confirm'), [('A', 'B'), ('B', 'A')] * 3)

    def test_no_arbitrary_gain_threshold(self):
        data = rows()
        for row in data:
            if row['variant'] == 'B':
                row['metrics']['throughput'] = 100.1 if row['load'] == 'maximum' else 100
                row['metrics']['cpu_per_op'] = 10
        self.assertEqual(c.summarize(data, 'confirm', required())['outcome'], 'retained')
        self.assertEqual(c.summarize(data, 'screen', required())['outcome'], 'inconclusive')

    def test_no_partial_matrix_or_bad_pair_adoption(self):
        data = rows()
        self.assertEqual(c.summarize(data[:-1], 'confirm', required())['outcome'], 'inconclusive')
        self.assertEqual(c.summarize(data + [data[0]], 'confirm', required())['outcome'], 'blocked')
        self.assertEqual(c.summarize(data[:24], 'confirm', required())['outcome'], 'inconclusive')
        data[0]['failures'] = ['error']
        self.assertEqual(c.summarize(data, 'confirm', required())['outcome'], 'blocked')

    def test_repeated_inconclusive_set_retains_both_sets(self):
        data = rows()
        for row in data:
            row['metrics'] = dict(throughput=100, p99_us=100, cpu_per_op=10, bytes_per_op=10, allocs_per_op=10)
        first = c.summarize(data, 'confirm', required())
        self.assertTrue(first['repeat_required'])
        repeated = data + [dict(row, pair=row['pair'] + 6) for row in data]
        summary = c.summarize(repeated, 'confirm', required(), confirmation_sets=2)
        self.assertEqual(summary['outcome'], 'inconclusive')
        self.assertFalse(summary['repeat_required'])
        self.assertEqual(summary['cases']['write:w64:85']['metrics']['p99_us']['pairs'], 12)

    def test_latency_uses_paired_scheduled_p99_at_matched_rates(self):
        data = rows()
        for row in data:
            if row['variant'] == 'B' and row['case'] == 'write' and row['load'] == '85':
                row['metrics']['p99_us'] = 100.1
        self.assertEqual(c.summarize(data, 'confirm', required())['outcome'], 'rejected')
        for row in data:
            if row['variant'] == 'B' and row['load'] == 'maximum':
                row['metrics']['p99_us'] = 10000
            elif row['variant'] == 'B':
                row['metrics']['p99_us'] = 100
        self.assertEqual(c.summarize(data, 'confirm', required())['outcome'], 'retained')

    def test_regression_in_another_case_blocks(self):
        data = rows()
        for row in data:
            if row['variant'] == 'B' and row['case'] == 'paged-stream':
                row['metrics']['allocs_per_op'] = 11
        self.assertEqual(c.summarize(data, 'confirm', required())['outcome'], 'rejected')

    def test_bootstrap_reproducible_and_noise_floor(self):
        summary = c.paired_ratio([.99, 1.01] * 3)
        self.assertEqual(summary, c.paired_ratio([.99, 1.01] * 3))
        self.assertLess(summary['ci95'][0], 1)
        self.assertGreater(summary['ci95'][1], 1)
        self.assertEqual(summary['resamples'], 20000)

    def test_zero_allocations_remain_a_valid_improvement(self):
        data = rows()
        for row in data:
            if row['variant'] == 'B':
                row['metrics'].update(allocs_per_op=0, bytes_per_op=0)
        summary = c.summarize(data, 'confirm', required())
        self.assertEqual(summary['outcome'], 'retained')
        self.assertEqual(summary['cases']['write:w64:85']['metrics']['allocs_per_op']['ci95'], [0, 0])

    def test_configurable_duration_and_worker_queue(self):
        result = f.result()
        result['concurrency'] = 1024
        result['measurement'].update(duration_seconds=120, queue_capacity=4096)
        self.assertFalse(c.roadmap.failures(result, 'paged-buffered', 0, seconds=120, workers=1024))
        self.assertTrue(c.roadmap.failures(result, 'paged-buffered', 0))
        result = f.result(100)
        result['measurement'].update(duration_seconds=120, backlog_samples=[0] * 90 + [100] * 30)
        self.assertTrue(c.roadmap.failures(result, 'paged-buffered', 100, seconds=120))

    def test_freeze_preserves_backend_and_rejects_unhealthy_controls(self):
        context = dict(backend='emulator', hosts='emulator', gomaxprocs=1, identities={'A': {'binary_sha256': 'a' * 64}})
        data = [dict(case='write', workers=64, pair=i, variant='A', rate=0,
                     directory=str(i), metrics={'throughput': 100}, failures=[]) for i in range(3)]
        frozen = c.freeze_rates(data, context)
        self.assertEqual(frozen['rates']['write:w64']['85'], 85)
        data[2]['failures'] = ['rejected arrivals']
        with self.assertRaises(ValueError):
            c.freeze_rates(data, context)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'rates.json'
            path.write_text(json.dumps(frozen))
            with self.assertRaises(ValueError):
                c.load_rates(path, 'scylla', 'emulator')
            frozen['rates']['write:w64']['70'] = 69
            path.write_text(json.dumps(frozen))
            with self.assertRaises(ValueError):
                c.load_rates(path, 'emulator', 'emulator')

    def test_frozen_rate_provenance_and_raw_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            baseline = Path(tmp)
            context = dict(backend='emulator', hosts='emulator', reference='master', profiling=False,
                           gomaxprocs=1, identities={'A': {'binary_sha256': 'a' * 64, 'git_head': c.MASTER}})
            data = [dict(case='write', workers=64, pair=i, variant='A', rate=0,
                         directory=str(i), metrics={'throughput': 100}, failures=[]) for i in range(1, 4)]
            (baseline / 'context.json').write_text(json.dumps(context))
            (baseline / 'observations.json').write_text(json.dumps(data))
            frozen = c.freeze_rates(data, context)
            frozen['provenance'] = dict(directory=tmp, files={name: c.r.sha(baseline / name)
                                       for name in ('context.json', 'observations.json')})
            path = baseline / 'frozen-rates.json'
            path.write_text(json.dumps(frozen))
            self.assertEqual(c.load_rates(path, 'emulator', 'emulator')['rates']['write:w64']['70'], 70)
            with self.assertRaises(ValueError):
                c.load_rates(path, 'emulator', 'emulator', cpus='9')
            (baseline / 'observations.json').write_text('[]')
            with self.assertRaisesRegex(ValueError, 'evidence changed'):
                c.load_rates(path, 'emulator', 'emulator')

    def test_unprofiled_observations_need_no_profiles(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            (directory / 'measurement.json').write_text(json.dumps(f.result()))
            (directory / 'measurement.json.process.json').write_text(json.dumps({'masks': ['8'], 'checks': 1}))
            self.assertFalse(c.roadmap.read_observation(directory, 'paged-buffered', 0, 1, 'A', 0, profiling=False)['failures'])
            self.assertTrue(c.roadmap.read_observation(directory, 'paged-buffered', 0, 1, 'A', 0)['failures'])
            data = f.result()
            data['instrumentation'] = dict(cpu_profiling=False, forced_gc=False, heap_sample_rate=1)
            (directory / 'measurement.json').write_text(json.dumps(data))
            self.assertTrue(c.roadmap.read_observation(directory, 'paged-buffered', 0, 1, 'A', 0, profiling=False)['failures'])

    def test_interrupted_pair_remains_blocked(self):
        data = rows()
        data[-1].update(metrics={}, failures=['observation incomplete'])
        summary = c.summarize(data, 'confirm', required())
        self.assertEqual(summary['outcome'], 'blocked')
        self.assertFalse(summary['complete'])

    def test_failed_warmup_and_completed_phase_error_are_preserved(self):
        data = f.result()
        data['warmup'] = dict(f.result()['measurement'], rejected=1)
        self.assertTrue(c.roadmap.failures(data, 'paged-buffered', 0))
        data = f.result()
        data['measurement_error'] = 'affinity audit failed'
        self.assertTrue(c.roadmap.failures(data, 'paged-buffered', 0))

    def run_mock_cli(self, directory, fail=False):
        cache = directory / 'cache'
        cache.mkdir()
        binaries = {name: cache / name for name in ('A', 'B')}
        for name, binary in binaries.items():
            binary.write_text(name)
        output = directory / 'results'
        args = ['compare', '--baseline', str(binaries['A']), '--baseline-manifest', 'unused',
                '--reference', 'latest', '--candidate', str(binaries['B']), '--candidate-manifest', 'unused',
                '--output', str(output), '--emulator-pid', str(os.getpid()), '--stage', 'confirm',
                '--case', 'paged-buffered', '--load', 'maximum', '--seconds', '2', '--warmup', '0']
        def identity(path, binary, reference=None):
            return dict(binary_sha256=c.r.sha(binary), git_head=c.REFERENCES['parked'])
        def client(path, hosts, binary, mode, *args, **kwargs):
            if mode == 'run':
                if fail:
                    return 1
                result = f.result()
                result['measurement']['duration_seconds'] = 2
                if binary == 'B':
                    result['measurement']['throughput'] *= 1.01
                path.write_text(json.dumps(result))
                path.with_suffix(path.suffix + '.process.json').write_text(json.dumps({'masks': ['8'], 'checks': 1}))
            return 0
        snapshot = {'emulator': {'failures': 0}, 'emulator_process': {'threads': {'1': {'affinity': [0, 1, 4, 5]}}}}
        with (mock.patch('sys.argv', args), mock.patch.object(c, 'identity', side_effect=identity),
              mock.patch.object(c.r, 'CACHE', cache), mock.patch.object(c.r, 'client', side_effect=client),
              mock.patch.object(c.roadmap, 'snapshot', return_value=snapshot),
              mock.patch.object(c.os, 'sched_setaffinity'), mock.patch.object(c.time, 'sleep'),
              mock.patch.object(c.signal, 'signal'), mock.patch('builtins.print')):
            if fail:
                with self.assertRaisesRegex(RuntimeError, 'no measurement result'):
                    c.main()
            else:
                c.main()
        return output

    def test_cli_cannot_retain_short_partial_confirmation(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = self.run_mock_cli(Path(tmp))
            summary = json.loads((output / 'summary.json').read_text())
            self.assertEqual(summary['outcome'], 'inconclusive')
            self.assertEqual(len(json.loads((output / 'observations.json').read_text())), 12)
            self.assertEqual(json.loads((output / 'state.json').read_text())['status'], 'complete')

    def test_cli_preserves_missing_measurement_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = self.run_mock_cli(Path(tmp), fail=True)
            self.assertEqual(json.loads((output / 'summary.json').read_text())['outcome'], 'blocked')
            rows = json.loads((output / 'observations.json').read_text())
            self.assertEqual(len(rows), 1)
            self.assertTrue(rows[0]['failures'])
            self.assertEqual(json.loads((output / 'state.json').read_text())['status'], 'failed')

class ReferenceOverloadPolicyTest(unittest.TestCase):
    def test_approved_overloaded_reference_remains_blocked_without_vetoing_valid_confirmation(self):
        data = rows()
        for row in data:
            if row['case'] == 'write' and row['load'] == '85':
                row.update(failures=['rejected arrivals'], capacity_only_failure=True)
        strict = c.summarize(data, 'confirm', required())
        self.assertEqual(strict['outcome'], 'blocked')
        approved = c.summarize(data, 'confirm', required(), allow_reference_overload=True)
        self.assertEqual(approved['outcome'], 'retained')
        self.assertFalse(approved['valid'])
        self.assertTrue(approved['decision_valid'])
        self.assertEqual(approved['blocked_reference_cases'], ['write:w64:85'])
        for metric in approved['cases']['write:w64:85']['metrics'].values():
            self.assertFalse(metric['inferential'])
            self.assertFalse(metric['benefit'])
            self.assertFalse(metric['regression'])
        self.assertEqual(len(data), 240)

    def test_candidate_only_delivery_failure_prevents_adoption(self):
        data = rows()
        data[-1].update(failures=['rejected arrivals'], capacity_only_failure=True)
        summary = c.summarize(data, 'confirm', required(), allow_reference_overload=True)
        self.assertEqual(summary['outcome'], 'rejected')
        self.assertTrue(summary['candidate_delivery_failures'])

    def test_timeouts_and_affinity_failures_are_not_reference_overload(self):
        data = rows()
        data[0].update(failures=['timeout'])
        self.assertEqual(c.summarize(data, 'confirm', required(), allow_reference_overload=True)['outcome'], 'blocked')
        result = f.result(100)
        result['measurement']['rejected'] = 5
        record = dict(case='paged-buffered', rate=100, returncode=0,
                      failures=c.roadmap.failures(result, 'paged-buffered', 100))
        self.assertFalse(c.capacity_only_failure(result, record))
        result['measurement'].update(queue_rejected=5, arrival_deadline_rejected=0)
        self.assertTrue(c.capacity_only_failure(result, record))
        result['measurement'].update(queue_rejected=4, arrival_deadline_rejected=1)
        self.assertFalse(c.capacity_only_failure(result, record))
        result['measurement'].update(queue_rejected=5, arrival_deadline_rejected=0)
        result['measurement']['errors'] = 1
        self.assertFalse(c.capacity_only_failure(result, record))
        result['measurement']['errors'] = 0
        record['failures'].append('external process affinity audit missing or failed')
        self.assertFalse(c.capacity_only_failure(result, record))

    def test_delivery_shortfall_without_queue_pressure_is_not_overload(self):
        result = f.result(100)
        result['measurement']['throughput'] = 98
        record = dict(case='paged-buffered', rate=100, returncode=0,
                      failures=c.roadmap.failures(result, 'paged-buffered', 100))
        self.assertFalse(c.capacity_only_failure(result, record))
        result['measurement']['backlog_samples'] = [0] * 15 + [20] * 15
        self.assertTrue(c.capacity_only_failure(result, record))


if __name__ == '__main__':
    unittest.main()
