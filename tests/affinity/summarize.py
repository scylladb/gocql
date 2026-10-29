#!/usr/bin/env python3
# Copyright 2026 The Apache Software Foundation.
# Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE
# in the repository root, or https://www.apache.org/licenses/LICENSE-2.0.

"""Summarize randomized blocks without treating samples as independent runs."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import random
import statistics

from matrix import validate_matrix

def median(values):
    return statistics.median(values)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', type=Path)
    parser.add_argument('--allow-incomplete', action='store_true', help='explicit progress report; never marks a partial matrix complete')
    args = parser.parse_args()
    rows = [json.loads(line) for line in (args.directory / 'runs.jsonl').read_text().splitlines()]
    environment = json.loads((args.directory / 'environment.json').read_text())
    try:
        groups, missing = validate_matrix(rows, environment)
    except (ValueError, KeyError, TypeError) as error:
        parser.error(str(error))
    if missing and not args.allow_incomplete:
        parser.error(f'incomplete matrix: {len(missing)} case/repetition identities missing')
    if not groups:
        parser.error('no successful results')
    latency_valid = environment.get('latency_sampling') == 'stratified-random-reservoir-v1'

    def has_latency(runs):
        return latency_valid and all(
            r['result']['BatchP99NS'] > 0 and all(w.get('LatencySamplesRetained', 0) > 0
                                                for w in r['result']['Workers']) for r in runs)
    summaries = []
    for label, runs in sorted(groups.items()):
        rates = [r['result']['OperationsPerSecond'] for r in runs]
        summaries.append({
            'label': label, 'runs': len(runs), 'mops_median': median(rates)/1e6,
            'mops_min': min(rates)/1e6, 'mops_max': max(rates)/1e6,
            'worker_completion_cv_median': median([r['result']['WorkerOperationsCV'] for r in runs]),
            'client_cores_median': median([r['result']['ProcessCPUSeconds']/r['result']['Seconds'] for r in runs]),
            'client_cpu_seconds_per_million_ops_median': median([r['result']['CPUSecondsPerMillionOps'] for r in runs]),
            'batch_p99_ms_median': median([r['result']['BatchP99NS']/1e6 for r in runs]) if has_latency(runs) else None,
            'allocations_per_million_ops_median': median([r['result']['Allocations']/r['result']['Operations']*1e6 for r in runs]),
            'server_cores_including_setup_warmup_median': median([r['server_cpu_seconds_including_client_setup_and_warmup']/r['wall_seconds'] for r in runs]),
        })
    comparisons = []
    for label in sorted(groups):
        if label.endswith('C-sharded'):
            prefix = label[:-len('C-sharded')]
            pairs = [('D-sharded-pinned','C-sharded','pinning after sharding')]
            if prefix+'A-shared' in groups:
                pairs += [('C-sharded','A-shared','sharding plus removing shared lock'),
                          ('B-shared-pinned','A-shared','pinning shared cache')]
            # A one-time socket option is not a maintained receive-placement
            # treatment. Do not emit a socket-affinity throughput comparison.
            for numerator, denominator, meaning in pairs:
                a = {r['repetition']: r['result'] for r in groups.get(prefix+numerator,[])}
                b = {r['repetition']: r['result'] for r in groups.get(prefix+denominator,[])}
                common = sorted(a.keys() & b.keys())
                if not common:
                    continue
                ratios = [a[k]['OperationsPerSecond']/b[k]['OperationsPerSecond'] for k in common]
                rng = random.Random(20261029)
                bootstrap = sorted(median(rng.choices(ratios,k=len(ratios))) for _ in range(10000))
                comparisons.append({
                    'workload': prefix.rstrip('-'), 'comparison': numerator+'/'+denominator,
                    'meaning': meaning, 'paired_runs': len(common),
                    'paired_median_gain_percent': (median(ratios)-1)*100,
                    'bootstrap_median_95_percent_interval': [(bootstrap[249]-1)*100,(bootstrap[9749]-1)*100],
                    'paired_gain_percent': [(ratio-1)*100 for ratio in ratios],
                    'paired_median_batch_p99_ratio': median([a[k]['BatchP99NS']/b[k]['BatchP99NS'] for k in common])
                        if has_latency(groups[prefix+numerator] + groups[prefix+denominator]) else None,
                    'paired_median_worker_cv_change': median([a[k]['WorkerOperationsCV']-b[k]['WorkerOperationsCV'] for k in common]),
                })
    pinned_samples = sum(sum(w['CPUObservations'].values()) for r in rows if r['result']['Config']['Pin'] for w in r['result']['Workers'])
    unpinned_cpu_changes = sum(w['SampledCPUChanges'] for r in rows if not r['result']['Config']['Pin'] for w in r['result']['Workers'])
    socket_workers = [w for r in rows if r['result']['Config']['SocketCPU'] for w in r['result']['Workers']]
    outside_target = sum(w['IncomingAfter'] != w['TargetCPU'] for w in socket_workers)
    before_target_missing = sum(w['IncomingBefore'] != w['TargetCPU'] for w in socket_workers)
    output = {
        'raw_result_sha256': hashlib.sha256((args.directory/'runs.jsonl').read_bytes()).hexdigest(),
        'total_runs': len(rows), 'total_operations': sum(r['result']['Operations'] for r in rows),
        'complete': not missing,
        'missing_case_repetitions': missing,
        'latency_valid': latency_valid,
        'summaries': summaries, 'comparisons': comparisons,
        'pinned_cpu_samples': pinned_samples, 'unpinned_sampled_cpu_changes': unpinned_cpu_changes,
        'socket_workers': len(socket_workers), 'socket_workers_incoming_cpu_changed_from_target': outside_target,
        'socket_workers_target_missing_at_measurement_start': before_target_missing,
        'socket_affinity_status': 'unsupported-observed-mismatch' if before_target_missing or outside_target or
            any(w.get('SocketOtherSamples', 0) for w in socket_workers) else
            ('unverified-no-mismatch-observed' if socket_workers else 'not-tested'),
        'socket_observed_cpu_values': sorted(set(w['IncomingAfter'] for w in socket_workers)),
        'socket_observed_napi_values': sorted(set(w['NAPIID'] for w in socket_workers)),
        'notes': [
            'Synthetic cache/LRU/encoding/loopback experiment; no actual gocql, database, TLS or physical NIC.',
            'LRU ownership/lock removal is distinct from worker pinning; compare D/C for worker pinning.',
            'E is a one-time socket-option feasibility probe, not a maintained socket-affinity throughput treatment.',
            'Latency describes sampled batches of 32 operations, not per-query p99. Skew includes bounded queue time.',
            f'Bootstrap intervals resample up to {environment["arguments"]["repeats"]} randomized blocks; exploratory shared-host evidence.',
            'No hardware cache-miss or full migration counters are collected by this harness.',
        ],
    }
    if not latency_valid:
        output['notes'].append('Legacy phase-locked latency samples are invalid: p99 and latency ratios are withheld; throughput counts are retained.')
    (args.directory/'summary.json').write_text(json.dumps(output,indent=2)+'\n')
    with open(args.directory/'summary.csv','w') as file:
        writer=csv.DictWriter(file,fieldnames=list(summaries[0]))
        writer.writeheader()
        writer.writerows(summaries)
    lines = ['LOCAL AFFINITY EXPERIMENT',f'Runs: {len(rows)}; complete matrix: {output["complete"]}',
             f'Total measured operations: {output["total_operations"]:,}',
             f'Throughput: median across repetitions; randomized latency sampling: {latency_valid}.', '',
             'Case | Mops/s median [min,max] | worker completion CV | batch p99 ms | client CPU cores']
    for s in summaries:
        latency = f'{s["batch_p99_ms_median"]:.3f}' if s['batch_p99_ms_median'] is not None else 'WITHHELD'
        lines.append(f'{s["label"]} | {s["mops_median"]:.3f} [{s["mops_min"]:.3f},{s["mops_max"]:.3f}] | '
                     f'{s["worker_completion_cv_median"]:.3f} | {latency} | {s["client_cores_median"]:.2f}')
    lines += ['', 'PAIRED COMPARISONS: throughput gain; exploratory 95% bootstrap interval']
    for c in comparisons:
        low,high=c['bootstrap_median_95_percent_interval']
        latency = f'{c["paired_median_batch_p99_ratio"]:.2f}' if c['paired_median_batch_p99_ratio'] is not None else 'WITHHELD'
        lines.append(f'{c["workload"]}: {c["comparison"]}: {c["paired_median_gain_percent"]:+.1f}% '
                     f'[{low:+.1f}%,{high:+.1f}%]; batch p99 ratio {latency}; '
                     f'worker CV change {c["paired_median_worker_cv_change"]:+.3f}')
    lines += ['',f'Pinned CPU samples: {pinned_samples:,}; unpinned sampled CPU changes: {unpinned_cpu_changes:,}.',
              f'Socket workers reporting incoming CPU different from target: {outside_target}/{len(socket_workers)}.',
              f'Socket target absent before measurement: {before_target_missing}/{len(socket_workers)}; status: {output["socket_affinity_status"]}.',
              f'Observed incoming CPUs: {output["socket_observed_cpu_values"]}; NAPI IDs: {output["socket_observed_napi_values"]}.',
              '', *output['notes']]
    (args.directory/'report.txt').write_text('\n'.join(lines)+'\n')
    print('\n'.join(lines))


if __name__ == '__main__':
    main()
