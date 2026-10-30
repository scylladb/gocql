#!/usr/bin/env python3
"""Build the step09 per-identity ledger from pinned source and real Go JSON.

A skipped test, build, failed package or old baseline is never passing evidence.
Source inventories preserve assertion expressions, conditions in complete bodies,
and shared helper/method implementations; equivalence is a scenario review, not
an assertion-count comparison.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]
EVIDENCE = ROOT / 'feature-specs/evidence'


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write(name, value):
    (EVIDENCE / name).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def events(path):
    result = []
    for line in Path(path).read_text().splitlines():
        try:
            item = json.loads(line)
        except ValueError:
            continue
        if isinstance(item, dict) and 'Action' in item:
            result.append(item)
    return result


def fixture(name, file):
    common = ['Three-node pinned native fixture, RF=3 and explicit per-run namespace; protocol/distribution gates below remain applicable.']
    if name in {'TestSessionAPI', 'TestConsistencyNames', 'TestIsUseStatement', 'TestShouldPrepareFunction', 'TestQueryShouldPrepare', 'TestRetryOnTopologyBusy', 'TestVector_SubTypeParsing'}:
        return ['Local API/codec/policy assertions; integration package startup still performs real fixture probes.']
    if name == 'TestCCM':
        return ['Live owned CCM cluster; finite CLI forwards status/stop/join/start/health to the same runner holding exact address locks.']
    if name.startswith('TestSelectWithoutFrom'):
        common += ['Development Scylla SELECT-without-FROM capability; actual prepared/unprepared and rejection cases as declared in source.']
    if name.startswith('TestVector') or name == 'TestSliceMapMapScanVectorTypes':
        common += ['Server support for the exact vector subtype/dimension; scalar and variable-size subtype gates retained in source.']
    if name in {'TestCustomPayloadMessages', 'TestCustomPayloadValues', 'TestWriteFailure'}:
        common += ['Cassandra Java FixtureQueryHandler: ordinary real QUERY/EXECUTE/BATCH processing, native echo/failure serialization; v5 for reason maps.']
    if name == 'TestInvalidPeerEntry':
        common += ['Cassandra plaintext uncompressed v4 and independent incomplete-peer transport fixture.']
    if name == 'TestAuthentication':
        common += ['Password authentication enforced by the real Scylla fixture, including rejected credentials.']
    if name == 'TestUnixSockets':
        common += ['Actual Scylla maintenance Unix-domain socket of the owned node.']
    if name in {'TestCloudConnection', 'TestTLSTicketResumption'}:
        common += ['TLS lane, trusted dedicated PKI/client certificate, real proxy/SNI or fresh ticket-resuming handshakes.']
    if name.startswith('TestShardAwarePort') or name in {'TestDriverConfigReporting', 'TestTablets'}:
        common += ['Two Scylla shards; actual native 9042/9142 and plaintext/TLS shard ports19042/19142, strict source-port targeting and wrong-shard feedback.']
    if name == 'TestRecreateSchema':
        common += ['Scylla Lua or Cassandra Java UDF/UDA, materialized views and original schema goldens/dump replay.']
    if any(k in name for k in ['NowInSeconds', 'SetKeyspace', 'OverriddenKeyspace', 'LargeSizeQuery']):
        common += ['Native protocol5 where the source gate requires protocol5 options/segmentation.']
    if name == 'TestQueryCompressionNotWorthIt':
        common += ['Cassandra v5 with actually negotiated LZ4 segments, including the uncompressed-payload escape within compressed segmentation.']
    if name == 'TestCompressorNegotiated':
        common += ['An explicitly requested compressor must be present on every expected host.']
    if name == 'TestPrepareExecuteMetadataChangedFlag':
        common += ['Cassandra native v5 and Scylla v4 negotiated metadata-ID extension, each exercised separately.']
    if name == 'TestPrepareExecuteScyllaEmptyMetadataID':
        common += ['Scylla v4 negotiated metadata-ID extension and controlled empty-ID cache input.']
    return common


def semantics(item):
    name = item['test']
    original = item['original']['source']
    changes = []
    if any(s in original for s in ['.Query(', '.Bind(', '.Batch(', '.NewBatch(', '.WithTimestamp(', '.SetKeyspace(', '.SetHostID(']):
        changes += ['Mutable query/batch setters and reflection binding become immutable Statement/Prepared/Bound/BatchItem plus captured Options; encoded values are explicit and reused.']
    if any(s in original for s in ['.Scan(', '.ScanCAS(', '.MapScan(', '.SliceMap(', '.MapScanCAS(']):
        changes += ['Scan/reflection destinations become typed row fields or explicit Value accessors; null/empty/unset are explicit states. Owned collection/Clone paths preserve retained results.']
    if 'Tuple' in name:
        changes += ['One physical tuple remains one typed tuple value; named/indexed member assertions replace destination flattening.']
    if 'UDT' in name or 'Marshaler' in name or name.startswith('TestWiki') or name == 'TestSerializationSimpleTypesCassandra':
        changes += ['Explicit custom/field codecs replace reflection and Marshal/Unmarshal callbacks; fixture values and schema/type/byte assertions remain.']
    if name == 'TestMapScanWithRefMap':
        changes += ['Preseeded map pointer replacement becomes explicit typed field access, including both custom FullName members and Nullable[int32].']
    if name in {'TestSessionAPI', 'TestQueryBasicAPI', 'TestBatchBasicAPI'}:
        changes += ['Session setters become atomic profile replacement; fractional prefetch becomes a bounded count0/1; metrics/traces belong to execution Results/Observers; invalid/closed operations return typed errors.']
    if 'Stats' in name or 'Observe' in name:
        changes += ['Immutable execution Result and observer events replace mutable request-owned metrics while retaining attempts, host, timing, rows/pages and batch entry assertions.']
    if name == 'TestQuery_SetHostID':
        changes += ['Malformed UUID text is rejected at construction; present-empty UUID yields ValidationError; nonempty unknown and down targets still return ErrNoConnections without fallback.']
    if name == 'TestRoutingKey':
        changes += ['Canonical encoded partition-key spans replace GetRoutingKey; test retains exact bytes, parameter indices/types, cache identity/size and token assertions.']
    if name == 'TestPrepare_PreparedCacheEviction':
        changes += ['Typed statement cache is keyed by CQL/keyspace with per-host IDs inside entries; capacity and exact retained/evicted statement membership remain checked.']
    if name == 'TestNoHangAllHostsDown':
        changes += ['Immutable routing snapshot fixture rebuilds derived indices; actual picker HostInfo down states retain original fault injection for all nine policies and one-host membership.']
    if name == 'TestRecreateSchema':
        changes += ['Cassandra uses equivalent Java aggregate; only exact server-declared aborted group0 concurrent-modification transactions receive a bounded fixture retry. All original golden/dump/replay assertions remain.']
    return changes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt', action='append', type=Path, required=True)
    args = parser.parse_args()
    source_path = EVIDENCE / 'step09-assertion-source.json'
    audit = json.loads(source_path.read_text())
    baseline = json.loads((ROOT / 'feature-specs/integration-baseline.json').read_text())
    translations = json.loads((ROOT / 'feature-specs/execution/09-assertion-translations.json').read_text())
    identities = {f['file'] + '::' + t for f in baseline['files'] for t in f['tests']}
    assert identities == {t['identity'] for t in audit['tests']} and len(identities) == 174
    for f in baseline['files']:
        old = subprocess.check_output(['git', 'show', baseline['revision'] + ':' + f['file']], cwd=ROOT)
        assert hashlib.sha256(old).hexdigest() == f['sha256'], f['file']
    commands, outcomes, receipts = {}, {}, []
    snapshot = None
    for path in args.receipt:
        receipt = json.loads(path.read_text())
        assert receipt['status'] == 'passed', path
        for lane in receipt['lanes']:
            assert all(lane['joined'].values()) and lane['endpoints_refused']
        native_source = json.loads(Path(receipt['source_snapshot']).read_text())
        assert all(sha(ROOT / name) == digest for name, digest in native_source.items()), 'native source changed'
        if snapshot is not None:
            assert native_source == snapshot, 'native sources differ'
        snapshot = native_source
        retained = 'step09-' + path.name
        (EVIDENCE / retained).write_bytes(path.read_bytes())
        receipts.append({'receipt': str(path), 'retained_receipt': 'feature-specs/evidence/' + retained,
                         'sha256': sha(path), 'runner_pid': receipt['pid'], 'status': receipt['status']})
        for command in receipt['commands']:
            if command['command'][:3] != ['go', 'test', '-json'] or 'cleanup' in command['name']:
                continue
            assert command['exit_code'] == 0
            records = events(command['log'])
            assert any(e['Action'] == 'pass' and 'Test' not in e for e in records), command['name']
            assert not any(e['Action'] == 'fail' for e in records), command['name']
            key = path.stem + '/' + command['name']
            commands[key] = {**command, 'source_sha256': receipt['source_sha256'], 'receipt': retained}
            selected = []
            for event in records:
                if event['Action'] not in {'pass', 'skip'} or 'Test' not in event or '/' in event['Test']:
                    continue
                name = event['Test']
                messages = [e.get('Output', '') for e in records if e.get('Test') == name and e['Action'] == 'output'
                            and 'logger.go' not in e.get('Output', '') and not e.get('Output', '').startswith(('===', '---'))]
                children = [e for e in records if e['Action'] in {'pass','skip'} and e.get('Test','').startswith(name + '/')]
                leaves = [e for e in children if not any(other.get('Test','').startswith(e['Test'] + '/') for other in children)]
                effective = event['Action'] == 'pass' and (not children or any(e['Action'] == 'pass' for e in leaves))
                value = {'command_id': key, 'outcome': event['Action'], 'seconds': event.get('Elapsed'), 'assertion_output': ''.join(messages),
                         'effective_behavior_pass': effective, 'subtests': [{'test':e['Test'],'outcome':e['Action'],'seconds':e.get('Elapsed')} for e in children]}
                outcomes.setdefault(name, []).append(value)
                selected.append({'test': name, **value})
            write('step09-' + command['name'] + '-outcomes.json', selected)
    write('step09-final-source.json', snapshot)
    mapping = []
    for item in audit['tests']:
        name = item['test']
        assert name in translations
        result = outcomes.get(name, [])
        assert any(o['effective_behavior_pass'] for o in result), 'no real successful lane for ' + name
        helper_skips = []
        for key in item['translated_helper_ids'] or []:
            fn = audit['helpers']['translated'][key]
            if fn['skips']:
                helper_skips.append({'helper': key, 'gates': fn['skips']})
        mapping.append({'identity': item['identity'], 'original_file': item['file'], 'original_line': item['original']['line'],
                        'original_body_sha256': item['original']['sha256'], 'translated_file': item['file'],
                        'translated_line': item['translated']['line'], 'translated_body_sha256': item['translated']['sha256'],
                        'translated_assertions': translations[name], 'api_semantic_changes': semantics(item),
                        'fixture_requirements': fixture(name, item['file']), 'direct_skip_gates': item['translated']['skips'],
                        'helper_skip_gates': helper_skips, 'assertion_inventory': {'file': 'step09-assertion-source.json',
                        'test_identity': item['identity'], 'original_helper_ids': item['original_helper_ids'],
                        'translated_helper_ids': item['translated_helper_ids']}, 'outcomes': result,
                        'real_pass_in_successful_lane': True})
    write('step09-test-mapping.json', {'baseline_revision': baseline['revision'], 'baseline_manifest_sha256': sha(ROOT / 'feature-specs/integration-baseline.json'),
          'count': len(mapping), 'assertion_source_sha256': sha(source_path), 'assertion_inventory_basis': 'Complete pinned test bodies, assertion expressions, transitive free-function helpers and supporting receiver methods. Scenario translations preserve behavior; assertion counts alone do not prove equivalence.',
          'commands': commands, 'tests': mapping, 'supporting_files': [f for f in baseline['files'] if not f['tests']],
          'receipts': receipts, 'source_snapshot': 'step09-final-source.json', 'source_sha256': sha(EVIDENCE / 'step09-final-source.json')})
    print(json.dumps({'mapped': len(mapping), 'real_pass': len(mapping), 'native_commands': len(commands), 'receipts': len(receipts)}))


if __name__ == '__main__':
    main()
