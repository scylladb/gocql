#!/usr/bin/env python3
"""Retain exact extraction provenance, dependency audit and native outcomes.

Does not provision, rerun, or reinterpret failed/skipped commands as passes.
"""
import hashlib
import json
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[2]
EVIDENCE = ROOT / 'feature-specs/evidence'
RUN = Path('/extra/scylladb/gocql-api-test-cluster/step11')
BASE = 'github.com/scylladb/gocql/v2'
PREDECESSOR = '61ae08b0184f54b49274bb11ef14d5129fb62b82'

def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def write(name, value):
    (EVIDENCE / name).write_text(json.dumps(value, sort_keys=True, indent=2) + '\n')

def events(path):
    out = []
    for line in Path(path).read_text().splitlines():
        try:
            item = json.loads(line)
        except ValueError:
            continue
        if isinstance(item, dict) and 'Action' in item:
            out.append(item)
    return out

def source():
    frozen = json.loads((EVIDENCE / 'step11-source.json').read_text())
    final = json.loads((EVIDENCE / 'step11-final-source.json').read_text())
    proof = json.loads((EVIDENCE / 'step11-method-rename-proof.json').read_text())
    approved = {x['file']: x for x in proof['files']}
    formatting = {x['file']: x for x in json.loads((EVIDENCE / 'step11-format-proof.json').read_text())['files']}
    doc = json.loads((EVIDENCE / 'step11-doc-preservation.json').read_text())
    assert set(final) - set(frozen) == {doc['file']} and set(frozen) <= set(final)
    assert doc['declarations'] == doc['imports'] == doc['compiler_or_build_directives'] == 0
    assert doc['identical_to_accepted_predecessor'] and final[doc['file']] == doc['sha256']
    assert (ROOT / doc['file']).read_text() == doc['source']
    changed = []
    for path, digest in final.items():
        assert sha(ROOT / path) == digest, path
        if path not in frozen:
            continue
        if digest != frozen[path]:
            if path in approved:
                p = approved[path]
                assert p['old_sha256'] == frozen[path] and p['new_sha256'] == digest
                value = p['old_source'].encode()
                for edit in sorted(p['edits'], key=lambda x: -x['offset']):
                    pos = edit['offset']
                    assert value[pos:pos + len(edit['old'])] == edit['old'].encode()
                    value = value[:pos] + edit['new'].encode() + value[pos + len(edit['old']):]
                assert value.decode() == p['new_source'] == (ROOT / path).read_text()
            else:
                p = formatting[path]
                assert p['old_sha256'] == frozen[path] and p['new_sha256'] == digest and p['tokens_equal']
                assert subprocess.check_output(['gofmt'], input=p['old_source'].encode()).decode() == p['new_source'] == (ROOT / path).read_text()
            changed.append(path)
    assert set(changed) == approved.keys() | formatting.keys()
    return {'native_source_count': len(frozen), 'native_source_sha256': sha(EVIDENCE / 'step11-source.json'),
            'final_source_count': len(final), 'final_source_sha256': sha(EVIDENCE / 'step11-final-source.json'),
            'only_final_delta': 'Original declaration-free package documentation restored, plus three Framer method declarations and bound selectors, plus two token-identical gofmt changes. Exact before/after source and edits are retained; signatures, statement shapes, literals and imports stay unchanged.',
            'changed_files': changed}

def dependency_audit():
    output = subprocess.check_output(['go', 'list', '-json', '-tags', 'unit', './...'], cwd=ROOT, text=True)
    decoder = json.JSONDecoder()
    graph = []
    while output.strip():
        package, end = decoder.raw_decode(output.lstrip())
        output = output.lstrip()[end:]
        path = package['ImportPath']
        if path == BASE or path.startswith(BASE + '/internal/'):
            imports = [p for p in package.get('Imports', []) if p.startswith(BASE)]
            graph.append({'package': path, 'imports': imports, 'sources': package.get('GoFiles', [])})
            if path.startswith(BASE + '/internal/'):
                assert BASE not in imports, path
            if path == BASE + '/internal/codec':
                assert set(imports) <= {BASE + '/internal/cqltype', BASE + '/internal/wire'}
            if path == BASE + '/internal/model':
                assert set(imports) <= {BASE + '/internal/cqltype', BASE + '/internal/wire'}
            if path == BASE + '/internal/protocol':
                assert not re.search(r'\b(?:Conn|Session)\b', '\n'.join(re.sub(r'//[^\n]*|/\*.*?\*/', '', (ROOT / 'internal/protocol' / file).read_text(), flags=re.S) for file in package['GoFiles']))
    for path in (ROOT / 'internal/model').glob('*.go'):
        if not path.name.endswith('_test.go'):
            assert not re.search(r'\btype\s+\w+\s*\[', path.read_text()), path
    for path in (ROOT / 'internal/codec').glob('stream_*.go'):
        if not path.name.endswith('_test.go'):
            body = re.sub(r'//[^\n]*|/\*.*?\*/', '', path.read_text(), flags=re.S)
            assert not re.search(r'\b(?:ValueReader|readValue|newValueReader|DecodeValue|OwnedBytes)\b', body), path
    for relative, typename in [('internal/cqltype/type.go', 'DataType'), ('internal/cqltype/type.go', 'Columns'), ('internal/cqltype/view.go', 'Bytes'), ('internal/cqltype/view.go', 'Text'), ('internal/cqltype/numeric.go', 'Varint'), ('internal/model/result.go', 'PageState')]:
        body = re.search(r'\btype\s+' + typename + r'\s+struct\s*\{([^}]+)\}', (ROOT / relative).read_text(), flags=re.S).group(1)
        fields = re.findall(r'(?:^|[;\n])\s*([A-Za-z_]\w*)', body)
        assert fields and all(not field[0].isupper() for field in fields), (relative, typename, fields)
    return {'findings': [], 'graph': graph,
            'rules': ['No internal package imports public driver.', 'codec imports only cqltype and wire within this module.',
                      'model has nongeneric declarations and only cqltype/wire dependencies.', 'protocol has no Conn/Session identifiers in production code.',
                      'Native streaming plans never call buffered ValueReader/DecodeValue/OwnedBytes adapters.'],
            'opaque_public_types': ['cqltype.DataType', 'cqltype.Columns', 'cqltype.Bytes', 'cqltype.Text', 'cqltype.Varint', 'model.PageState'],
            'retained_root_responsibilities': ['Conn and framerPool admission/lifecycle/cache/write orchestration', 'prepared metadata/schema revision fences, routing, metadata and execution (step12)',
                                             'public observer/consistency facade', 'independent test-only request oracles and runtime fault-injection fixtures']}

def tests():
    names = re.compile(r'^func ((?:Test|Fuzz|Benchmark|Example)\w*)\(', re.M)
    original = []
    for path in subprocess.check_output(['git', 'ls-tree', '--name-only', PREDECESSOR], cwd=ROOT, text=True).splitlines():
        if path.endswith('_test.go'):
            body = subprocess.check_output(['git', 'show', PREDECESSOR + ':' + path], cwd=ROOT, text=True)
            original.extend((path, name) for name in names.findall(body))
    current = {}
    paths = list(ROOT.glob('*_test.go'))
    for package in ['cqltype', 'codec', 'model', 'protocol']:
        paths += list((ROOT / 'internal' / package).glob('*_test.go'))
    for path in paths:
        for name in names.findall(path.read_text()):
            current.setdefault(name, []).append(str(path.relative_to(ROOT)))
    mapping = []
    for path, name in original:
        candidates = current.get(name, [])
        assert len(candidates) == 1, (path, name, candidates)
        mapping.append({'previous_file': path, 'declaration': name, 'current_file': candidates[0]})
    return {'previous_root_named_test_example_benchmark_declarations': len(original),
            'all_have_one_current_declaration': True,
            'scope': 'Declaration identity inventory, not a numerical assertion-equivalence claim. Baseline bodies/helpers and explicit semantic-source proof are retained separately.',
            'mapping': mapping}

def native():
    outcomes = []
    for name in ['stable', 'cassandra']:
        receipt = json.loads((RUN / (name + '.json')).read_text())
        assert receipt['status'] == 'passed'
        assert receipt['source_sha256'] == sha(EVIDENCE / 'step11-source.json')
        for lane in receipt['lanes']:
            assert all(lane['joined'].values()) and lane['endpoints_refused']
        write('step11-native-' + name + '.json', receipt)
        for command in receipt['commands']:
            if command['exit_code'] != 0:
                raise AssertionError(command)
            ev = events(command['log'])
            packages = [e for e in ev if 'Test' not in e and e['Action'] in ('pass', 'fail')]
            if not packages or '-cleanup' in command['name']:
                continue
            assert all(e['Action'] == 'pass' for e in packages)
            terminal = {e['Test']: e['Action'] for e in ev if 'Test' in e and e['Action'] in ('pass', 'skip', 'fail')}
            rows = []
            for test, status in terminal.items():
                descendants = {key: value for key, value in terminal.items() if key.startswith(test + '/')}
                effective = status == 'pass' and (not descendants or 'pass' in descendants.values())
                rows.append({'test': test, 'status': status, 'effective_behavior_pass': effective})
            tops = [r for r in rows if '/' not in r['test']]
            outcomes.append({'command': command['name'], 'source_sha256': receipt['source_sha256'], 'log': command['log'],
                             'log_sha256': command['sha256'], 'command_args': command['command'], 'exit_code': 0,
                             'top_level_pass': sum(r['status'] == 'pass' for r in tops), 'top_level_skip': sum(r['status'] == 'skip' for r in tops), 'tests': rows})
    assert len(outcomes) == 4
    write('step11-native-outcomes.json', {'successful_packages': outcomes})
    previous = json.loads((EVIDENCE / 'step10-test-mapping.json').read_text())
    sources = {t['identity']: t for t in json.loads((EVIDENCE / 'step11-assertion-source.json').read_text())['tests']}
    rows = []
    for item in previous['tests']:
        records = []
        for package in outcomes:
            for result in package['tests']:
                if result['test'] == item['test']:
                    records.append({**result, 'command': package['command'], 'log_sha256': package['log_sha256']})
        rows.append({'identity': item['identity'], 'file': item['file'], 'test': item['test'],
                     'original_sha256': sources[item['identity']]['original']['sha256'], 'translated_sha256': sources[item['identity']]['translated']['sha256'],
                     'current_behavioral_pass': any(r['effective_behavior_pass'] for r in records), 'current_native_outcomes': records,
                     'predecessor_mapping': 'step10-test-mapping.json#' + item['identity'], 'historical_mapping': item['historical_mapping']})
    write('step11-test-mapping.json', {'count': len(rows), 'current_behavioral_pass_count': sum(r['current_behavioral_pass'] for r in rows),
                                     'historical_all174_passed_at_step09': True,
                                     'scope': 'Focused foundation lanes; prior158 step10 and all174 step09 evidence retain their own source bindings. Full final-tree all174 matrix remains14/17. Skips never count as passes.', 'tests': rows})
    return [{'command': p['command'], 'pass': p['top_level_pass'], 'skip': p['top_level_skip']} for p in outcomes]

if __name__ == '__main__':
    provenance = source()
    write('step11-dependency-audit.json', dependency_audit())
    write('step11-test-disposition.json', tests())
    print(json.dumps({'source': provenance, 'native': native()}, indent=2))
