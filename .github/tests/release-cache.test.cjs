// Copyright (C) 2026 ScyllaDB SPDX-License-Identifier: Apache-2.0
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {join} = require('node:path');
const {spawnSync} = require('node:child_process');
const {test} = require('node:test');
const yaml = require('js-yaml');

const load = name => yaml.load(readFileSync(join(__dirname, '../workflows', name), 'utf8'));
const workflow = load('build.yml');
const checkout = 'actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1';
const node = 'actions/setup-node@249970729cb0ef3589644e2896645e5dc5ba9c38';

function mandatory(job) {
  // Default success() scheduling propagates failed/skipped dependencies.
  assert.equal(job.if, undefined, 'job must not bypass or conditionally skip its gate');
  assert.equal(job['continue-on-error'], undefined, 'job failure must remain blocking');
  for (const step of job.steps ?? []) {
    assert.equal(step.if, undefined, 'required step must not be conditional');
    assert.equal(step['continue-on-error'], undefined, 'required step failure must propagate');
  }
}

function trustedAutomation(current) {
  const job = current.jobs.automation;
  mandatory(job);
  assert.equal(job.uses, undefined);
  assert.equal(job.needs, undefined);
  assert.equal(job.strategy, undefined);
  assert.equal(job.env, undefined);
  assert.equal(job.defaults, undefined);
  assert.equal(job.container, undefined);
  assert.equal(job.services, undefined);
  assert.doesNotMatch(JSON.stringify(job), /inputs\.|needs\.|matrix\./,
    'automation must not consume candidate inputs, outputs or dependency metadata');
  const [source, runtime, tests] = job.steps;
  assert.equal(job.steps.length, 3, 'no candidate artifact, cache or extra execution steps');
  assert.equal(source.uses, checkout);
  assert.deepEqual(source.with, {'persist-credentials': false, ref: '${{ github.sha }}'});
  assert.equal(runtime.uses, node);
  assert.deepEqual(runtime.with, {'node-version': '24'});
  assert.equal(tests.run.trim(),
    'npm ci --ignore-scripts --prefix .github\nnode --test .github/tests/*.test.cjs');
  for (const step of job.steps) {
    assert.deepEqual(Object.keys(step).filter(key => key !== 'name').sort(),
      step === tests ? ['run'] : ['uses', 'with']);
  }
}

function requiredAutomation(current) {
  mandatory(current.jobs.automation);
  mandatory(current.jobs.build);
  assert.deepEqual(current.jobs.build.needs, ['matrix', 'automation']);
}

test('shared Build tests caller workflow tooling without candidate checkout or added caches', () => {
  trustedAutomation(workflow);
  assert.deepEqual(workflow.permissions, {contents: 'read'});
});

test('checkout boundary rejects candidate refs, artifacts, cache and dependency script execution', () => {
  for (const mutate of [
    job => { job.steps[0].with.ref = '${{ inputs.target_commit }}'; },
    job => { job.steps[0].with['persist-credentials'] = true; },
    job => { job.steps[0].with.repository = '${{ inputs.repository }}'; },
    job => { job.env = {CANDIDATE: '${{ inputs.target_commit }}'}; },
    job => { job.defaults = {run: {'working-directory': 'candidate'}}; },
    job => { job.steps.splice(2, 0, {uses: 'actions/download-artifact@candidate'}); },
    job => { job.steps.splice(2, 0, {uses: 'actions/cache@candidate'}); },
    job => { job.steps[1].with.cache = 'npm'; },
    job => { job.steps[2].run = job.steps[2].run.replace(' --ignore-scripts', ''); },
    job => { job.steps[2].run = 'node candidate/helper.cjs'; }
  ]) {
    const current = structuredClone(workflow);
    mutate(current.jobs.automation);
    assert.throws(() => trustedAutomation(current), assert.AssertionError);
  }
});

test('automation failure blocks the Go matrix in every shared Build context', () => {
  requiredAutomation(workflow);
  for (const mutate of [
    current => { current.jobs.build.needs = ['matrix']; },
    current => { current.jobs.build.if = 'always()'; },
    current => { current.jobs.automation.if = "github.event_name != 'workflow_dispatch'"; },
    current => { current.jobs.automation['continue-on-error'] = true; },
    current => { current.jobs.automation.steps[2]['continue-on-error'] = true; },
    current => { current.jobs.automation.steps[2].if = 'false'; }
  ]) {
    const current = structuredClone(workflow);
    mutate(current);
    assert.throws(() => requiredAutomation(current), assert.AssertionError);
  }
});

function candidateBuild(current) {
  const build = current.jobs.build;
  const steps = build.steps;
  assert.equal(steps[0].uses, checkout);
  assert.deepEqual(steps[0].with, {
    'persist-credentials': false, ref: '${{ inputs.target_commit }}', 'fetch-depth': 0
  });
  assert.equal(build['runs-on'], "${{ matrix.arch == 'arm64' && 'ubuntu-24.04-arm' || 'ubuntu-latest' }}");
  assert.deepEqual(build.strategy, {
    matrix: {arch: '${{ fromJSON(needs.matrix.outputs.arch) }}'}, 'fail-fast': false
  });
  assert.equal(steps[1].uses, 'actions/cache@55cc8345863c7cc4c66a329aec7e433d2d1c52a9');
  assert.match(steps[1].with.key, /runner\.arch/);
  assert.equal(steps[2].uses, 'actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e');
  assert.equal(steps[2].with['go-version-file'], 'go.mod');
  assert.deepEqual(steps.filter(step => step.run).map(step => step.run), [
    'make check', 'make test-unit', 'sudo sh -c "echo 2097152 >> /proc/sys/fs/aio-max-nr"'
  ]);
}

test('candidate Go lint and unit tests retain full checkout, caches and architecture matrix', () => {
  candidateBuild(workflow);
  for (const mutate of [
    job => { job.steps[0].with.ref = '${{ github.sha }}'; },
    job => { job.steps[0].with['fetch-depth'] = 1; },
    job => { job.strategy.matrix.arch = 'amd64'; }
  ]) {
    const current = structuredClone(workflow);
    mutate(current.jobs.build);
    assert.throws(() => candidateBuild(current), assert.AssertionError);
  }
});

test('all callers retain candidate and architecture inputs, with blocking downstream gates', () => {
  for (const [name, arch] of [['pr.yml', 'amd64'], ['main.yml', 'amd64'],
    ['merge-queue.yml', 'amd64,arm64'], ['release.yml', 'amd64,arm64']]) {
    const caller = load(name);
    const build = caller.jobs.build;
    mandatory(build);
    assert.equal(build.uses, './.github/workflows/build.yml');
    const target = name === 'release.yml' ? '${{ needs.preflight.outputs.resolved_sha }}' : '${{ github.sha }}';
    assert.deepEqual(build.with, {target_commit: target, arch});
    if (caller.jobs.integration) {
      assert.ok(caller.jobs.integration.needs.includes('build'));
      assert.equal(caller.jobs.integration.with.target_commit, target);
    }
    if (caller.jobs.gate) {
      const gate = caller.jobs.gate;
      assert.ok(gate.needs.includes('build'));
      assert.equal(gate.if, 'always()');
      const step = gate.steps[0];
      assert.equal(step.env.BUILD_RESULT, '${{ needs.build.result }}');
      // Execute the actual gate shell: a skipped Build after automation failure
      // must fail the required caller gate even if integration reports success.
      for (const result of ['success', 'failure', 'skipped', 'cancelled']) {
        const run = spawnSync('bash', ['-e', '-c', step.run], {
          env: {...process.env, BUILD_RESULT: result, INTEGRATION_RESULT: 'success'}
        });
        assert.ifError(run.error);
        assert.equal(run.status === 0, result === 'success', `${name}: ${result}`);
      }
    }
    if (name === 'release.yml') {
      assert.equal(build.needs, 'preflight');
      for (const job of ['publication-gate', 'publish']) {
        assert.ok(caller.jobs[job].needs.includes('build'));
      }
      assert.equal(caller.jobs['publication-gate'].if, undefined);
    }
  }
});
