import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { unzipSync, strFromU8 } from 'fflate';
import { checkedBytes, evidenceBundle, portableReport } from '../src/evidence.ts';

const figureId = '00000000-0000-4000-8000-000000000001';
const reportId = '00000000-0000-4000-8000-000000000002';
const hash = (bytes) => createHash('sha256').update(bytes).digest('hex');
const figure = new TextEncoder().encode('<svg xmlns="http://www.w3.org/2000/svg"></svg>');
const report = new TextEncoder().encode(
  `<html><img src="/api/artifacts/${figureId}"><a href='/api/artifacts/${figureId}'>Evidence</a></html>`,
);
const artifacts = [
  { id: figureId, name: 'predictions.svg', size: figure.length, sha256: hash(figure) },
  { id: reportId, name: 'report.html', size: report.length, sha256: hash(report) },
];
const snapshot = {
  parameters_hash: 'params',
  data_hash: 'data',
  test_hash: 'test',
  snapshot_hash: 'code',
  parameters: { n_test: 128 },
};
const experiment = {
  id: 'test-experiment',
  recipe_id: 'toy_compare_v1',
  phase: 'complete',
  revision: 1,
  evidence_revision: 1,
  artifacts,
  notes: [{ text: 'Private note', author: 'researcher' }],
  snapshot,
};
test('portable bundle preserves original evidence and gives offline report relative links', async () => {
  const zip = await evidenceBundle(experiment, async (a) => (a.id === figureId ? figure : report));
  const files = unzipSync(zip);
  assert.deepEqual(files['evidence/report.html'], report);
  assert.deepEqual(files['evidence/predictions.svg'], figure);
  const portable = strFromU8(files['report.html']);
  assert.match(portable, /src="evidence\/predictions.svg"/);
  assert.match(portable, /href='evidence\/predictions.svg'/);
  assert.doesNotMatch(portable, /\/api\/artifacts/);
  const checks = JSON.parse(strFromU8(files['checksums.json']));
  assert.equal(
    checks.artifacts.find((a) => a.path === 'evidence/report.html').sha256,
    hash(report),
  );
  assert.equal(checks.portable_report_sha256, hash(files['report.html']));
  assert.equal(JSON.parse(strFromU8(files['experiment.json'])).notes[0].text, 'Private note');
});

test('export rejects tampering, unpublished results, unsafe names and missing citations', async () => {
  await assert.rejects(
    () => checkedBytes(artifacts[0], new Uint8Array(figure.length)),
    /Hash mismatch/,
  );
  await assert.rejects(
    () => evidenceBundle({ ...experiment, phase: 'publishing' }),
    /after publication/,
  );
  await assert.rejects(
    () => evidenceBundle({ ...experiment, artifacts: [{ ...artifacts[0], name: '../secret' }] }),
    /filename/,
  );
  assert.throws(
    () => portableReport(`<img src='/api/artifacts/${reportId}'>`, [artifacts[0]]),
    /missing evidence/,
  );
});

test('export commands follow the prepared method and stage order', async () => {
  const exp = {
    ...experiment,
    recipe_id: 'agent_experiment_v1',
    snapshot: {
      plan: { protocol: 'Requested method', stages: [{ id: 'z_fit' }, { id: 'a_evaluate' }] },
    },
  };
  const zip = await evidenceBundle(exp, async (a) => (a.id === figureId ? figure : report));
  const readme = strFromU8(unzipSync(zip)['README.txt']);
  assert.match(readme, /pip install -r evidence\/requirements.txt/);
  assert.ok(readme.indexOf('z_fit --output') < readme.indexOf('a_evaluate --output'));
  assert.doesNotMatch(readme, /train_linear/);
});

test('shell/GPU reproduction describes required allocation and executor-only checkpoints', async () => {
  const exp = {
    ...experiment,
    recipe_id: 'agent_experiment_v1',
    snapshot: {
      plan: {
        stages: [
          { id: 'train', script: 'train.sh' },
          { id: 'evaluate', script: 'eval.sh' },
        ],
      },
    },
  };
  const files = unzipSync(
    await evidenceBundle(exp, async (a) => (a.id === figureId ? figure : report)),
  );
  const readme = strFromU8(files['README.txt']);
  assert.match(readme, /bash evidence\/train.sh/);
  assert.match(readme, /bash evidence\/eval.sh/);
  assert.match(readme, /CPU\/GPU allocation/);
  assert.match(readme, /Executor-only checkpoints/);
  assert.doesNotMatch(readme, /recipe.py train|fit CPU models locally/);
});

test('iteration downloads preserve duplicate historical names by object ID without reproduction claims', async () => {
  const exp = {
    ...experiment,
    contract: { execution_contract: 3 },
    artifacts: [...artifacts, { ...artifacts[0], id: '00000000-0000-4000-8000-000000000099' }],
  };
  const files = unzipSync(
    await evidenceBundle(exp, async (a) => (a.name === 'predictions.svg' ? figure : report)),
  );
  assert.ok(files[`evidence/${artifacts[0].id}/${artifacts[0].name}`]);
  assert.ok(files[`evidence/00000000-0000-4000-8000-000000000099/${artifacts[0].name}`]);
  assert.match(strFromU8(files['README.txt']), /does not promise reproducibility/);
  assert.doesNotMatch(strFromU8(files['README.txt']), /pip install/);
});
