import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { readFile, readdir, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { Miniflare } from 'miniflare';

let mf, directory;
const user = 'u'.repeat(40),
  machine = 'e'.repeat(40),
  run = randomUUID();
const req = (url, method = 'GET', body, token = user) =>
  mf.dispatchFetch('https://autopilot.test' + url, {
    method,
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
before(async () => {
  directory = await mkdtemp(path.join(tmpdir(), 'et-autopilot-'));
  mf = new Miniflare({
    modules: true,
    scriptPath: path.resolve('dist/server/index.js'),
    compatibilityDate: '2025-09-01',
    bindings: {
      AUTH_MODE: 'token',
      USER_TOKEN: user,
      EXECUTOR_TOKEN: machine,
      EXECUTOR_ID: 'acacia',
    },
    d1Databases: { DB: 'test-autopilot' },
    r2Buckets: { BUCKET: 'test-autopilot' },
    d1Persist: path.join(directory, 'd1'),
    r2Persist: path.join(directory, 'r2'),
  });
  const db = await mf.getD1Database('DB');
  for (const name of (await readdir('drizzle')).filter((n) => n.endsWith('.sql')).sort())
    for (const statement of (await readFile('drizzle/' + name, 'utf8'))
      .split('--> statement-breakpoint')
      .map((s) => s.trim())
      .filter(Boolean))
      await db.prepare(statement).run();
});
after(async () => {
  await mf?.dispose();
  if (directory) await rm(directory, { recursive: true, force: true });
});
const view = (revision, docs = [], extra = {}) => ({
  project_id: 'test-project',
  run_id: run,
  revision,
  observed_at: 1000 + revision,
  view: {
    schema_version: 1,
    run: { id: run, project_id: 'test-project', revision, created_at: 1000, status: 'active' },
    documents: docs,
    ...extra,
  },
});

test('unsupported executor and browser/machine roles are enforced', async () => {
  assert.equal(
    (await req('/api/projects/test-project/autopilot', 'GET', undefined, machine)).status,
    401,
  );
  assert.equal((await req('/api/executor/autopilot-commands')).status, 401);
  const response = await req('/api/projects/test-project/autopilot/commands', 'POST', {
    request_id: randomUUID(),
    kind: 'activate',
    expected_revision: 0,
  });
  assert.equal(response.status, 409);
  const db = await mf.getD1Database('DB');
  await db
    .prepare('INSERT INTO executor_status VALUES(?,?,?)')
    .bind('acacia', JSON.stringify({ autopilot: { available: true } }), 1000)
    .run();
});
test('offline command redelivery has stable identities and immutable outcomes', async () => {
  const payload = { request_id: randomUUID(), kind: 'pause', expected_revision: 2 };
  const create = () => req('/api/projects/test-project/autopilot/commands', 'POST', payload);
  assert.equal((await create()).status, 202);
  assert.equal((await create()).status, 202);
  let response = await req('/api/executor/autopilot-commands', 'GET', undefined, machine);
  const commands = (await response.json()).commands;
  assert.equal(commands.length, 1);
  assert.equal(commands[0].request_id, payload.request_id);
  assert.equal(
    (
      await req('/api/projects/test-project/autopilot/commands', 'POST', {
        ...payload,
        kind: 'stop',
      })
    ).status,
    409,
  );
  const receipt = { request_id: payload.request_id, state: 'accepted', error: null };
  assert.equal(
    (await req('/api/executor/autopilot-commands', 'POST', receipt, machine)).status,
    200,
  );
  assert.equal(
    (await req('/api/executor/autopilot-commands', 'POST', receipt, machine)).status,
    200,
  );
  assert.equal(
    (
      await req(
        '/api/executor/autopilot-commands',
        'POST',
        { ...receipt, state: 'rejected', error: 'different' },
        machine,
      )
    ).status,
    409,
  );
  response = await req('/api/executor/autopilot-commands', 'GET', undefined, machine);
  assert.equal((await response.json()).commands.length, 0);
});
test('R2 objects precede readiness, immutable versions survive stale projections', async () => {
  const doc = { name: 'paper.tex', version: 1 };
  assert.equal((await req('/api/executor/autopilot', 'POST', view(1, [doc]), machine)).status, 409);
  const object = {
    project_id: 'test-project',
    run_id: run,
    ...doc,
    content: 'Synthetic working draft. TODO real science.',
    sources: [],
    author: 'synthetic-fixture',
    created_at: 1000,
  };
  assert.equal(
    (await req('/api/executor/autopilot-documents', 'POST', object, machine)).status,
    200,
  );
  assert.equal(
    (await req('/api/executor/autopilot-documents', 'POST', object, machine)).status,
    200,
  );
  assert.equal(
    (
      await req(
        '/api/executor/autopilot-documents',
        'POST',
        { ...object, content: 'Changed bytes' },
        machine,
      )
    ).status,
    409,
  );
  assert.equal((await req('/api/executor/autopilot', 'POST', view(3, [doc]), machine)).status, 200);
  assert.equal((await req('/api/executor/autopilot', 'POST', view(1, []), machine)).status, 200);
  const response = await req('/api/projects/test-project/autopilot');
  const saved = await response.json();
  assert.equal(saved.run.revision, 3);
  assert.equal(saved.documents.length, 1);
  const path = `/api/projects/test-project/autopilot/documents/paper.tex/versions/1?run_id=${run}`;
  assert.equal(await (await req(path)).text(), object.content);
  assert.equal((await req(path.replace('test-project', 'other-project'))).status, 404);
  const handoff = await req('/api/projects/test-project/autopilot/handoff');
  assert.equal(handoff.status, 200);
  assert.equal(handoff.headers.get('content-type'), 'application/zip');
  assert.ok((await handoff.arrayBuffer()).byteLength > 20);
});
test('project input and handoff notices join the private inbox without repeated budget notices', async () => {
  const notice = {
    id: 'synthetic-attention',
    kind: 'researcher_input',
    text: 'Synthetic dataset access prerequisite',
    created_at: 1000,
    resolved_at: null,
  };
  assert.equal(
    (
      await req(
        '/api/executor/autopilot',
        'POST',
        view(4, [{ name: 'paper.tex', version: 1 }], { notices: [notice] }),
        machine,
      )
    ).status,
    200,
  );
  const response = await req('/api/inbox');
  assert.equal(response.status, 200);
  const entries = (await response.json()).entries;
  assert.equal(entries.filter((e) => e.id === notice.id).length, 1);
  assert.equal(entries.find((e) => e.id === notice.id).project_id, 'test-project');
});

test('attention push intents commit with project view and deduplicate unchanged observations', async () => {
  const db = await mf.getD1Database('DB'),
    eid = randomUUID();
  await db
    .prepare('INSERT INTO projections(id,revision,payload,updated_at) VALUES(?,1,?,1000)')
    .bind(eid, JSON.stringify({ id: eid, revision: 1 }))
    .run();
  await db
    .prepare('INSERT INTO push_subscriptions VALUES(?,?,?,1000)')
    .bind('synthetic-device', 'local-owner', '{}')
    .run();
  await db.prepare('INSERT INTO push_follows VALUES(?,?,1000)').bind('synthetic-device', eid).run();
  const notice = {
    id: 'synthetic-handoff-notice',
    kind: 'handoff',
    text: 'Synthetic incomplete handoff for review',
    created_at: 1000,
    resolved_at: null,
  };
  const value = view(5, [{ name: 'paper.tex', version: 1 }], {
    policy: { notifications: true },
    experiments: [{ experiment_id: eid }],
    notices: [notice],
  });
  assert.equal((await req('/api/executor/autopilot', 'POST', value, machine)).status, 200);
  assert.equal((await req('/api/executor/autopilot', 'POST', value, machine)).status, 200);
  let rows = await db.prepare("SELECT * FROM push_outbox WHERE id LIKE 'autopilot:%'").all();
  assert.equal(rows.results.length, 1);
  assert.equal(JSON.parse(rows.results[0].payload).url, '/?project=test-project&view=memory');
  value.revision++;
  value.view.run.revision++;
  value.observed_at++;
  assert.equal((await req('/api/executor/autopilot', 'POST', value, machine)).status, 200);
  rows = await db.prepare("SELECT * FROM push_outbox WHERE id LIKE 'autopilot:%'").all();
  assert.equal(rows.results.length, 1);
});
