import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { readFile, mkdtemp, rm, readdir } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
import { Miniflare } from 'miniflare';

let mf, directory;
const user = 'u'.repeat(40),
  executor = 'e'.repeat(40);
const headers = (token) => ({
  Authorization: `Bearer ${token}`,
  'Content-Type': 'application/json',
});
async function req(url, { token = user, ...options } = {}) {
  return mf.dispatchFetch('https://gateway.test' + url, {
    ...options,
    headers: { ...headers(token), ...options.headers },
  });
}
async function create(extra = {}) {
  const payload = {
    schema_version: 1,
    request_id: randomUUID(),
    brief: 'Compare fixed synthetic linear and quadratic regression.',
    recipe_id: 'toy_compare_v1',
    ...extra,
  };
  const response = await req('/api/experiments', { method: 'POST', body: JSON.stringify(payload) });
  assert.equal(response.status, 202);
  return { payload, result: await response.json() };
}
before(async () => {
  directory = await mkdtemp(path.join(tmpdir(), 'et-gateway-'));
  mf = new Miniflare({
    modules: true,
    scriptPath: path.resolve('dist/server/index.js'),
    compatibilityDate: '2025-09-01',
    bindings: {
      AUTH_MODE: 'token',
      USER_TOKEN: user,
      EXECUTOR_TOKEN: executor,
      EXECUTOR_ID: 'acacia',
    },
    d1Databases: { DB: 'et-test-db' },
    r2Buckets: { BUCKET: 'et-test-bucket' },
    d1Persist: path.join(directory, 'd1'),
    r2Persist: path.join(directory, 'r2'),
  });
  const db = await mf.getD1Database('DB');
  for (const file of (await readdir('drizzle')).filter((name) => name.endsWith('.sql')).sort()) {
    const sql = await readFile(path.join('drizzle', file), 'utf8');
    for (const statement of sql
      .split('--> statement-breakpoint')
      .map((v) => v.trim())
      .filter(Boolean))
      await db.prepare(statement).run();
  }
});
after(async () => {
  await mf?.dispose();
  if (directory) await rm(directory, { recursive: true, force: true });
});
test('user and executor credentials cannot cross roles', async () => {
  assert.equal((await req('/api/experiments', { token: executor })).status, 401);
  assert.equal((await req('/api/executor/commands', { token: user })).status, 401);
  assert.equal((await req('/api/experiments', { token: 'invalid' })).status, 401);
  assert.equal(
    (
      await req('/api/experiments', {
        method: 'POST',
        body: JSON.stringify({
          request_id: randomUUID(),
          brief: 'Unsafe arbitrary recipe',
          recipe_id: 'shell',
          shell: 'sbatch',
        }),
      })
    ).status,
    422,
  );
});
test('Sites browser mutations require matching Origin even with an unrelated bearer header', async () => {
  const sites = new Miniflare({
    modules: true,
    scriptPath: path.resolve('dist/server/index.js'),
    compatibilityDate: '2025-09-01',
    bindings: { AUTH_MODE: 'sites', EXECUTOR_TOKEN: executor, EXECUTOR_ID: 'acacia' },
    d1Databases: { DB: 'et-auth-only' },
    r2Buckets: { BUCKET: 'et-auth-only' },
  });
  try {
    const request = (origin) =>
      sites.dispatchFetch('https://gateway.test/api/experiments', {
        method: 'POST',
        headers: {
          ...headers(executor),
          'oai-authenticated-user-id': 'test-dispatch-identity',
          Origin: origin,
        },
        body: '{}',
      });
    assert.equal((await request('https://elsewhere.test')).status, 403);
    assert.equal((await request('https://gateway.test')).status, 422); // authenticated, then invalid schema
    assert.equal(
      (
        await sites.dispatchFetch('https://gateway.test/api/experiments', {
          headers: headers(executor),
        })
      ).status,
      401,
    );
  } finally {
    await sites.dispose();
  }
});
test('duplicate commands allocate one stable identity; notes survive worker absence', async () => {
  const { payload, result } = await create();
  const again = await req('/api/experiments', { method: 'POST', body: JSON.stringify(payload) });
  assert.equal((await again.json()).experiment_id, result.experiment_id);
  assert.equal(
    (
      await req('/api/experiments', {
        method: 'POST',
        body: JSON.stringify({ ...payload, brief: 'Another different brief' }),
      })
    ).status,
    409,
  );
  const note = {
    schema_version: 1,
    request_id: randomUUID(),
    text: 'Preserved while executor is offline',
  };
  assert.equal(
    (
      await req(`/api/experiments/${result.experiment_id}/notes`, {
        method: 'POST',
        body: JSON.stringify(note),
      })
    ).status,
    202,
  );
  await req(`/api/experiments/${result.experiment_id}/notes`, {
    method: 'POST',
    body: JSON.stringify(note),
  });
  const view = await (await req(`/api/experiments/${result.experiment_id}`)).json();
  assert.equal(view.phase, 'waiting_for_executor');
  assert.equal(view.notes.length, 1);
  assert.equal(view.pending_commands.length, 2);
  assert.equal(
    (await (await req(`/api/commands/${payload.request_id}`)).json()).state,
    'waiting_for_executor',
  );
  const first = await (await req('/api/executor/commands', { token: executor })).json();
  const second = await (await req('/api/executor/commands', { token: executor })).json();
  assert.deepEqual(first, second);
  const ack = { schema_version: 1, request_ids: [payload.request_id, note.request_id] };
  await req('/api/executor/ack', { token: executor, method: 'POST', body: JSON.stringify(ack) });
  await req('/api/executor/ack', { token: executor, method: 'POST', body: JSON.stringify(ack) });
  assert.equal(
    (await (await req('/api/executor/commands', { token: executor })).json()).commands.some((c) =>
      ack.request_ids.includes(c.request_id),
    ),
    false,
  );
  assert.equal((await (await req(`/api/commands/${payload.request_id}`)).json()).state, 'applied');
  assert.equal(
    (await (await req(`/api/experiments/${result.experiment_id}`)).json()).pending_commands.length,
    0,
  );
});
test('durable command refusals survive redelivery without poisoning the queue', async () => {
  const { payload, result } = await create();
  const refusal = {
    schema_version: 1,
    rejections: [
      { request_id: payload.request_id, reason: 'Saved request expired before delivery' },
    ],
  };
  for (let i = 0; i < 2; i++)
    assert.equal(
      (
        await req('/api/executor/reject', {
          token: executor,
          method: 'POST',
          body: JSON.stringify(refusal),
        })
      ).status,
      200,
    );
  await req('/api/executor/ack', {
    token: executor,
    method: 'POST',
    body: JSON.stringify({ schema_version: 1, request_ids: [payload.request_id] }),
  });
  const commands = (await (await req('/api/executor/commands', { token: executor })).json())
    .commands;
  assert.equal(
    commands.some((c) => c.request_id === payload.request_id),
    false,
  );
  const inbox = (await (await req('/api/inbox')).json()).entries.filter(
    (n) => n.experiment_id === result.experiment_id && n.kind === 'command_rejected',
  );
  assert.equal(inbox.length, 1);
  assert.match(inbox[0].text, /expired/);
  assert.equal((await (await req(`/api/commands/${payload.request_id}`)).json()).state, 'rejected');
  assert.equal(
    (await req('/api/experiments', { method: 'POST', body: JSON.stringify(payload) })).status,
    409,
  );
});
test('plan review rejects stale versions and steering is a durable active command', async () => {
  const { result } = await create();
  const eid = result.experiment_id;
  const db = await mf.getD1Database('DB');
  const exp = await (await req(`/api/experiments/${eid}`)).json();
  exp.revision = 1;
  exp.phase = 'preparing';
  exp.plan_review = {
    state: 'awaiting_review',
    revision: 3,
    summary: 'Compare two scales',
    protocol: 'Fixed split',
    stages: [],
  };
  await db
    .prepare('UPDATE projections SET revision=?,payload=? WHERE id=?')
    .bind(exp.revision, JSON.stringify(exp), eid)
    .run();
  const endpoint = `/api/experiments/${eid}/plan-review`;
  const approval = {
    schema_version: 1,
    request_id: randomUUID(),
    decision: 'revise',
    feedback: 'Include native scale',
    plan_revision: 3,
  };
  const post = (url, value) => req(url, { method: 'POST', body: JSON.stringify(value) });
  assert.equal((await post(endpoint, { ...approval, plan_revision: 2 })).status, 409);
  assert.equal((await post(endpoint, { ...approval, feedback: '' })).status, 422);
  assert.equal((await post(endpoint, approval)).status, 202);
  const again = await post(endpoint, approval);
  assert.equal(again.status, 202);
  assert.equal((await again.json()).experiment_id, eid);
  exp.plan_review.state = 'reviewed';
  await db
    .prepare('UPDATE projections SET payload=? WHERE id=?')
    .bind(JSON.stringify(exp), eid)
    .run();
  assert.equal((await post(endpoint, approval)).status, 202);
  assert.equal((await post(endpoint, { ...approval, request_id: randomUUID() })).status, 409);
  const steer = { schema_version: 1, request_id: randomUUID(), text: 'x'.repeat(50000) };
  assert.equal((await post(`/api/experiments/${eid}/steering`, steer)).status, 202);
  assert.equal(
    (
      await post(`/api/experiments/${eid}/steering`, {
        ...steer,
        request_id: randomUUID(),
        text: 'x'.repeat(50001),
      })
    ).status,
    422,
  );
  const restart = {
    schema_version: 1,
    request_id: randomUUID(),
    text: 'Use native scale instead',
    mode: 'restart',
  };
  assert.equal((await post(`/api/experiments/${eid}/steering`, restart)).status, 202);
  assert.equal(
    (await post(`/api/experiments/${eid}/steering`, { ...restart, mode: 'invalid' })).status,
    422,
  );
  const commands = (await (await req('/api/executor/commands', { token: executor })).json())
    .commands;
  assert.deepEqual(commands.find((c) => c.request_id === approval.request_id).payload, approval);
  assert.equal(commands.find((c) => c.request_id === approval.request_id).kind, 'plan_review');
  assert.equal(commands.find((c) => c.request_id === steer.request_id).kind, 'steer');
  assert.equal(commands.find((c) => c.request_id === steer.request_id).payload.text, steer.text);
  assert.equal(commands.find((c) => c.request_id === steer.request_id).payload.mode, 'later');
  assert.deepEqual(commands.find((c) => c.request_id === restart.request_id).payload, restart);
  exp.phase = 'complete';
  await db
    .prepare('UPDATE projections SET payload=? WHERE id=?')
    .bind(JSON.stringify(exp), eid)
    .run();
  assert.equal(
    (await post(`/api/experiments/${eid}/steering`, { ...steer, request_id: randomUUID() })).status,
    409,
  );
  exp.phase = 'failed';
  await db
    .prepare('UPDATE projections SET payload=? WHERE id=?')
    .bind(JSON.stringify(exp), eid)
    .run();
  assert.equal(
    (await post(`/api/experiments/${eid}/steering`, { ...steer, request_id: randomUUID() })).status,
    409,
  );
  assert.equal(
    (await post(`/api/experiments/${eid}/steering`, { ...restart, request_id: randomUUID() }))
      .status,
    202,
  );
  const title = { schema_version: 1, request_id: randomUUID(), action: 'summarize_title' };
  for (let i = 0; i < 2; i++)
    assert.equal((await post(`/api/experiments/${eid}/actions`, title)).status, 202);
  const queued = (await (await req('/api/executor/commands', { token: executor })).json()).commands;
  assert.equal(queued.filter((c) => c.request_id === title.request_id).length, 1);
});
test('quota reserve commands survive offline delivery and have global receipts', async () => {
  const payload = {
    schema_version: 1,
    request_id: randomUUID(),
    five_hour_percent: 20,
    weekly_percent: 8,
  };
  const post = (value) =>
    req('/api/quota/reserve', { method: 'POST', body: JSON.stringify(value) });
  assert.equal((await post({ ...payload, five_hour_percent: 101 })).status, 422);
  assert.equal((await post(payload)).status, 202);
  assert.equal((await post(payload)).status, 202);
  assert.equal((await post({ ...payload, weekly_percent: 9 })).status, 409);
  let receipt = await (await req(`/api/commands/${payload.request_id}`)).json();
  assert.equal(receipt.state, 'waiting_for_executor');
  assert.equal(receipt.experiment_id, undefined);
  const commands = (await (await req('/api/executor/commands', { token: executor })).json())
    .commands;
  const delivered = commands.find((c) => c.request_id === payload.request_id);
  assert.equal(delivered.kind, 'reserve');
  assert.equal(delivered.experiment_id, null);
  assert.deepEqual(delivered.payload, payload);
  await req('/api/executor/ack', {
    token: executor,
    method: 'POST',
    body: JSON.stringify({ schema_version: 1, request_ids: [payload.request_id] }),
  });
  receipt = await (await req(`/api/commands/${payload.request_id}`)).json();
  assert.equal(receipt.state, 'applied');
  assert.equal((await (await post(payload)).json()).state, 'applied');
  const rejected = { ...payload, request_id: randomUUID() };
  assert.equal((await post(rejected)).status, 202);
  await req('/api/executor/reject', {
    token: executor,
    method: 'POST',
    body: JSON.stringify({
      schema_version: 1,
      rejections: [{ request_id: rejected.request_id, reason: 'Invalid local policy' }],
    }),
  });
  assert.equal(
    (await (await req(`/api/commands/${rejected.request_id}`)).json()).state,
    'rejected',
  );
});
test('project instructions update has a durable identity and immutable local result', async () => {
  const payload = {
    schema_version: 1,
    request_id: randomUUID(),
    instructions: 'Prefer available capable GPUs.',
    execution: {
      context: 'jean-zay',
      ssh_host: 'jz',
      remote_project: '$WORK/source-supervision',
      rsync_up: 'rsync -az {local_project}/ {host}:{remote_project}/',
      rsync_down: 'rsync -az {host}:{remote_run}/ {local_run}/',
      poll_seconds: 600,
    },
  };
  const url = '/api/projects/test-project/settings';
  const post = (value) => req(url, { method: 'PATCH', body: JSON.stringify(value) });
  assert.equal((await post(payload)).status, 202);
  assert.equal((await post(payload)).status, 202);
  assert.equal((await post({ ...payload, instructions: 'Different' })).status, 409);
  const queued = (
    await (await req('/api/executor/project-settings-commands', { token: executor })).json()
  ).commands;
  assert.deepEqual(queued.find((c) => c.request_id === payload.request_id).payload, payload);
  const result = {
    request_id: payload.request_id,
    project_id: 'test-project',
    state: 'applied',
    error: null,
    settings: { instructions: payload.instructions, revision: 1, execution: payload.execution },
  };
  const publish = (item) =>
    req('/api/executor/project-settings-results', {
      token: executor,
      method: 'POST',
      body: JSON.stringify({ schema_version: 1, results: [item] }),
    });
  assert.equal((await publish(result)).status, 200);
  assert.equal((await publish(result)).status, 200);
  assert.equal(
    (await publish({ ...result, settings: { ...result.settings, revision: 2 } })).status,
    409,
  );
  const receipt = await (await req(`/api/projects/settings/requests/${payload.request_id}`)).json();
  assert.deepEqual(receipt.settings, result.settings);
  assert.equal(receipt.state, 'applied');
  const status = await (await req('/api/status')).json();
  assert.equal(
    status.projects.find((p) => p.id === 'test-project').instructions,
    payload.instructions,
  );
  assert.deepEqual(
    status.projects.find((p) => p.id === 'test-project').execution,
    payload.execution,
  );
  const db = await mf.getD1Database('DB');
  await db
    .prepare('DELETE FROM project_settings_requests WHERE request_id=?')
    .bind(payload.request_id)
    .run();
});
test('project deletion receipt hides stale published and configured projects', async () => {
  const request_id = randomUUID();
  const payload = { schema_version: 1, request_id, instructions: '', delete: true };
  const url = '/api/projects/test-project/settings';
  assert.equal((await req(url, { method: 'PATCH', body: JSON.stringify(payload) })).status, 202);
  const result = {
    request_id,
    project_id: 'test-project',
    state: 'applied',
    error: null,
    settings: { instructions: '', revision: 999, deleted: true },
  };
  const response = await req('/api/executor/project-settings-results', {
    token: executor,
    method: 'POST',
    body: JSON.stringify({ schema_version: 1, results: [result] }),
  });
  assert.equal(response.status, 200);
  const status = await (await req('/api/status')).json();
  assert.equal(
    status.projects.some((p) => p.id === 'test-project'),
    false,
  );
  assert.equal((await req(url, { method: 'PATCH', body: JSON.stringify(payload) })).status, 202);
  const db = await mf.getD1Database('DB');
  await db
    .prepare('DELETE FROM project_settings_requests WHERE request_id=?')
    .bind(request_id)
    .run();
});
test('job telemetry survives D1 publication and cannot advance readiness or overwrite newer samples', async () => {
  const { result } = await create();
  const eid = result.experiment_id;
  const initial = await (await req(`/api/experiments/${eid}`)).json();
  const job = {
    intent_id: randomUUID(),
    stage_id: 'train_linear',
    job_id: '123',
    state: 'submitted',
    scheduler_state: 'RUNNING',
    progress: { completed: 25, total: 100, reported_at: 100, eta_seconds: 300 },
  };
  const view = {
    ...initial,
    revision: 2,
    phase: 'training',
    hold_reason: null,
    stages: [{ id: 'train_linear', state: 'running' }],
    jobs: [job],
  };
  const send = (experiment, event_id = randomUUID()) =>
    req('/api/executor/publish', {
      token: executor,
      method: 'POST',
      body: JSON.stringify({
        schema_version: 1,
        executor_id: 'acacia',
        messages: [{ schema_version: 1, event_id, experiment, inbox: [] }],
      }),
    });
  const event = randomUUID();
  assert.equal((await send(view, event)).status, 200);
  assert.equal((await send(view, event)).status, 200);
  assert.deepEqual(
    (await (await req(`/api/experiments/${eid}`)).json()).jobs[0].progress,
    job.progress,
  );
  const newer = {
    ...view,
    revision: 3,
    jobs: [{ ...job, progress: { ...job.progress, completed: 100, reported_at: 200 } }],
  };
  assert.equal((await send(newer)).status, 200);
  assert.equal((await send(view)).status, 200);
  const saved = await (await req(`/api/experiments/${eid}`)).json();
  assert.equal(saved.jobs[0].progress.completed, 100);
  assert.equal(saved.phase, 'training');
  assert.equal(saved.stages[0].state, 'running');
  assert.equal(saved.artifacts.length, 0);
});
test('monotonic projections, upload verification, and result readiness', async () => {
  const { result } = await create(),
    eid = result.experiment_id;
  const initial = await (await req(`/api/experiments/${eid}`)).json();
  const names = [
    'linear.json',
    'quadratic.json',
    'results.json',
    'predictions.json',
    'predictions.svg',
    'analysis.json',
    'report.html',
  ];
  const bytes = Buffer.from('trusted synthetic test deliverable');
  const sha256 = createHash('sha256').update(bytes).digest('hex');
  const artifacts = names.map((name) => ({
    id: randomUUID(),
    name,
    sha256,
    size: bytes.length,
    media_type: name.endsWith('.html')
      ? 'text/html'
      : name.endsWith('.svg')
        ? 'image/svg+xml'
        : 'application/json',
    local_valid: 1,
    remote_published: 0,
  }));
  const send = async (exp, event = randomUUID(), inbox = []) =>
    req('/api/executor/publish', {
      token: executor,
      method: 'POST',
      body: JSON.stringify({
        schema_version: 1,
        executor_id: 'acacia',
        messages: [{ schema_version: 1, event_id: event, experiment: exp, inbox }],
      }),
    });
  const exp = {
    ...initial,
    phase: 'publishing',
    hold_reason: 'publication_pending',
    revision: 5,
    artifacts,
    snapshot: {
      snapshot_hash: 'fixed',
      parameters_hash: 'params',
      data_hash: 'data',
      test_hash: 'test',
    },
  };
  const event = randomUUID();
  assert.equal((await send(exp, event)).status, 200);
  assert.equal((await send(exp, event)).status, 200);
  assert.equal((await send({ ...exp, revision: 6 }, event)).status, 409);
  assert.equal(
    (
      await send({
        ...exp,
        revision: 6,
        artifacts: artifacts.map((a, i) => (i === 0 ? { ...a, name: 'renamed.json' } : a)),
      })
    ).status,
    409,
  );
  await send({ ...exp, phase: 'training', revision: 2 });
  assert.equal((await (await req(`/api/experiments/${eid}`)).json()).revision, 5);
  assert.equal(
    (await send({ ...exp, phase: 'complete', revision: 6, hold_reason: null })).status,
    409,
  );
  const bad = await req(`/api/executor/artifacts/${artifacts[0].id}`, {
    token: executor,
    method: 'PUT',
    body: 'wrong bytes',
  });
  assert.equal(bad.status, 409);
  assert.equal((await req(`/api/artifacts/${artifacts[0].id}`)).status, 404);
  for (const artifact of artifacts) {
    const response = await req(`/api/executor/artifacts/${artifact.id}`, {
      token: executor,
      method: 'PUT',
      body: bytes,
    });
    assert.equal(response.status, 200);
  }
  const entry = {
    id: randomUUID(),
    experiment_id: eid,
    kind: 'result_ready',
    text: 'Interpreted result ready',
    created_at: Date.now() / 1000,
  };
  assert.equal(
    (
      await send({ ...exp, phase: 'complete', revision: 6, hold_reason: null }, randomUUID(), [
        entry,
      ])
    ).status,
    200,
  );
  await send({ ...exp, phase: 'complete', revision: 6, hold_reason: null }, randomUUID(), [entry]);
  const inbox = await (await req('/api/inbox')).json();
  assert.equal(inbox.entries.filter((n) => n.id === entry.id).length, 1);
  const fetched = await req(`/api/artifacts/${artifacts[6].id}`);
  assert.equal(await fetched.text(), bytes.toString());
  assert.match(fetched.headers.get('content-security-policy'), /sandbox/);
  const db = await mf.getD1Database('DB');
  assert.equal(
    (await db.prepare('SELECT COUNT(*) AS n FROM events WHERE id=?').bind(event).first()).n,
    1,
  );
});
test('new experiments may reference only completed frozen results', async () => {
  const list = (await (await req('/api/experiments')).json()).experiments;
  const reference = list.find((e) => e.phase === 'complete');
  assert.ok(reference);
  const { result } = await create({ reference_experiment_id: reference.id });
  const child = await (await req(`/api/experiments/${result.experiment_id}`)).json();
  assert.equal(child.reference_experiment_id, reference.id);
  assert.equal(child.phase, 'waiting_for_executor');
  const invalid = {
    schema_version: 1,
    request_id: randomUUID(),
    recipe_id: 'toy_compare_v1',
    brief: 'Compare against an unfinished reference.',
    reference_experiment_id: child.id,
  };
  assert.equal(
    (await req('/api/experiments', { method: 'POST', body: JSON.stringify(invalid) })).status,
    409,
  );
});

test('arbitrary-method briefs persist their generic recipe identity', async () => {
  const { payload, result } = await create({
    recipe_id: 'agent_experiment_v1',
    brief: 'Test a decision tree and visualize its frontier.',
  });
  const view = await (await req(`/api/experiments/${result.experiment_id}`)).json();
  assert.equal(view.recipe_id, 'agent_experiment_v1');
  assert.equal(view.brief, payload.brief);
  assert.deepEqual(
    view.stages.map((s) => s.id),
    ['prepare', 'validate', 'interpret', 'publish'],
  );
  const duplicate = await req('/api/experiments', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
  assert.equal((await duplicate.json()).experiment_id, result.experiment_id);
});

test('generic shell/GPU plans exceed three stages and require published local checkpoint provenance', async () => {
  const { result } = await create({ recipe_id: 'agent_experiment_v1' });
  const eid = result.experiment_id;
  const view = await (await req(`/api/experiments/${eid}`)).json();
  const bytes = Buffer.from('generic evidence');
  const names = [
    'run.sh',
    'requirements.txt',
    'manifest.json',
    'preparation.json',
    'results.json',
    'validation.json',
    'analysis.json',
    'report.html',
    'frontier.png',
  ];
  const artifacts = names.map((name) => ({
    id: randomUUID(),
    name,
    sha256: createHash('sha256').update(bytes).digest('hex'),
    size: bytes.length,
    media_type: name.endsWith('.png') ? 'image/png' : 'application/octet-stream',
    local_valid: true,
    remote_published: false,
  }));
  const exp = {
    ...view,
    revision: 1,
    phase: 'publishing',
    artifacts,
    snapshot: {
      files: { 'run.sh': artifacts[0].sha256, 'requirements.txt': artifacts[1].sha256 },
      plan: {
        stages: [
          {
            id: 'train',
            script: 'run.sh',
            outputs: [],
            local_outputs: ['checkpoints/model.pth'],
            resources: { gpu_type: 'a100', gpus_per_job: 1, walltime_minutes_per_job: 720 },
          },
          { id: 'infer', script: 'run.sh', outputs: [] },
          { id: 'measure', script: 'run.sh', outputs: [] },
          {
            id: 'requested_method',
            script: 'run.sh',
            outputs: ['results.json', 'validation.json', 'frontier.png', 'raw.csv'],
          },
        ],
      },
    },
  };
  const send = (e) =>
    req('/api/executor/publish', {
      token: executor,
      method: 'POST',
      body: JSON.stringify({
        schema_version: 1,
        executor_id: 'acacia',
        messages: [{ schema_version: 1, event_id: randomUUID(), experiment: e }],
      }),
    });
  assert.equal((await send(exp)).status, 200);
  for (const artifact of artifacts)
    assert.equal(
      (
        await req(`/api/executor/artifacts/${artifact.id}`, {
          token: executor,
          method: 'PUT',
          body: bytes,
        })
      ).status,
      200,
    );
  assert.equal(
    (await send({ ...exp, revision: 2, phase: 'complete', hold_reason: null })).status,
    409,
  );
  const raw = { ...artifacts[0], id: randomUUID(), name: 'raw.csv', media_type: 'text/csv' };
  const complete = { ...exp, revision: 2, artifacts: [...artifacts, raw] };
  assert.equal((await send(complete)).status, 200);
  assert.equal(
    (
      await req(`/api/executor/artifacts/${raw.id}`, {
        token: executor,
        method: 'PUT',
        body: bytes,
      })
    ).status,
    200,
  );
  assert.equal(
    (await send({ ...complete, revision: 3, phase: 'complete', hold_reason: null })).status,
    409,
  );
  const provenance = {
    ...raw,
    id: randomUUID(),
    name: 'local-output-train.json',
    media_type: 'application/json',
  };
  const withProvenance = {
    ...complete,
    revision: 3,
    artifacts: [...complete.artifacts, provenance],
  };
  assert.equal((await send(withProvenance)).status, 200);
  assert.equal(
    (
      await req(`/api/executor/artifacts/${provenance.id}`, {
        token: executor,
        method: 'PUT',
        body: bytes,
      })
    ).status,
    200,
  );
  assert.equal(
    (await send({ ...withProvenance, revision: 4, phase: 'complete', hold_reason: null })).status,
    200,
  );
  assert.ok(!withProvenance.artifacts.some((a) => a.name === 'checkpoints/model.pth'));
  const figure = await req(`/api/artifacts/${artifacts[8].id}`);
  assert.equal(figure.headers.get('content-type'), 'image/png');
  assert.equal(await figure.text(), bytes.toString());
});

test('selected model persists in command and waiting projection', async () => {
  const { payload, result } = await create({
    model: 'gpt-6.1-sol',
    reasoning_effort: 'medium',
    workspace_mode: 'worktree',
  });
  const view = await (await req(`/api/experiments/${result.experiment_id}`)).json();
  assert.deepEqual(view.contract.agent, { model: 'gpt-6.1-sol', reasoning_effort: 'medium' });
  const command = (
    await (await req('/api/executor/commands', { token: executor })).json()
  ).commands.find((c) => c.request_id === payload.request_id);
  assert.equal(command.payload.model, 'gpt-6.1-sol');
  assert.equal(command.payload.reasoning_effort, 'medium');
  assert.equal(command.payload.workspace_mode, 'worktree');
});

test('deletion hides attention, survives retries and prevents projection resurrection', async () => {
  const { result } = await create();
  const eid = result.experiment_id;
  const initial = await (await req(`/api/experiments/${eid}`)).json();
  const send = (exp) =>
    req('/api/executor/publish', {
      token: executor,
      method: 'POST',
      body: JSON.stringify({
        schema_version: 1,
        executor_id: 'acacia',
        messages: [
          {
            schema_version: 1,
            event_id: randomUUID(),
            experiment: exp,
            inbox: [
              {
                id: randomUUID(),
                experiment_id: eid,
                kind: 'required_decision',
                text: 'Recovery exhausted',
                created_at: 100,
              },
            ],
          },
        ],
      }),
    });
  const body = { schema_version: 1, request_id: randomUUID() };
  const erase = () =>
    req(`/api/experiments/${eid}/delete`, { method: 'POST', body: JSON.stringify(body) });
  assert.equal((await erase()).status, 409);
  const failed = {
    ...initial,
    phase: 'failed',
    revision: 1,
    hold_reason: 'agent_recovery_exhausted',
  };
  assert.equal((await send(failed)).status, 200);
  assert.equal((await erase()).status, 202);
  assert.equal((await send({ ...failed, revision: 2, deleted_at: 200 })).status, 200);
  await req('/api/executor/ack', {
    token: executor,
    method: 'POST',
    body: JSON.stringify({ schema_version: 1, request_ids: [body.request_id] }),
  });
  assert.equal((await erase()).status, 202);
  assert.equal((await (await erase()).json()).state, 'applied');
  assert.equal((await send(failed)).status, 200); // delayed older projection cannot undo tombstone
  assert.equal((await send({ ...failed, revision: 3 })).status, 409);
  assert.equal((await req(`/api/experiments/${eid}`)).status, 410);
  assert.ok(!(await (await req('/api/experiments')).json()).experiments.some((e) => e.id === eid));
  assert.ok(!(await (await req('/api/inbox')).json()).entries.some((e) => e.experiment_id === eid));
  const db = await mf.getD1Database('DB');
  assert.equal(
    (await db.prepare('SELECT deleted_at FROM projections WHERE id=?').bind(eid).first())
      .deleted_at,
    200,
  );
});

test('a report revision registers while original remains complete and becomes ready only after upload', async () => {
  const original = (await (await req('/api/experiments')).json()).experiments.find(
    (e) => e.phase === 'complete',
  );
  assert.ok(original);
  const bytes = Buffer.from('<html>new readable report</html>');
  const sha256 = createHash('sha256').update(bytes).digest('hex');
  const reportId = randomUUID();
  const artifact = {
    id: reportId,
    name: 'report-v2.html',
    sha256,
    size: bytes.length,
    media_type: 'text/html',
    local_valid: 1,
    remote_published: 0,
  };
  const version = {
    version: 2,
    state: 'publishing',
    artifact_ids: [reportId],
    report_artifact_id: reportId,
    error: null,
    created_at: 200,
    updated_at: 200,
  };
  const exp = {
    ...original,
    revision: original.revision + 1,
    artifacts: [...original.artifacts, artifact],
    report_versions: [version],
    active_report_id: null,
  };
  const send = (experiment) =>
    req('/api/executor/publish', {
      token: executor,
      method: 'POST',
      body: JSON.stringify({
        schema_version: 1,
        executor_id: 'acacia',
        messages: [{ schema_version: 1, event_id: randomUUID(), experiment, inbox: [] }],
      }),
    });
  assert.equal((await send(exp)).status, 200);
  assert.equal((await req(`/api/experiments/${exp.id}`)).status, 200);
  const ready = {
    ...exp,
    revision: exp.revision + 1,
    report_versions: [{ ...version, state: 'ready' }],
    active_report_id: reportId,
  };
  assert.equal((await send(ready)).status, 409);
  const unrelated = {
    ...exp,
    revision: exp.revision + 1,
    report_versions: [{ ...version, artifact_ids: [reportId, original.artifacts[0].id] }],
  };
  assert.equal((await send(unrelated)).status, 422); // scientific evidence cannot be exempted as presentation
  assert.equal(
    (
      await req(`/api/executor/artifacts/${reportId}`, {
        token: executor,
        method: 'PUT',
        body: bytes,
      })
    ).status,
    200,
  );
  assert.equal((await send(ready)).status, 200);
  const fetched = await (await req(`/api/experiments/${exp.id}`)).json();
  assert.equal(fetched.active_report_id, reportId);
  assert.ok(fetched.artifacts.some((a) => a.name === 'report.html'));
  assert.equal(await (await req(`/api/artifacts/${reportId}`)).text(), bytes.toString());
});

test('discussion files register before upload while the original result remains ready', async () => {
  const list = await (await req('/api/experiments')).json();
  const original = list.experiments.find((e) => e.phase === 'complete');
  const id = randomUUID();
  const bytes = Buffer.from('<svg xmlns="http://www.w3.org/2000/svg"><path d="M0 1L1 0"/></svg>');
  const image = {
    id: randomUUID(),
    name: `discussion-${id}-loss.svg`,
    stage_id: `discuss:${id}`,
    size: bytes.length,
    sha256: createHash('sha256').update(bytes).digest('hex'),
    media_type: 'image/svg+xml',
    local_valid: 1,
    remote_published: 0,
  };
  const exp = {
    ...original,
    revision: original.revision + 1,
    artifacts: [...original.artifacts, image],
  };
  const send = (experiment) =>
    req('/api/executor/publish', {
      token: executor,
      method: 'POST',
      body: JSON.stringify({
        schema_version: 1,
        executor_id: 'acacia',
        messages: [{ schema_version: 1, event_id: randomUUID(), experiment, inbox: [] }],
      }),
    });
  assert.equal((await send(exp)).status, 200);
  assert.equal((await (await req(`/api/experiments/${exp.id}`)).json()).phase, 'complete');
  assert.equal((await req(`/api/artifacts/${image.id}`)).status, 404);
  assert.equal(
    (
      await req(`/api/executor/artifacts/${image.id}`, {
        token: executor,
        method: 'PUT',
        body: bytes,
      })
    ).status,
    200,
  );
  assert.equal(await (await req(`/api/artifacts/${image.id}`)).text(), bytes.toString());
  // A name alone does not exempt an original scientific artifact from readiness.
  const unrelated = {
    ...image,
    id: randomUUID(),
    name: `discussion-${id}-unregistered.svg`,
    stage_id: 'fit',
  };
  assert.equal(
    (await send({ ...exp, revision: exp.revision + 1, artifacts: [...exp.artifacts, unrelated] }))
      .status,
    409,
  );
});

test('conversation commands and replies persist, paginate, and reject identity changes', async () => {
  const list = await (await req('/api/experiments')).json();
  const original = list.experiments.find((e) => e.phase === 'complete');
  assert.ok(original);
  const url = `/api/experiments/${original.id}/conversation`;
  const payload = {
    request_id: randomUUID(),
    text: 'What should we test next?',
    model: 'gpt-6.1-sol',
    reasoning_effort: 'medium',
  };
  assert.equal(
    (await req(url, { token: executor, method: 'POST', body: JSON.stringify(payload) })).status,
    401,
  );
  assert.equal((await req(url, { method: 'POST', body: JSON.stringify(payload) })).status, 202);
  assert.equal((await req(url, { method: 'POST', body: JSON.stringify(payload) })).status, 202);
  assert.equal(
    (
      await req(url, {
        method: 'POST',
        body: JSON.stringify({ ...payload, text: 'Another request' }),
      })
    ).status,
    409,
  );
  const pending = await (await req(`/api/experiments/${original.id}`)).json();
  assert.equal(pending.pending_messages[0].text, payload.text);
  const send = (experiment) =>
    req('/api/executor/publish', {
      token: executor,
      method: 'POST',
      body: JSON.stringify({
        schema_version: 1,
        executor_id: 'acacia',
        messages: [{ schema_version: 1, event_id: randomUUID(), experiment, inbox: [] }],
      }),
    });
  const first = {
    id: payload.request_id,
    seq: 1,
    text: payload.text,
    model: payload.model,
    reasoning_effort: 'medium',
    state: 'waiting',
    hold_reason: 'quota_hold',
    error: null,
    reply: null,
    evidence_ids: [],
    report_version: null,
    created_at: 300,
    updated_at: 300,
    activity: [],
  };
  let exp = {
    ...original,
    revision: original.revision + 1,
    conversation: [first],
    conversation_total: 1,
  };
  assert.equal((await send(exp)).status, 200);
  const completed = {
    ...first,
    state: 'completed',
    hold_reason: null,
    reply: 'Compare independent data splits before making a general claim.',
    evidence_ids: [original.artifacts[0].id],
    artifact_ids: [original.artifacts[0].id],
    followup_experiment_id: randomUUID(),
    updated_at: 301,
  };
  exp = { ...exp, revision: exp.revision + 1, conversation: [completed] };
  assert.equal((await send(exp)).status, 200);
  for (const change of [{ artifact_ids: [] }, { followup_experiment_id: randomUUID() }]) {
    assert.equal(
      (
        await send({
          ...exp,
          revision: exp.revision + 1,
          conversation: [{ ...completed, ...change }],
        })
      ).status,
      409,
    );
  }
  assert.equal(
    (
      await send({
        ...exp,
        revision: exp.revision + 1,
        conversation: [{ ...completed, id: randomUUID(), seq: 2, artifact_ids: [randomUUID()] }],
      })
    ).status,
    422,
  );
  assert.equal(
    (
      await send({
        ...exp,
        revision: exp.revision + 1,
        conversation: [{ ...completed, reply: 'changed' }],
      })
    ).status,
    409,
  );
  assert.equal(
    (
      await send({
        ...exp,
        revision: exp.revision + 1,
        conversation: [{ ...completed, text: 'changed' }],
      })
    ).status,
    409,
  );
  assert.equal(
    (
      await send({
        ...exp,
        revision: exp.revision + 1,
        conversation: [{ ...completed, id: randomUUID() }],
      })
    ).status,
    409,
  );
  assert.equal(
    (
      await send({
        ...exp,
        revision: exp.revision + 1,
        conversation: [{ ...completed, id: randomUUID(), seq: 2, evidence_ids: [randomUUID()] }],
      })
    ).status,
    422,
  );
  const more = Array.from({ length: 24 }, (_, i) => ({
    ...completed,
    id: randomUUID(),
    seq: i + 2,
    text: `Historical message ${i + 2}`,
    created_at: 302 + i,
  }));
  for (const batch of [more.slice(0, 20), more.slice(20)]) {
    exp = {
      ...exp,
      revision: exp.revision + 1,
      conversation: batch,
      conversation_total: batch.at(-1).seq,
    };
    assert.equal((await send(exp)).status, 200);
  }
  // The latest projection omits the first 21 messages; the durable history still serves them.
  const latest = await (await req(url)).json();
  assert.equal(latest.messages.length, 20);
  assert.equal(latest.messages[0].seq, 6);
  assert.ok(latest.has_more);
  const older = await (await req(url + '?before=6')).json();
  assert.equal(older.messages.length, 5);
  assert.equal(older.messages[0].reply, completed.reply);
  assert.equal(older.has_more, false);
  assert.equal((await req(url + '?before=NaN')).status, 422);
  assert.equal((await req(url + '?before=0')).status, 422);
  assert.equal(
    (await send({ ...original, revision: original.revision + 1, conversation: [first] })).status,
    200,
  );
  assert.equal((await (await req(url + '?before=2')).json()).messages[0].state, 'completed');
  const waiting = await create();
  assert.equal(
    (
      await req(`/api/experiments/${waiting.result.experiment_id}/conversation`, {
        method: 'POST',
        body: JSON.stringify({ ...payload, request_id: randomUUID() }),
      })
    ).status,
    409,
  );
});

test('PWA manifest and phone icons are served with intact binary bytes and refreshable metadata', async () => {
  const manifestResponse = await req('/manifest.webmanifest');
  assert.equal(manifestResponse.headers.get('content-type'), 'application/manifest+json');
  assert.equal(manifestResponse.headers.get('cache-control'), 'no-cache');
  const manifest = await manifestResponse.json();
  assert.equal(manifest.display, 'standalone');
  assert.equal(manifest.id, '/');
  for (const name of [...manifest.icons.map((icon) => icon.src), '/apple-touch-icon.png']) {
    const response = await req(name);
    assert.equal(response.status, 200);
    assert.equal(response.headers.get('content-type'), 'image/png');
    const webRoot = existsSync('web/public') ? 'web/public' : '../web/public';
    const expected = await readFile(path.resolve(webRoot, name.slice(1)));
    assert.deepEqual(Buffer.from(await response.arrayBuffer()), expected);
  }
  const font = await req('/fonts/dm-sans-latin.woff2');
  assert.equal(font.status, 200);
  assert.equal(font.headers.get('content-type'), 'font/woff2');
  const bytes = Buffer.from(await font.arrayBuffer());
  assert.equal(bytes.subarray(0, 4).toString(), 'wOF2');
  const webRoot = existsSync('web/public') ? 'web/public' : '../web/public';
  assert.deepEqual(bytes, await readFile(path.resolve(webRoot, 'fonts/dm-sans-latin.woff2')));
  assert.equal((await req('/sw.js')).headers.get('cache-control'), 'no-cache');
});

test('configured projects persist through waiting, delivery and publication; references cannot cross projects', async () => {
  const projects = [
    { id: 'test-project', name: 'Test project', available: true },
    { id: 'anyup-rs', name: 'anyup-rs', available: true, supports_worktrees: false },
  ];
  const heartbeat = async (projects) =>
    req('/api/executor/publish', {
      token: executor,
      method: 'POST',
      body: JSON.stringify({
        schema_version: 1,
        executor_id: 'acacia',
        messages: [],
        heartbeat: { projects },
      }),
    });
  assert.equal((await heartbeat(projects)).status, 200);
  try {
    assert.deepEqual((await (await req('/api/status')).json()).projects, projects);
    const { result, payload } = await create({
      project_id: 'anyup-rs',
      recipe_id: 'agent_experiment_v1',
    });
    const initial = await (await req(`/api/experiments/${result.experiment_id}`)).json();
    assert.equal(initial.project_id, 'anyup-rs');
    assert.deepEqual(initial.contract.project, { id: 'anyup-rs', name: 'anyup-rs' });
    const commands = (await (await req('/api/executor/commands', { token: executor })).json())
      .commands;
    assert.equal(
      commands.find((c) => c.request_id === payload.request_id).payload.project_id,
      'anyup-rs',
    );
    const send = (experiment) =>
      req('/api/executor/publish', {
        token: executor,
        method: 'POST',
        body: JSON.stringify({
          schema_version: 1,
          executor_id: 'acacia',
          messages: [{ schema_version: 1, event_id: randomUUID(), experiment }],
        }),
      });
    assert.equal(
      (await send({ ...initial, phase: 'preparing', hold_reason: null, revision: 1 })).status,
      200,
    );
    assert.equal(
      (
        await send({
          ...initial,
          revision: 2,
          project_id: 'test-project',
          contract: { project: { id: 'test-project', name: 'Test project' } },
        })
      ).status,
      409,
    );
    const db = await mf.getD1Database('DB');
    await db
      .prepare('UPDATE projections SET payload=? WHERE id=?')
      .bind(
        JSON.stringify({ ...initial, phase: 'complete', snapshot: { snapshot_hash: 'fixture' } }),
        initial.id,
      )
      .run();
    const referenceBody = {
      request_id: randomUUID(),
      brief: 'A separate project-specific comparison.',
      recipe_id: 'agent_experiment_v1',
      reference_experiment_id: initial.id,
      project_id: 'test-project',
    };
    assert.equal(
      (await req('/api/experiments', { method: 'POST', body: JSON.stringify(referenceBody) }))
        .status,
      409,
    );
    await create({ ...referenceBody, request_id: randomUUID(), project_id: 'anyup-rs' });
    await heartbeat([projects[0], { ...projects[1], available: false }]);
    // Previously saved creation receipts survive a missing checkout and remain deduplicated.
    assert.deepEqual(
      await (
        await req('/api/experiments', { method: 'POST', body: JSON.stringify(payload) })
      ).json(),
      result,
    );
    assert.equal(
      (
        await req('/api/experiments', {
          method: 'POST',
          body: JSON.stringify({ ...payload, request_id: randomUUID() }),
        })
      ).status,
      409,
    );
  } finally {
    await heartbeat([projects[0]]);
  }
});

test('new and existing project requests survive offline delivery and immutable saved outcomes', async () => {
  const payload = {
    schema_version: 1,
    request_id: randomUUID(),
    name: 'Existing research',
    mode: 'existing',
    folder: '~/bench/example',
    allow_edits: true,
    instructions: 'Choose a capable GPU with a short queue.',
  };
  const post = (body, token = user) =>
    req('/api/projects', { token, method: 'POST', body: JSON.stringify(body) });
  assert.equal((await post(payload, executor)).status, 401);
  const response = await post(payload);
  assert.equal(response.status, 202);
  const saved = await response.json();
  assert.equal(saved.state, 'waiting_for_executor');
  assert.equal(saved.project_id, 'project-' + payload.request_id.replaceAll('-', ''));
  assert.deepEqual(await (await post(payload)).json(), saved);
  assert.equal((await post({ ...payload, name: 'Changed' })).status, 409);
  assert.equal(
    (await post({ ...payload, request_id: randomUUID(), allow_edits: false })).status,
    422,
  );
  const waiting = await (await req(`/api/projects/requests/${payload.request_id}`)).json();
  assert.equal(waiting.state, 'waiting_for_executor');
  assert.equal('payload' in waiting, false);
  assert.equal((await req('/api/executor/project-commands')).status, 401);
  const commands = (await (await req('/api/executor/project-commands', { token: executor })).json())
    .commands;
  assert.deepEqual(commands.find((c) => c.request_id === payload.request_id).payload, payload);
  const exp = {
    request_id: randomUUID(),
    brief: 'Implement the project experiment.',
    recipe_id: 'agent_experiment_v1',
    project_id: saved.project_id,
  };
  assert.equal(
    (await req('/api/experiments', { method: 'POST', body: JSON.stringify(exp) })).status,
    409,
  );
  const project = {
    id: saved.project_id,
    name: payload.name,
    available: true,
    writable: true,
    non_empty: true,
    instructions: payload.instructions,
    revision: 1,
  };
  const result = {
    request_id: payload.request_id,
    project_id: saved.project_id,
    state: 'applied',
    error: null,
    project,
  };
  const publish = (result) =>
    req('/api/executor/project-results', {
      token: executor,
      method: 'POST',
      body: JSON.stringify({ schema_version: 1, results: [result] }),
    });
  for (let i = 0; i < 2; i++) assert.equal((await publish(result)).status, 200);
  assert.equal(
    (await publish({ ...result, project: { ...project, writable: false } })).status,
    409,
  );
  assert.equal((await publish({ ...result, project_id: 'wrong-project' })).status, 409);
  assert.equal(
    (await (await req(`/api/projects/requests/${payload.request_id}`)).json()).state,
    'applied',
  );
  assert.equal((await (await post(payload)).json()).state, 'applied');
  assert.deepEqual(
    (await (await req('/api/status')).json()).projects.find((p) => p.id === project.id),
    project,
  );
  assert.equal(
    (await req('/api/experiments', { method: 'POST', body: JSON.stringify(exp) })).status,
    202,
  );
  const pending = (await (await req('/api/executor/project-commands', { token: executor })).json())
    .commands;
  assert.equal(
    pending.some((c) => c.request_id === payload.request_id),
    false,
  );
  const fresh = {
    schema_version: 1,
    request_id: randomUUID(),
    name: 'New project',
    mode: 'new',
    allow_edits: true,
  };
  const newReceipt = await (await post(fresh)).json();
  const refusal = {
    request_id: fresh.request_id,
    project_id: newReceipt.project_id,
    state: 'rejected',
    error: 'Folder already exists; select it as existing',
    project: null,
  };
  assert.equal((await publish(refusal)).status, 200);
  assert.equal(
    (await (await req(`/api/projects/requests/${fresh.request_id}`)).json()).error,
    refusal.error,
  );
  assert.equal(
    (
      await req('/api/experiments', {
        method: 'POST',
        body: JSON.stringify({ ...exp, request_id: fresh.request_id }),
      })
    ).status,
    409,
  );
});

test('manual agent retry is durable, deduplicated and restricted to failed compute', async () => {
  const { result } = await create({
    recipe_id: 'agent_experiment_v1',
    brief: 'Measure a new method',
  });
  const eid = result.experiment_id;
  const endpoint = `/api/experiments/${eid}/actions`;
  const retry = { schema_version: 1, request_id: randomUUID(), action: 'retry_agent' };
  const post = (payload) => req(endpoint, { method: 'POST', body: JSON.stringify(payload) });
  assert.equal((await post(retry)).status, 409);
  const exp = await (await req(`/api/experiments/${eid}`)).json();
  Object.assign(exp, { phase: 'failed', hold_reason: 'compute_failed', snapshot: { plan: {} } });
  const db = await mf.getD1Database('DB');
  await db
    .prepare('UPDATE projections SET payload=? WHERE id=?')
    .bind(JSON.stringify(exp), eid)
    .run();
  assert.equal((await post(retry)).status, 202);
  assert.equal((await post(retry)).status, 202);
  exp.phase = 'preparing';
  await db
    .prepare('UPDATE projections SET payload=? WHERE id=?')
    .bind(JSON.stringify(exp), eid)
    .run();
  assert.equal((await post(retry)).status, 202);
  assert.equal((await post({ ...retry, request_id: randomUUID() })).status, 409);
  for (const [phase, hold_reason] of [
    ['interpreting', 'agent_session_unavailable'],
    ['failed', 'agent_recovery_exhausted'],
  ]) {
    Object.assign(exp, { phase, hold_reason });
    await db
      .prepare('UPDATE projections SET payload=? WHERE id=?')
      .bind(JSON.stringify(exp), eid)
      .run();
    assert.equal((await post({ ...retry, request_id: randomUUID() })).status, 202);
  }
  const commands = (await (await req('/api/executor/commands', { token: executor })).json())
    .commands;
  assert.equal(commands.filter((c) => c.request_id === retry.request_id).length, 1);
  assert.equal(
    commands.find((c) => c.request_id === retry.request_id).payload.action,
    'retry_agent',
  );
});

test('historical event replay is bounded, idempotent and cannot publish a state or claim readiness', async () => {
  const { result } = await create();
  const initial = await (await req(`/api/experiments/${result.experiment_id}`)).json();
  const view = {
    ...initial,
    revision: 600,
    phase: 'failed',
    hold_reason: 'compute_failed',
    error: 'saved failure',
  };
  const currentId = randomUUID();
  assert.equal(
    (
      await req('/api/executor/publish', {
        token: executor,
        method: 'POST',
        body: JSON.stringify({
          schema_version: 1,
          executor_id: 'acacia',
          messages: [{ schema_version: 1, event_id: currentId, experiment: view, inbox: [] }],
        }),
      })
    ).status,
    200,
  );
  const events = Array.from({ length: 500 }, (_, i) => ({
    event_id: randomUUID(),
    experiment_id: initial.id,
    revision: i + 1,
  }));
  const replay = (items, options = {}) =>
    req('/api/executor/events', {
      token: executor,
      method: 'POST',
      body: JSON.stringify({ schema_version: 1, executor_id: 'acacia', events: items }),
      ...options,
    });
  for (let i = 0; i < 2; i++) {
    const response = await replay(events);
    assert.equal(response.status, 200);
    assert.deepEqual(
      (await response.json()).accepted,
      events.map((e) => e.event_id),
    );
  }
  assert.equal((await replay(events, { token: user })).status, 401);
  assert.equal((await replay([...events, { ...events[0], event_id: randomUUID() }])).status, 422);
  assert.equal((await replay([events[0], events[0]])).status, 422);
  assert.equal((await replay([{ ...events[0], revision: 601 }])).status, 409);
  assert.equal((await replay([{ ...events[0], experiment_id: randomUUID() }])).status, 409);
  assert.equal((await replay([{ ...events[0], revision: 2 }])).status, 409);
  assert.equal(
    (await replay([{ ...events[0], experiment: { ...view, phase: 'complete' } }])).status,
    422,
  );
  const current = await (await req(`/api/experiments/${initial.id}`)).json();
  assert.equal(current.revision, 600);
  assert.equal(current.phase, 'failed');
  const db = await mf.getD1Database('DB');
  const saved = await db
    .prepare('SELECT COUNT(*) AS n FROM events WHERE experiment_id=?')
    .bind(initial.id)
    .first();
  assert.equal(saved.n, 501);
});

test('project memory has durable commands, immutable versions and private source-scoped publication', async () => {
  const base = '/api/projects/test-project/knowledge';
  const payload = {
    schema_version: 1,
    request_id: randomUUID(),
    kind: 'chat',
    text: 'What should we test next?',
  };
  assert.equal(
    (await req(base, { method: 'POST', body: JSON.stringify(payload), token: executor })).status,
    401,
  );
  assert.equal((await req('/api/executor/project-memory-commands')).status, 401);
  assert.equal((await req(base, { method: 'POST', body: JSON.stringify(payload) })).status, 202);
  assert.equal((await req(base, { method: 'POST', body: JSON.stringify(payload) })).status, 202);
  assert.equal(
    (await req(base, { method: 'POST', body: JSON.stringify({ ...payload, text: 'Changed' }) }))
      .status,
    409,
  );
  const waiting = await (await req(base)).json();
  assert.ok(
    waiting.messages.some(
      (m) => m.id === payload.request_id && m.hold_reason === 'waiting_for_executor',
    ),
  );
  const commands = await (
    await req('/api/executor/project-memory-commands', { token: executor })
  ).json();
  assert.equal(commands.commands.filter((c) => c.request_id === payload.request_id).length, 1);
  const ack = {
    schema_version: 1,
    results: [{ request_id: payload.request_id, state: 'accepted', error: null }],
  };
  for (let i = 0; i < 2; i++)
    assert.equal(
      (
        await req('/api/executor/project-memory-commands', {
          token: executor,
          method: 'POST',
          body: JSON.stringify(ack),
        })
      ).status,
      200,
    );
  const empty = {
    schema_version: 1,
    project_id: 'test-project',
    revision: 1,
    weekly: true,
    next_due: 2000000000,
    report: null,
    summaries: [],
    pending_experiment_ids: [],
    report_versions: [],
    messages: [],
  };
  const publish = async (value) =>
    req('/api/executor/project-memory', {
      token: executor,
      method: 'POST',
      body: JSON.stringify(value),
    });
  assert.equal((await publish(empty)).status, 200);
  assert.equal((await publish({ ...empty, weekly: false })).status, 409);
  const report = {
    project_id: 'test-project',
    version: 1,
    request_id: randomUUID(),
    document: {
      title: 'Working report',
      overview: 'Saved synthetic context',
      sections: [{ title: 'Open questions', body: 'No research claims.', experiment_ids: [] }],
      open_questions: ['What next?'],
    },
    sources: {},
    created_at: 1,
  };
  const next = { ...empty, revision: 2, report, report_versions: [1] };
  assert.equal((await publish(next)).status, 200);
  assert.deepEqual((await (await req(base + '/reports/1')).json()).report, report);
  assert.equal(
    (
      await publish({
        ...next,
        revision: 3,
        report: { ...report, document: { ...report.document, title: 'Overwritten' } },
      })
    ).status,
    409,
  );
  assert.equal(
    (await publish({ ...next, revision: 3, report: { ...report, project_id: 'foreign' } })).status,
    409,
  );
  assert.equal(
    (
      await publish({
        ...next,
        revision: 3,
        summaries: [
          {
            id: randomUUID(),
            title: 'Foreign',
            brief: 'Foreign',
            tldr: 'Unpublished',
            limitations: [],
            next_ideas: [],
            synthetic: true,
            evidence_revision: 1,
            report_version: 1,
            evidence: [
              {
                id: randomUUID(),
                name: 'results.json',
                sha256: 'b'.repeat(64),
                size: 42,
                media_type: 'application/json',
              },
            ],
            metrics: [{ name: 'accuracy', variant: 'baseline', value: 0.5, unit: '' }],
            metrics_truncated: false,
            fingerprint: 'a'.repeat(64),
          },
        ],
      })
    ).status,
    409,
  );
  const sourceId = randomUUID();
  const artifactId = randomUUID();
  const richReport = {
    ...report,
    version: 2,
    request_id: randomUUID(),
    document: {
      ...report.document,
      source_scope: 'synthetic',
      sections: [
        {
          title: 'Results',
          body: 'The comparison is illustrative.',
          experiment_ids: [sourceId],
          blocks: [
            { kind: 'paragraph', text: 'The synthetic run completed.' },
            {
              kind: 'table',
              caption: 'Saved result',
              columns: ['Variant', 'Accuracy'],
              rows: [
                {
                  cells: [
                    { text: 'Baseline' },
                    {
                      metric: {
                        experiment_id: sourceId,
                        name: 'accuracy',
                        variant: 'baseline',
                        decimals: 2,
                      },
                      display: '0.50',
                    },
                  ],
                  experiment_ids: [sourceId],
                },
              ],
            },
            {
              kind: 'figure',
              caption: 'Comparison plot',
              alt: 'A comparison plot',
              experiment_id: sourceId,
              artifact_id: artifactId,
            },
          ],
        },
      ],
    },
  };
  assert.equal(
    (await publish({ ...next, revision: 3, report: richReport, report_versions: [1, 2] })).status,
    200,
  );
  assert.deepEqual((await (await req(base + '/reports/2')).json()).report, richReport);
  assert.equal((await publish(empty)).status, 200); // stale delivery is acknowledged without regressing
  assert.equal((await (await req(base)).json()).revision, 3);
  const removal = {
    schema_version: 1,
    request_id: randomUUID(),
    kind: 'dismiss_idea',
    experiment_id: sourceId,
    text: 'Repeat with independent seeds.',
  };
  const postRemoval = (payload) => req(base, { method: 'POST', body: JSON.stringify(payload) });
  assert.equal((await postRemoval({ ...removal, experiment_id: undefined })).status, 422);
  assert.equal((await postRemoval({ ...removal, text: '' })).status, 422);
  assert.equal((await postRemoval(removal)).status, 202);
  assert.equal((await postRemoval(removal)).status, 202);
  assert.equal((await postRemoval({ ...removal, text: 'Changed' })).status, 409);
  const dismissed = [{ experiment_id: sourceId, text: removal.text }];
  assert.deepEqual((await (await req(base)).json()).dismissed_ideas, dismissed);
  const acknowledgeRemoval = (state) =>
    req('/api/executor/project-memory-commands', {
      token: executor,
      method: 'POST',
      body: JSON.stringify({
        schema_version: 1,
        results: [
          {
            request_id: removal.request_id,
            state,
            error: state === 'rejected' ? 'Idea no longer exists' : null,
          },
        ],
      }),
    });
  assert.equal((await acknowledgeRemoval('accepted')).status, 200);
  // Acknowledgement can arrive before its view: keep the exact idea hidden during that gap.
  assert.deepEqual((await (await req(base)).json()).dismissed_ideas, dismissed);
  assert.equal((await publish({ ...next, revision: 4, dismissed_ideas: dismissed })).status, 200);
  assert.deepEqual((await (await req(base)).json()).dismissed_ideas, dismissed);
  const rejected = { ...removal, request_id: randomUUID(), text: 'Another suggestion' };
  assert.equal((await postRemoval(rejected)).status, 202);
  assert.equal(
    (
      await req('/api/executor/project-memory-commands', {
        token: executor,
        method: 'POST',
        body: JSON.stringify({
          schema_version: 1,
          results: [
            { request_id: rejected.request_id, state: 'rejected', error: 'Idea no longer exists' },
          ],
        }),
      })
    ).status,
    200,
  );
  const afterRejection = await (await req(base)).json();
  assert.deepEqual(afterRejection.dismissed_ideas, dismissed);
  assert.ok(
    afterRejection.messages.some((m) => m.id === rejected.request_id && m.state === 'failed'),
  );
});

test('TensorBoard previews survive private D1 publication with bounded safe image sources', async () => {
  const { result } = await create();
  const initial = await (await req(`/api/experiments/${result.experiment_id}`)).json();
  const tensorboard = {
    scalar: {
      tag: 'loss/train',
      points: [
        [0, 2],
        [10, 0.4],
      ],
    },
    image: { tag: 'prediction', step: 10, wall_time: 123, src: 'data:image/jpeg;base64,YWJj' },
    updated_at: 123,
    limited: false,
  };
  const send = (preview) =>
    req('/api/executor/publish', {
      token: executor,
      method: 'POST',
      body: JSON.stringify({
        schema_version: 1,
        executor_id: 'acacia',
        messages: [
          {
            schema_version: 1,
            event_id: randomUUID(),
            inbox: [],
            experiment: {
              ...initial,
              revision: 1,
              jobs: [
                {
                  stage_id: 'fit',
                  state: 'submitted',
                  scheduler_state: 'RUNNING',
                  tensorboard: preview,
                },
              ],
            },
          },
        ],
      }),
    });
  for (const src of [
    'https://private-host/image',
    'data:image/svg+xml;base64,YWJj',
    'javascript:alert(1)',
  ]) {
    assert.equal(
      (await send({ ...tensorboard, image: { ...tensorboard.image, src } })).status,
      422,
    );
  }
  assert.equal(
    (
      await send({
        ...tensorboard,
        scalar: { tag: 'loss', points: Array.from({ length: 161 }, (_, i) => [i, 1]) },
      })
    ).status,
    422,
  );
  assert.equal((await send(tensorboard)).status, 200);
  const published = await (await req(`/api/experiments/${result.experiment_id}`)).json();
  assert.deepEqual(published.jobs[0].tensorboard, tensorboard);
  assert.equal(
    (await req(`/api/experiments/${result.experiment_id}`, { token: executor })).status,
    401,
  );
});

test('iteration metrics remain available through plot failure and recovery without final certificates', async () => {
  const { result } = await create({ recipe_id: 'agent_experiment_v1' });
  const eid = result.experiment_id;
  const initial = await (await req(`/api/experiments/${eid}`)).json();
  const producer = randomUUID(),
    plot = randomUUID();
  const send = async (exp) =>
    req('/api/executor/publish', {
      token: executor,
      method: 'POST',
      body: JSON.stringify({
        schema_version: 1,
        executor_id: 'acacia',
        messages: [{ schema_version: 1, event_id: randomUUID(), experiment: exp, inbox: [] }],
      }),
    });
  const exp = {
    ...initial,
    recipe_id: 'agent_experiment_v1',
    contract: { ...initial.contract, execution_contract: 3 },
    revision: 1,
    phase: 'evaluating',
    hold_reason: null,
    jobs: [
      {
        intent_id: producer,
        stage_id: 'evaluate',
        state: 'complete',
        scheduler_state: 'COMPLETED',
      },
      { intent_id: plot, stage_id: 'plot', state: 'submitted', scheduler_state: 'RUNNING' },
    ],
    partial_results: {
      tasks: {
        evaluate: {
          intent_id: producer,
          metric_count: 1,
          metrics_truncated: false,
          result: {
            schema_version: 1,
            metrics: [{ name: 'score', variant: 'synthetic', value: 42, unit: '' }],
          },
        },
      },
    },
    output_refs: [
      { id: randomUUID(), intent_id: producer, name: 'metrics.json', kind: 'file', created_at: 1 },
    ],
  };
  assert.equal((await send(exp)).status, 200);
  let saved = await (await req(`/api/experiments/${eid}`)).json();
  assert.equal(saved.partial_results.tasks.evaluate.result.metrics[0].value, 42);
  assert.equal(
    (
      await send({
        ...exp,
        revision: 2,
        phase: 'failed',
        hold_reason: 'compute_failed',
        jobs: [exp.jobs[0], { ...exp.jobs[1], state: 'failed', scheduler_state: 'FAILED' }],
      })
    ).status,
    200,
  );
  saved = await (await req(`/api/experiments/${eid}`)).json();
  assert.equal(saved.partial_results.tasks.evaluate.intent_id, producer);
  assert.equal(
    (
      await send({
        ...exp,
        revision: 3,
        jobs: [
          exp.jobs[0],
          {
            ...exp.jobs[1],
            intent_id: randomUUID(),
            state: 'complete',
            scheduler_state: 'COMPLETED',
          },
        ],
      })
    ).status,
    200,
  );
  const wrong = structuredClone(exp);
  wrong.revision = 4;
  wrong.partial_results.tasks.evaluate.intent_id = randomUUID();
  assert.equal((await send(wrong)).status, 422);
  const privatePath = structuredClone(exp);
  privatePath.revision = 4;
  privatePath.output_refs[0].path = '/private/research';
  assert.equal((await send(privatePath)).status, 422);
});

test('contract 3 repair artifacts retain duplicate filenames with distinct immutable IDs', async () => {
  const { result } = await create({ recipe_id: 'agent_experiment_v1' });
  const initial = await (await req(`/api/experiments/${result.experiment_id}`)).json();
  const artifacts = [0, 1].map(() => ({
    id: randomUUID(),
    name: 'preparation.json',
    sha256: 'a'.repeat(64),
    size: 2,
    media_type: 'application/json',
    local_valid: 1,
    remote_published: 0,
  }));
  const exp = {
    ...initial,
    revision: 1,
    phase: 'training',
    hold_reason: null,
    contract: { ...initial.contract, execution_contract: 3 },
    artifacts,
  };
  const send = (experiment) =>
    req('/api/executor/publish', {
      token: executor,
      method: 'POST',
      body: JSON.stringify({
        schema_version: 1,
        executor_id: 'acacia',
        messages: [{ schema_version: 1, event_id: randomUUID(), experiment, inbox: [] }],
      }),
    });
  assert.equal((await send(exp)).status, 200);
  const saved = await (await req(`/api/experiments/${exp.id}`)).json();
  assert.deepEqual(
    saved.artifacts.map((a) => a.id),
    artifacts.map((a) => a.id),
  );
  assert.equal(
    (await send({ ...exp, revision: 2, artifacts: [artifacts[0], artifacts[0]] })).status,
    422,
  );
  assert.equal(
    (
      await send({
        ...exp,
        revision: 2,
        artifacts: [{ ...artifacts[0], sha256: 'b'.repeat(64) }, artifacts[1]],
      })
    ).status,
    409,
  );
  assert.equal(
    (
      await send({
        ...exp,
        revision: 2,
        contract: {
          ...exp.contract,
          execution_contract: 2,
        },
      })
    ).status,
    422,
  );
});

test('maintenance authorization, immutable receipts, stale progress and credential roles', async () => {
  const releaseId = 'a'.repeat(40);
  const state = {
    current_commit: 'b'.repeat(40),
    available: true,
    release: {
      commit_id: releaseId,
      summary: 'Synthetic tested release',
      state: 'ready',
      error: null,
    },
    operation: null,
    blockers: [],
    running_jobs: 3,
    observed_at: Date.now() / 1000,
  };
  assert.equal((await req('/api/maintenance', { token: executor })).status, 401);
  assert.equal(
    (await req('/api/executor/maintenance', { method: 'POST', body: JSON.stringify(state) }))
      .status,
    401,
  );
  assert.equal(
    (
      await req('/api/executor/maintenance', {
        token: executor,
        method: 'POST',
        body: JSON.stringify(state),
      })
    ).status,
    200,
  );
  const payload = { schema_version: 1, request_id: randomUUID(), release_id: releaseId };
  assert.equal(
    (
      await req('/api/maintenance/install', {
        method: 'POST',
        body: JSON.stringify({ ...payload, shell: 'forbidden' }),
      })
    ).status,
    422,
  );
  const send = () =>
    req('/api/maintenance/install', { method: 'POST', body: JSON.stringify(payload) });
  assert.equal((await send()).status, 202);
  assert.equal((await send()).status, 202);
  assert.equal(
    (
      await req('/api/maintenance/install', {
        method: 'POST',
        body: JSON.stringify({ ...payload, release_id: 'c'.repeat(40) }),
      })
    ).status,
    409,
  );
  assert.equal(
    (
      await req('/api/maintenance/install', {
        method: 'POST',
        body: JSON.stringify({ ...payload, request_id: randomUUID() }),
      })
    ).status,
    409,
  );
  const commands = await (
    await req('/api/executor/maintenance-commands', { token: executor })
  ).json();
  assert.deepEqual(commands.commands, [{ request_id: payload.request_id, release_id: releaseId }]);
  const receipt = { ...payload, state: 'accepted', error: null };
  for (let i = 0; i < 2; i++)
    assert.equal(
      (
        await req('/api/executor/maintenance-commands', {
          token: executor,
          method: 'POST',
          body: JSON.stringify(receipt),
        })
      ).status,
      200,
    );
  assert.equal(
    (
      await req('/api/executor/maintenance-commands', {
        token: executor,
        method: 'POST',
        body: JSON.stringify({ ...receipt, state: 'rejected', error: 'changed' }),
      })
    ).status,
    409,
  );
  const installing = {
    ...state,
    observed_at: state.observed_at + 2,
    operation: {
      request_id: payload.request_id,
      release_id: releaseId,
      state: 'backing_up',
      error: null,
      updated_at: state.observed_at + 2,
    },
  };
  assert.equal(
    (
      await req('/api/executor/maintenance', {
        token: executor,
        method: 'POST',
        body: JSON.stringify(installing),
      })
    ).status,
    200,
  );
  await req('/api/executor/maintenance', {
    token: executor,
    method: 'POST',
    body: JSON.stringify(state),
  });
  const latest = await (await req('/api/maintenance')).json();
  assert.equal(latest.operation.state, 'backing_up');
  assert.equal(latest.pending, null);
  assert.equal(
    (
      await req('/api/maintenance/install', {
        method: 'POST',
        body: JSON.stringify({ ...payload, request_id: randomUUID() }),
      })
    ).status,
    409,
  );
});

test('cancellation is durable in every phase and requires a capable executor', async () => {
  const db = await mf.getD1Database('DB');
  const prior = await db.prepare('SELECT * FROM executor_status WHERE id=?').bind('acacia').first();
  try {
    await db.prepare('DELETE FROM executor_status WHERE id=?').bind('acacia').run();
    const { result } = await create();
    const eid = result.experiment_id;
    const post = (payload) =>
      req(`/api/experiments/${eid}/actions`, { method: 'POST', body: JSON.stringify(payload) });
    assert.equal(
      (await post({ schema_version: 1, request_id: randomUUID(), action: 'cancel' })).status,
      409,
    );
    await db
      .prepare('INSERT INTO executor_status(id,payload,received_at) VALUES(?,?,?)')
      .bind('acacia', JSON.stringify({ experiment_cancellation: true }), Date.now() / 1000)
      .run();
    const exp = await (await req(`/api/experiments/${eid}`)).json();
    for (const phase of [
      'waiting_for_executor',
      'preparing',
      'training',
      'evaluating',
      'validating',
      'interpreting',
      'publishing',
      'failed',
      'cancelled',
    ]) {
      exp.phase = phase;
      await db
        .prepare('UPDATE projections SET payload=? WHERE id=?')
        .bind(JSON.stringify(exp), eid)
        .run();
      const payload = { schema_version: 1, request_id: randomUUID(), action: 'cancel' };
      assert.equal((await post(payload)).status, 202, phase);
      assert.equal((await post(payload)).status, 202, `${phase} redelivery`);
      const saved = await db
        .prepare('SELECT kind,payload FROM commands WHERE request_id=?')
        .bind(payload.request_id)
        .first();
      assert.equal(saved.kind, 'action');
      assert.equal(JSON.parse(saved.payload).action, 'cancel');
    }
    exp.phase = 'complete';
    await db
      .prepare('UPDATE projections SET payload=? WHERE id=?')
      .bind(JSON.stringify(exp), eid)
      .run();
    assert.equal(
      (await post({ schema_version: 1, request_id: randomUUID(), action: 'cancel' })).status,
      409,
    );
  } finally {
    await db.prepare('DELETE FROM executor_status WHERE id=?').bind('acacia').run();
    if (prior)
      await db
        .prepare('INSERT INTO executor_status(id,payload,received_at) VALUES(?,?,?)')
        .bind(prior.id, prior.payload, prior.received_at)
        .run();
  }
});
