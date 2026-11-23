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
    recipe_id: 'agent_experiment_v1',
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
test('the old executor routes are gone; only the relay serves executors', async () => {
  for (const [method, path] of [
    ['POST', '/api/executor/publish'],
    ['POST', '/api/executor/events'],
    ['GET', '/api/executor/commands'],
    ['POST', '/api/executor/ack'],
    ['POST', '/api/executor/reject'],
    ['PUT', `/api/executor/artifacts/${randomUUID()}`],
    ['GET', '/api/executor/project-commands'],
    ['POST', '/api/executor/project-results'],
    ['GET', '/api/executor/project-settings-commands'],
    ['POST', '/api/executor/project-settings-results'],
    ['GET', '/api/executor/project-memory-commands'],
    ['POST', '/api/executor/project-memory'],
    ['GET', '/api/executor/autopilot-commands'],
    ['POST', '/api/executor/autopilot'],
    ['POST', '/api/executor/autopilot-documents'],
    ['POST', `/api/executor/autopilot-files/${randomUUID()}`],
    ['GET', '/api/executor/maintenance-commands'],
    ['POST', '/api/executor/maintenance'],
  ])
    assert.equal(
      (await req(path, { token: executor, method, body: method === 'GET' ? undefined : '{}' }))
        .status,
      404,
      `${method} ${path}`,
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
test('retired toy recipes cannot create projections or commands', async () => {
  const db = await mf.getD1Database('DB');
  const before = {
    commands: (await db.prepare('SELECT COUNT(*) AS n FROM commands').first()).n,
    projections: (await db.prepare('SELECT COUNT(*) AS n FROM projections').first()).n,
  };
  const response = await req('/api/experiments', {
    method: 'POST',
    body: JSON.stringify({
      schema_version: 1,
      request_id: randomUUID(),
      brief: 'Try the retired fixed synthetic comparison.',
      recipe_id: 'toy_compare_v1',
    }),
  });
  assert.equal(response.status, 422);
  assert.deepEqual(
    {
      commands: (await db.prepare('SELECT COUNT(*) AS n FROM commands').first()).n,
      projections: (await db.prepare('SELECT COUNT(*) AS n FROM projections').first()).n,
    },
    before,
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
    ['plan', 'prepare', 'interpret', 'publish'],
  );
  const duplicate = await req('/api/experiments', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
  assert.equal((await duplicate.json()).experiment_id, result.experiment_id);
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

test('frozen terminal records reject execution mutations', async () => {
  const { result } = await create({ brief: 'Historical frozen record' });
  const eid = result.experiment_id;
  const db = await mf.getD1Database('DB');
  const exp = await (await req(`/api/experiments/${eid}`)).json();
  Object.assign(exp, {
    phase: 'failed',
    hold_reason: 'agent_recovery_exhausted',
    contract: { ...exp.contract, execution_contract: 2 },
    plan_review: { state: 'awaiting_review', revision: 1 },
  });
  await db
    .prepare('UPDATE projections SET payload=? WHERE id=?')
    .bind(JSON.stringify(exp), eid)
    .run();

  for (const [path, payload] of [
    [`/api/experiments/${eid}/actions`, { action: 'summarize_title' }],
    [`/api/experiments/${eid}/steering`, { text: 'continue', mode: 'later' }],
  ])
    assert.equal(
      (
        await req(path, {
          method: 'POST',
          body: JSON.stringify({ ...payload, schema_version: 1, request_id: randomUUID() }),
        })
      ).status,
      409,
    );

  assert.equal(
    await db
      .prepare("SELECT COUNT(*) AS n FROM commands WHERE experiment_id=? AND kind!='create'")
      .bind(eid)
      .first('n'),
    0,
  );
});

test('active archived commands stay cancel-only', async () => {
  const { result } = await create({ brief: 'Active historical record' });
  const eid = result.experiment_id;
  const db = await mf.getD1Database('DB');
  const exp = await (await req(`/api/experiments/${eid}`)).json();
  Object.assign(exp, {
    phase: 'training',
    contract: { ...exp.contract, execution_contract: 2 },
    plan_review: { state: 'awaiting_review', revision: 1 },
    pending_request: {
      id: 'pending',
      method: 'item/commandExecution/requestApproval',
      params: {},
      expired: false,
    },
  });
  await db
    .prepare('UPDATE projections SET payload=? WHERE id=?')
    .bind(JSON.stringify(exp), eid)
    .run();
  for (const [path, payload] of [
    [`/api/experiments/${eid}/actions`, { action: 'pause_agent' }],
    [`/api/experiments/${eid}/actions`, { action: 'pause_new_compute' }],
    [`/api/experiments/${eid}/answers`, { pending_request_id: 'pending', answer: 'decline' }],
  ])
    assert.equal(
      (
        await req(path, {
          method: 'POST',
          body: JSON.stringify({ ...payload, schema_version: 1, request_id: randomUUID() }),
        })
      ).status,
      409,
    );
  assert.equal(
    await db
      .prepare("SELECT COUNT(*) AS n FROM commands WHERE experiment_id=? AND kind!='create'")
      .bind(eid)
      .first('n'),
    0,
  );

  const owner = await db
    .prepare('SELECT payload,received_at FROM executor_status WHERE id=?')
    .bind('acacia')
    .first();
  await db
    .prepare(
      'INSERT INTO executor_status(id,payload,received_at) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload,received_at=excluded.received_at',
    )
    .bind('acacia', JSON.stringify({ experiment_cancellation: true }), Date.now() / 1000)
    .run();
  const cancelled = await req(`/api/experiments/${eid}/actions`, {
    method: 'POST',
    body: JSON.stringify({ schema_version: 1, request_id: randomUUID(), action: 'cancel' }),
  });
  assert.equal(cancelled.status, 202);
  assert.equal(
    await db
      .prepare("SELECT kind FROM commands WHERE experiment_id=? AND kind!='create'")
      .bind(eid)
      .first('kind'),
    'action',
  );
  if (owner)
    await db
      .prepare('UPDATE executor_status SET payload=?,received_at=? WHERE id=?')
      .bind(owner.payload, owner.received_at, 'acacia')
      .run();
  else await db.prepare('DELETE FROM executor_status WHERE id=?').bind('acacia').run();
});

test('executor archive policy blocks fresh contract-three mutations and preserves receipts', async () => {
  const { result } = await create({ brief: 'Contract three archive transition' });
  const eid = result.experiment_id;
  const db = await mf.getD1Database('DB');
  const exp = await (await req(`/api/experiments/${eid}`)).json();
  exp.contract.execution_contract = 3;
  exp.phase = 'training';
  const save = () =>
    db.prepare('UPDATE projections SET payload=? WHERE id=?').bind(JSON.stringify(exp), eid).run();
  await save();
  const post = (path, body) =>
    req(`/api/experiments/${eid}/${path}`, {
      method: 'POST',
      body: JSON.stringify({ schema_version: 1, request_id: randomUUID(), ...body }),
    });
  const pause = { action: 'pause_agent', request_id: randomUUID() };
  const earlier = await post('actions', pause);
  assert.equal(earlier.status, 202);
  const receipt = await earlier.json();
  const status = await db
    .prepare('SELECT * FROM executor_status WHERE id=?')
    .bind('acacia')
    .first();
  await db
    .prepare('INSERT OR REPLACE INTO executor_status VALUES(?,?,?)')
    .bind(
      'acacia',
      JSON.stringify({ execution_contract_3_archived: true, experiment_cancellation: true }),
      Date.now() / 1000,
    )
    .run();

  try {
    const replay = await post('actions', pause);
    assert.equal(replay.status, 202);
    assert.deepEqual(await replay.json(), receipt);
    assert.equal((await post('actions', { action: 'resume_agent' })).status, 409);
    assert.equal((await post('actions', { action: 'cancel' })).status, 202);
    exp.phase = 'complete';
    await save();
    assert.equal((await post('actions', { action: 'summarize_title' })).status, 409);
    assert.equal((await post('integrate', {})).status, 409);
    const action = { id: randomUUID(), phase: 'discuss:results', state: 'running' };
    exp.agent_actions = [action];
    exp.pending_request = {
      id: 'discussion-request',
      action_id: action.id,
      method: 'item/tool/requestUserInput',
      params: {},
      expired: false,
    };
    await save();
    assert.equal(
      (await post('answers', { pending_request_id: 'discussion-request', answer: 'Yes' })).status,
      202,
    );
    const view = await (await req(`/api/experiments/${eid}`)).json();
    assert.equal(view.contract.execution_contract, 3);
    assert.equal(view.phase, 'complete');
  } finally {
    if (status)
      await db
        .prepare('INSERT OR REPLACE INTO executor_status VALUES(?,?,?)')
        .bind(status.id, status.payload, status.received_at)
        .run();
    else await db.prepare('DELETE FROM executor_status WHERE id=?').bind('acacia').run();
  }
});

test('frozen completed replies require an exact saved discussion action', async () => {
  const db = await mf.getD1Database('DB');
  for (const binding of ['valid', 'unbound', 'expired']) {
    const { result } = await create({ brief: `Historical reader ${binding}` });
    const eid = result.experiment_id;
    const exp = await (await req(`/api/experiments/${eid}`)).json();
    const actionId = randomUUID();
    Object.assign(exp, {
      phase: 'complete',
      contract: { ...exp.contract, execution_contract: 2 },
      pending_request: {
        id: 'pending',
        ...(binding === 'unbound' ? {} : { action_id: actionId }),
        method: 'item/commandExecution/requestApproval',
        params: {},
        expired: binding === 'expired',
      },
      agent_actions: [{ id: actionId, phase: 'discuss:discussion-id', state: 'running' }],
    });
    await db
      .prepare('UPDATE projections SET payload=? WHERE id=?')
      .bind(JSON.stringify(exp), eid)
      .run();
    const response = await req(`/api/experiments/${eid}/answers`, {
      method: 'POST',
      body: JSON.stringify({
        schema_version: 1,
        request_id: randomUUID(),
        pending_request_id: 'pending',
        answer: 'decline',
      }),
    });
    assert.equal(response.status, binding === 'valid' ? 202 : 409);
    assert.equal(
      await db
        .prepare("SELECT COUNT(*) AS n FROM commands WHERE experiment_id=? AND kind!='create'")
        .bind(eid)
        .first('n'),
      binding === 'valid' ? 1 : 0,
    );
  }
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
