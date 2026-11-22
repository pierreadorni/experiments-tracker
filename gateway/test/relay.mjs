import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { readFile, readdir, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { randomUUID, createHash } from 'node:crypto';
import { Miniflare } from 'miniflare';

let mf, directory;
const user = 'u'.repeat(40),
  machine = 'e'.repeat(40),
  X = '/api/executor/relay';
const call = (url, method, body, token, headers = {}) =>
  mf.dispatchFetch('https://relay.test' + url, {
    method,
    headers: { Authorization: `Bearer ${token}`, ...headers },
    ...(body === undefined
      ? {}
      : {
          body:
            typeof body === 'string' || body instanceof Uint8Array ? body : JSON.stringify(body),
        }),
  });
const exec = (url, method = 'GET', body, headers = {}) =>
  call(url, method, body, machine, { 'x-executor-id': 'acacia', ...headers });
const browser = (url, method = 'GET', body) => call(url, method, body, user);
const sha = (bytes) => createHash('sha256').update(bytes).digest('hex');
const experiment = () => `experiment:${randomUUID()}`;
const put = (k, revision, document, extra = {}) =>
  exec(`${X}/projections/${k}`, 'PUT', { schema_version: 1, key: k, revision, document, ...extra });
const blob = (id, bytes, owner, headers = {}) =>
  exec(`${X}/blobs/${id}`, 'PUT', bytes, {
    'x-sha256': sha(bytes),
    'x-owner-key': owner,
    'content-type': 'text/plain',
    ...headers,
  });
const command = (body = {}) => ({ request_id: randomUUID(), kind: 'pause_run', ...body });

before(async () => {
  directory = await mkdtemp(path.join(tmpdir(), 'et-relay-'));
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
    d1Databases: { DB: 'test-relay' },
    r2Buckets: { BUCKET: 'test-relay' },
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

test('credentials do not cross roles', async () => {
  const k = experiment();
  assert.equal(
    (await call(`${X}/revisions`, 'GET', undefined, user, { 'x-executor-id': 'acacia' })).status,
    401,
  );
  assert.equal((await call(`/api/projections/${k}`, 'GET', undefined, machine)).status, 401);
  assert.equal((await call('/api/commands', 'POST', command(), machine)).status, 401);
  assert.equal((await call('/api/blobs/x', 'GET', undefined, machine)).status, 401);
  assert.equal(
    (await call(`${X}/revisions`, 'GET', undefined, 'bad', { 'x-executor-id': 'acacia' })).status,
    401,
  );
});

test('executor identity mismatch is 403', async () => {
  const k = experiment();
  for (const headers of [{ 'x-executor-id': 'jean-zay' }, {}]) {
    const r = await call(
      `${X}/projections/${k}`,
      'PUT',
      { revision: 1, document: {} },
      machine,
      headers,
    );
    assert.equal(r.status, 403);
  }
  assert.equal((await browser(`/api/projections/${k}`)).status, 404);
});

test('revisions are monotonic', async () => {
  const k = experiment();
  assert.equal((await put(k, 5, { a: 1 })).status, 200);
  assert.equal((await put(k, 5, { a: 1 })).status, 200, 'equal and identical');
  assert.equal((await put(k, 5, { a: 2 })).status, 409, 'equal and different');
  assert.equal((await put(k, 4, { a: 1 })).status, 409, 'older');
  assert.equal((await put(k, 6, { a: 3 })).status, 200, 'newer');
  const got = await (await browser(`/api/projections/${k}`)).json();
  assert.deepEqual([got.key, got.revision, got.document], [k, 6, { a: 3 }]);
  const manifest = await (await exec(`${X}/revisions`)).json();
  assert.equal(manifest.revisions[k][0], 6);
  assert.equal((await put('bad key', 1, {})).status, 400);
});

test('list honors prefix, before and limit', async () => {
  const p = experiment();
  await put(p, 1, { n: 1 });
  const listed = await (await browser('/api/projections?prefix=experiment:&limit=100')).json();
  assert.ok(listed.items.some((i) => i.key === p));
  const one = await (await browser('/api/projections?prefix=experiment:&limit=1')).json();
  assert.equal(one.items.length, 1);
  const none = await (await browser('/api/projections?prefix=knowledge:')).json();
  assert.equal(none.items.length, 0);
});

test('size caps return 413', async () => {
  const k = experiment();
  const big = 'x'.repeat(1024 * 1024);
  assert.equal((await put(k, 1, { big })).status, 413);
  assert.equal(
    (await browser('/api/commands', 'POST', command({ payload: { t: 'x'.repeat(70000) } }))).status,
    413,
  );
  const bytes = new Uint8Array(10 * 1024 * 1024 + 1);
  assert.equal((await blob('toobig', bytes, k)).status, 413);
});

test('tombstone hides the key and its blobs', async () => {
  const k = experiment(),
    bytes = new TextEncoder().encode('figure bytes');
  await put(k, 1, { ok: true });
  assert.equal((await blob('fig-1', bytes, k)).status, 200);
  const served = await browser('/api/blobs/fig-1');
  assert.equal(served.status, 200);
  assert.equal(new Uint8Array(await served.arrayBuffer()).length, bytes.length);
  assert.match(served.headers.get('content-security-policy'), /sandbox/);
  assert.equal((await put(k, 2, null)).status, 200);
  assert.equal((await browser(`/api/projections/${k}`)).status, 404);
  assert.equal((await browser('/api/blobs/fig-1')).status, 404);
  const listed = await (await browser('/api/projections?prefix=experiment:&limit=100')).json();
  assert.ok(!listed.items.some((i) => i.key === k));
  assert.equal((await put(k, 1, { ok: true })).status, 409, 'older than tombstone');
  assert.equal((await exec(`${X}/revisions`).then((r) => r.json())).revisions[k][0], 2);
});

test('blobs check hash, size and immutability', async () => {
  const k = experiment(),
    bytes = new TextEncoder().encode('abc');
  assert.equal((await blob('b1', bytes, k, { 'x-sha256': sha('other') })).status, 409);
  assert.equal((await blob('b1', bytes, k, { 'x-size': '99' })).status, 409);
  assert.equal((await blob('b1', bytes, k, { 'x-sha256': 'zz' })).status, 400);
  assert.equal((await blob('b1', bytes, k)).status, 200);
  assert.equal((await blob('b1', bytes, k)).status, 200, 'same bytes');
  assert.equal(
    (await blob('b1', new TextEncoder().encode('abd'), k)).status,
    409,
    'id is immutable',
  );
});

test('command replay, conflict and terminal receipts', async () => {
  const body = command({ target: experiment(), payload: { n: 1 } });
  const first = await browser('/api/commands', 'POST', body);
  assert.equal(first.status, 200);
  const receipt = await first.json();
  assert.equal(receipt.state, 'pending');
  const again = await browser('/api/commands', 'POST', body);
  assert.deepEqual(await again.json(), receipt);
  assert.equal(
    (await browser('/api/commands', 'POST', { ...body, payload: { n: 2 } })).status,
    409,
  );
  assert.equal((await browser('/api/commands', 'POST', command({ kind: 'Bad Kind' }))).status, 400);
  const queue = await (await exec(`${X}/commands`)).json();
  const mine = queue.commands.find((c) => c.request_id === body.request_id);
  assert.deepEqual([mine.kind, mine.target, mine.payload], [body.kind, body.target, body.payload]);
  const pending = await (await browser('/api/commands?state=pending')).json();
  assert.ok(pending.commands.some((c) => c.request_id === body.request_id));

  const settle = (extra) =>
    exec(`${X}/receipts`, 'POST', { request_id: body.request_id, ...extra });
  assert.equal((await settle({ state: 'applied', result: { done: true } })).status, 200);
  assert.equal(
    (await settle({ state: 'applied', result: { done: true } })).status,
    200,
    'identical repeat',
  );
  assert.equal((await settle({ state: 'rejected', error: 'no' })).status, 409);
  assert.equal((await settle({ state: 'applied', result: { done: false } })).status, 409);
  const done = await (await browser(`/api/commands/${body.request_id}`)).json();
  assert.deepEqual([done.state, done.result], ['applied', { done: true }]);
  assert.equal((await browser('/api/commands', 'POST', body)).status, 200, 'replay after settle');
  assert.equal(
    (await exec(`${X}/receipts`, 'POST', { request_id: randomUUID(), state: 'applied' })).status,
    404,
  );
  assert.equal(
    (await browser(`/api/commands/${randomUUID()}`)).status,
    404,
    'falls through to the old route',
  );
  const left = await (await exec(`${X}/commands`)).json();
  assert.ok(!left.commands.some((c) => c.request_id === body.request_id));
});

test('concurrent identical posts store one row', async () => {
  const body = command();
  const responses = await Promise.all(
    Array.from({ length: 8 }, () => browser('/api/commands', 'POST', body)),
  );
  assert.ok(responses.every((r) => r.status === 200));
  const db = await mf.getD1Database('DB');
  const row = await db
    .prepare('SELECT COUNT(*) AS n FROM relay_commands WHERE request_id=?')
    .bind(body.request_id)
    .first();
  assert.equal(row.n, 1);
});

test('more than 500 pending commands returns 429', async () => {
  const db = await mf.getD1Database('DB');
  await db.prepare("UPDATE relay_commands SET state='applied' WHERE state='pending'").run();
  await db
    .prepare(
      "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<500) INSERT INTO relay_commands(request_id,executor_id,kind,payload,state,received_at) SELECT 'fill-'||i,'acacia','fill','{}','pending',1 FROM n",
    )
    .run();
  assert.equal((await browser('/api/commands', 'POST', command())).status, 429);
  await db
    .prepare("UPDATE relay_commands SET state='applied' WHERE request_id LIKE 'fill-%'")
    .run();
  assert.equal((await browser('/api/commands', 'POST', command())).status, 200);
});
