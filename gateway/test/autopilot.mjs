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
