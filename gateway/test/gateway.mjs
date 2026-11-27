import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { readFile, mkdtemp, rm, readdir } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
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
  assert.equal((await req('/api/projections', { token: executor })).status, 401);
  assert.equal((await req('/api/executor/relay/commands', { token: user })).status, 401);
  assert.equal((await req('/api/projections', { token: 'invalid' })).status, 401);
});
test('the old executor and browser routes are gone', async () => {
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
    ['GET', '/api/experiments'],
    ['POST', '/api/experiments'],
    ['GET', '/api/status'],
    ['GET', '/api/inbox'],
    ['GET', '/api/maintenance'],
    ['POST', '/api/projects'],
    ['GET', '/api/projects/demo/knowledge'],
    ['GET', '/api/autopilot'],
  ])
    assert.equal(
      (
        await req(path, {
          token: path.startsWith('/api/executor/') ? executor : user,
          method,
          body: method === 'GET' ? undefined : '{}',
        })
      ).status,
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
      sites.dispatchFetch('https://gateway.test/api/commands', {
        method: 'POST',
        headers: {
          ...headers(executor),
          'oai-authenticated-user-id': 'test-dispatch-identity',
          Origin: origin,
        },
        body: '{}',
      });
    assert.equal((await request('https://elsewhere.test')).status, 403);
    assert.equal((await request('https://gateway.test')).status, 400); // authenticated, then an invalid command
    assert.equal(
      (
        await sites.dispatchFetch('https://gateway.test/api/commands', {
          headers: headers(executor),
        })
      ).status,
      401,
    );
  } finally {
    await sites.dispose();
  }
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
