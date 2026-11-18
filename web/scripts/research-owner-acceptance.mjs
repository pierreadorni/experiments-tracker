// Start the temporary real-API fixture, run headless browser acceptance, then remove state.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const scripts = path.dirname(fileURLToPath(import.meta.url));
const repo = path.resolve(scripts, '../..');
async function freeLoopbackPort() {
  const listener = net.createServer();
  await new Promise((resolve, reject) => {
    listener.once('error', reject);
    listener.listen(0, '127.0.0.1', resolve);
  });
  const { port } = listener.address();
  await new Promise((resolve, reject) =>
    listener.close((error) => (error ? reject(error) : resolve())),
  );
  return port;
}
const port = Number(process.env.ET_C4_PORT || (await freeLoopbackPort()));
const root = await mkdtemp(path.join(os.tmpdir(), 'research-owner-fixture-'));
const output = process.env.ET_C4_OUTPUT || '/tmp/research-owner-acceptance';
const server = spawn(
  'uv',
  [
    'run',
    '--locked',
    'python',
    'web/scripts/research-owner-fixture.py',
    '--root',
    root,
    '--port',
    String(port),
  ],
  { cwd: repo, stdio: 'inherit' },
);
try {
  let ready = false;
  for (let attempt = 0; attempt < 300; attempt += 1) {
    if (server.exitCode !== null)
      throw new Error(`Temporary FastAPI fixture exited (${server.exitCode})`);
    try {
      const response = await fetch(`http://127.0.0.1:${port}/healthz`);
      if (response.status === 200 || response.status === 503) {
        ready = true;
        break;
      }
    } catch {
      /* Wait for the loopback listener to start. */
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  assert.ok(ready, 'Temporary FastAPI fixture did not become healthy');
  const browser = spawn(process.execPath, ['web/scripts/research-owner-smoke.mjs'], {
    cwd: repo,
    stdio: 'inherit',
    env: {
      ...process.env,
      ET_C4_FIXTURE_ROOT: root,
      ET_UI_URL: `http://127.0.0.1:${port}`,
      ET_C4_OUTPUT: output,
    },
  });
  const exit = await new Promise((resolve, reject) => {
    browser.once('error', reject);
    browser.once('exit', (code) => resolve(code));
  });
  assert.equal(exit, 0, 'Headless C4 browser flow failed');
} finally {
  server.kill('SIGTERM');
  await new Promise((resolve) => {
    if (server.exitCode !== null) resolve();
    else server.once('exit', resolve);
    setTimeout(resolve, 5000).unref();
  });
  await rm(root, { recursive: true, force: true });
}
