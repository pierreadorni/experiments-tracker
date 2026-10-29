import { Miniflare } from 'miniflare';
import { readFile, mkdir, readdir } from 'node:fs/promises';
import path from 'node:path';

const root = path.resolve(import.meta.dirname, '..');
const state = process.env.ET_GATEWAY_STATE || '/tmp/experimenttracker-gateway-local';
await mkdir(state, { recursive: true });
const userToken = (await readFile(path.join(root, '../.local/api-token'), 'utf8')).trim();
const executorToken = (await readFile(path.join(root, '../.local/executor-token'), 'utf8')).trim();
const mf = new Miniflare({
  modules: true,
  scriptPath: path.join(root, 'dist/server/index.js'),
  compatibilityDate: '2025-09-01',
  host: '127.0.0.1',
  port: 8788,
  bindings: {
    AUTH_MODE: 'token',
    USER_TOKEN: userToken,
    EXECUTOR_TOKEN: executorToken,
    EXECUTOR_ID: 'acacia',
  },
  d1Databases: { DB: 'et-local-db' },
  r2Buckets: { BUCKET: 'et-local-bucket' },
  d1Persist: path.join(state, 'd1'),
  r2Persist: path.join(state, 'r2'),
});
const db = await mf.getD1Database('DB');
const schema = await db
  .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='projections'")
  .first();
// Local Miniflare only; Sites applies production migrations from the archive.
await db.prepare('CREATE TABLE IF NOT EXISTS et_dev_migrations(name TEXT PRIMARY KEY)').run();
const migrations = (await readdir(path.join(root, 'drizzle')))
  .filter((name) => name.endsWith('.sql'))
  .sort();
for (const [index, name] of migrations.entries()) {
  if (await db.prepare('SELECT name FROM et_dev_migrations WHERE name=?').bind(name).first())
    continue;
  const statements = [];
  if (!(index === 0 && schema)) {
    const sql = await readFile(path.join(root, 'drizzle', name), 'utf8');
    for (const stmt of sql
      .split('--> statement-breakpoint')
      .map((v) => v.trim())
      .filter(Boolean))
      statements.push(db.prepare(stmt));
  }
  statements.push(db.prepare('INSERT INTO et_dev_migrations(name) VALUES(?)').bind(name));
  await db.batch(statements);
}
console.log(`Local gateway ready at ${await mf.ready}; fake platform storage, loopback only.`);
let stopping = false;
for (const signal of ['SIGINT', 'SIGTERM'])
  process.on(signal, async () => {
    if (stopping) return;
    stopping = true;
    await mf.dispose();
    process.exit(0);
  });
