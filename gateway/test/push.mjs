import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import {
  randomUUID,
  randomBytes,
  createECDH,
  hkdfSync,
  createDecipheriv,
  createPublicKey,
  verify,
} from 'node:crypto';
import { Miniflare, createFetchMock } from 'miniflare';
import { pushEndpoint } from '../src/push.ts';

const server = createECDH('prime256v1');
server.generateKeys();
const receiver = createECDH('prime256v1');
receiver.generateKeys();
const auth = randomBytes(16);
const subscription = {
  endpoint: 'https://fcm.googleapis.com/fcm/send/synthetic-test',
  expirationTime: null,
  keys: { p256dh: receiver.getPublicKey().toString('base64url'), auth: auth.toString('base64url') },
};
const user = 'u'.repeat(40),
  executor = 'e'.repeat(40);
let mf,
  db,
  directory,
  responseStatus = 201;
const deliveries = [];
function decrypt(body) {
  const bytes = Buffer.from(body);
  assert.equal(bytes.length, 4096);
  assert.equal(bytes.readUInt32BE(16), 4096);
  assert.equal(bytes[20], 65);
  const salt = bytes.subarray(0, 16),
    sender = bytes.subarray(21, 86);
  const secret = receiver.computeSecret(sender);
  const ikm = hkdfSync(
    'sha256',
    secret,
    auth,
    Buffer.concat([Buffer.from('WebPush: info\0'), receiver.getPublicKey(), sender]),
    32,
  );
  const cek = hkdfSync('sha256', ikm, salt, Buffer.from('Content-Encoding: aes128gcm\0'), 16);
  const nonce = hkdfSync('sha256', ikm, salt, Buffer.from('Content-Encoding: nonce\0'), 12);
  const decipher = createDecipheriv('aes-128-gcm', cek, nonce);
  decipher.setAuthTag(bytes.subarray(-16));
  const plain = Buffer.concat([decipher.update(bytes.subarray(86, -16)), decipher.final()]);
  let end = plain.length - 1;
  while (plain[end] === 0) end--;
  assert.equal(plain[end], 2);
  return JSON.parse(plain.subarray(0, end).toString());
}
async function req(url, body, machine = false, method = 'POST') {
  return mf.dispatchFetch('https://gateway.test' + url, {
    method: body === undefined ? 'GET' : method,
    headers: {
      Authorization: `Bearer ${machine ? executor : user}`,
      ...(machine ? { 'x-executor-id': 'acacia' } : {}),
      'Content-Type': 'application/json',
    },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
}
async function settled() {
  for (let i = 0; i < 100; i++) {
    const row = await db
      .prepare(
        "SELECT count(*) AS n FROM relay_push_outbox WHERE state='dispatching' OR (state='pending' AND next_attempt_at<=?)",
      )
      .bind(Date.now() / 1000)
      .first();
    if (!row.n) return;
    await new Promise((resolve) => setTimeout(resolve, 30));
  }
  throw new Error('Synthetic notification delivery did not settle');
}
async function create() {
  const exp = {
    id: randomUUID(),
    revision: 1,
    brief: 'Synthetic notification acceptance experiment.',
    phase: 'preparing',
    hold_reason: null,
    stages: [],
    jobs: [],
  };
  assert.equal((await publish(exp)).status, 200);
  return exp;
}
// A stand-in for the executor's notify derivation: the Worker only compares fingerprints.
function notification(exp) {
  if (exp.plan_review?.state === 'awaiting_review')
    return { body: 'Plan ready for your approval', alert: true };
  if (exp.phase === 'failed') return { body: 'Experiment failed', alert: true };
  return { body: String(exp.phase).replaceAll('_', ' '), alert: false };
}
// The executor's projection PUT with its notify envelope (T5.2f).
function publish(exp, revision = exp.revision) {
  const n = notification(exp);
  const key = `experiment:${exp.id}`;
  return req(
    `/api/executor/relay/projections/${key}`,
    exp.deleted_at
      ? { schema_version: 1, key, revision }
      : {
          schema_version: 1,
          key,
          revision,
          document: { ...exp, revision },
          notify: {
            fingerprint: JSON.stringify([n.body, n.alert]),
            title: 'Synthetic experiment',
            body: n.body,
            url: `/?experiment=${exp.id}`,
            alert: n.alert,
            targets: [key],
          },
        },
    true,
    'PUT',
  );
}
const followUrl = (exp) => `/api/push/follows/experiment:${exp.id}`;
async function follow(exp, following = true, request_id = randomUUID()) {
  const payload = { schema_version: 1, request_id, subscription, following };
  const r = await req(followUrl(exp), payload);
  assert.equal(r.status, 200, await r.clone().text());
  await settled();
  return payload;
}
before(async () => {
  directory = await mkdtemp(path.join(tmpdir(), 'et-push-'));
  const fetchMock = createFetchMock();
  fetchMock.disableNetConnect();
  fetchMock
    .get('https://fcm.googleapis.com')
    .intercept({ method: 'POST', path: '/fcm/send/synthetic-test' })
    .reply((options) => {
      const decoded = (async () => {
        const data = decrypt(await new Response(options.body).arrayBuffer());
        assert.equal(options.headers['content-encoding'], 'aes128gcm');
        const authorization = options.headers.authorization;
        assert.match(authorization, /^vapid t=/);
        const token = authorization.match(/t=([^,]+)/)[1],
          [header, claims, signature] = token.split('.');
        const jwt = JSON.parse(Buffer.from(claims, 'base64url'));
        assert.equal(jwt.aud, 'https://fcm.googleapis.com');
        const publicBytes = server.getPublicKey();
        const key = createPublicKey({
          key: {
            kty: 'EC',
            crv: 'P-256',
            x: publicBytes.subarray(1, 33).toString('base64url'),
            y: publicBytes.subarray(33).toString('base64url'),
          },
          format: 'jwk',
        });
        assert.ok(
          verify(
            'sha256',
            Buffer.from(header + '.' + claims),
            { key, dsaEncoding: 'ieee-p1363' },
            Buffer.from(signature, 'base64url'),
          ),
        );
        deliveries.push(data);
        return '';
      })();
      return { statusCode: responseStatus, data: decoded };
    })
    .persist();
  mf = new Miniflare({
    modules: true,
    scriptPath: path.resolve('dist/server/index.js'),
    compatibilityDate: '2025-09-01',
    fetchMock,
    bindings: {
      AUTH_MODE: 'token',
      USER_TOKEN: user,
      EXECUTOR_TOKEN: executor,
      EXECUTOR_ID: 'acacia',
      VAPID_PUBLIC_KEY: server.getPublicKey().toString('base64url'),
      VAPID_PRIVATE_KEY: server.getPrivateKey().toString('base64url'),
      VAPID_SUBJECT: 'https://gateway.test',
    },
    d1Databases: { DB: 'push-test' },
    r2Buckets: { BUCKET: 'push-test' },
    d1Persist: path.join(directory, 'd1'),
  });
  db = await mf.getD1Database('DB');
  for (const file of (await readdir('drizzle')).filter((f) => f.endsWith('.sql')).sort())
    for (const sql of (await readFile(path.join('drizzle', file), 'utf8'))
      .split('--> statement-breakpoint')
      .map((s) => s.trim())
      .filter(Boolean))
      await db.prepare(sql).run();
});
after(async () => {
  await mf?.dispose();
  if (directory) await rm(directory, { recursive: true, force: true });
});

test('push service validation rejects credentials, redirects and arbitrary network targets', async () => {
  for (const url of [
    'http://fcm.googleapis.com/send',
    'https://localhost/send',
    'https://push.apple.com.evil.test/send',
    'https://user:secret@fcm.googleapis.com/send',
    'https://fcm.googleapis.com:444/send',
  ])
    assert.equal(pushEndpoint(url), false);
  assert.equal(pushEndpoint('https://web.push.apple.com/send'), true);
  const exp = await create();
  const r = await req(followUrl(exp), {
    request_id: randomUUID(),
    following: true,
    subscription: { ...subscription, endpoint: 'https://localhost/send' },
  });
  assert.equal(r.status, 422);
  assert.equal((await req('/api/push/config', {}, true)).status, 401);
  assert.equal((await req(followUrl(exp), {}, true)).status, 401);
  const missing = { ...exp, id: randomUUID() };
  assert.equal(
    (await req(followUrl(missing), { request_id: randomUUID(), following: true, subscription }))
      .status,
    404,
  );
  assert.equal(
    (await req('/api/push/follows/projects', { request_id: randomUUID(), subscription })).status,
    400,
  );
});
test('follow receipts, encrypted delivery, monotonic updates and fingerprint changes are idempotent', async () => {
  const exp = await create();
  const before = deliveries.length;
  const payload = await follow(exp);
  assert.equal(deliveries.length, before + 1);
  assert.equal(deliveries.at(-1).experiment_id, exp.id);
  assert.match(deliveries.at(-1).url, new RegExp(exp.id));
  await req(followUrl(exp), payload);
  await settled();
  assert.equal(deliveries.length, before + 1);
  assert.equal((await req(followUrl(exp), { ...payload, following: false })).status, 409);
  const updated = {
    ...exp,
    phase: 'preparing',
    plan_review: { state: 'awaiting_review', revision: 1, stages: [] },
  };
  assert.equal((await publish(updated, 2)).status, 200);
  await settled();
  assert.equal(deliveries.at(-1).alert, true);
  assert.match(deliveries.at(-1).body, /approval/);
  const sent = deliveries.length;
  // Replays, stale revisions and an unchanged fingerprint deliver nothing.
  assert.equal((await publish(updated, 2)).status, 200);
  assert.equal((await publish(exp, 1)).status, 409);
  assert.equal((await publish({ ...updated, updated_at: Date.now() / 1000 }, 3)).status, 200);
  await settled();
  assert.equal(deliveries.length, sent);
  const failed = { ...updated, phase: 'failed', plan_review: null };
  assert.equal((await publish(failed, 4)).status, 200);
  await settled();
  assert.equal(deliveries.length, sent + 1);
  assert.match(deliveries.at(-1).body, /failed/);
  assert.equal(
    (await (await req('/api/push/follows/status', { endpoint: subscription.endpoint })).json()).keys
      .length >= 1,
    true,
  );
  await follow(exp, false);
  const count = () =>
    db
      .prepare('SELECT count(*) AS n FROM relay_follows WHERE key=?')
      .bind(`experiment:${exp.id}`)
      .first();
  assert.equal((await count()).n, 0);
  // Replaying an old follow receipt cannot restore a later-removed follow.
  await req(followUrl(exp), payload);
  await settled();
  assert.equal((await count()).n, 0);
});
test('transient delivery retries persisted intent; expired endpoints remove all device follows', async () => {
  const exp = await create();
  responseStatus = 503;
  await follow(exp);
  let row = await db
    .prepare('SELECT * FROM relay_push_outbox WHERE key=?')
    .bind(`experiment:${exp.id}`)
    .first();
  assert.equal(row.state, 'pending');
  assert.equal(row.attempts, 1);
  assert.equal(row.last_status, 503);
  await db.prepare('UPDATE relay_push_outbox SET next_attempt_at=0 WHERE id=?').bind(row.id).run();
  responseStatus = 201;
  await req('/api/executor/relay/commands', undefined, true);
  await settled();
  row = await db.prepare('SELECT * FROM relay_push_outbox WHERE id=?').bind(row.id).first();
  assert.equal(row.state, 'sent');
  assert.equal(row.attempts, 2);
  const another = await create();
  await follow(another);
  responseStatus = 410;
  await publish({ ...exp, phase: 'failed' }, 2);
  await settled();
  assert.equal((await db.prepare('SELECT count(*) AS n FROM push_subscriptions').first()).n, 0);
  assert.equal((await db.prepare('SELECT count(*) AS n FROM relay_follows').first()).n, 0);
  responseStatus = 201;
});
test('tombstone and sign-out revoke follows and queued intents; subscriptions are bound to their user', async () => {
  const exp = await create();
  await follow(exp);
  await db.prepare("UPDATE push_subscriptions SET owner='sites:another-user'").run();
  assert.deepEqual(
    (await (await req('/api/push/follows/status', { endpoint: subscription.endpoint })).json())
      .keys,
    [],
  );
  assert.equal(
    (
      await req(followUrl(exp), {
        request_id: randomUUID(),
        following: true,
        subscription,
      })
    ).status,
    409,
  );
  await db.prepare("UPDATE push_subscriptions SET owner='local-owner'").run();
  await req('/api/push/forget', { endpoint: subscription.endpoint });
  assert.equal((await db.prepare('SELECT count(*) AS n FROM push_subscriptions').first()).n, 0);
  await follow(exp);
  assert.equal((await publish({ ...exp, deleted_at: 1 }, 2)).status, 200);
  await settled();
  assert.equal((await db.prepare('SELECT count(*) AS n FROM relay_follows').first()).n, 0);
  assert.equal((await db.prepare('SELECT count(*) AS n FROM relay_push_outbox').first()).n, 0);
});
test('migration 0011 copies old follows by key and ignores repeats', async () => {
  const sql = (await readFile('drizzle/0011_relay_follows.sql', 'utf8')).trim();
  assert.doesNotMatch(sql, /\b(DROP|ALTER)\b/i);
  const id = randomUUID();
  await db
    .prepare('INSERT INTO projections(id,revision,payload,updated_at) VALUES(?,1,?,1)')
    .bind(id, '{}')
    .run();
  await db.prepare("INSERT INTO push_subscriptions VALUES('copy-dev','local-owner','{}',1)").run();
  await db.prepare('PRAGMA foreign_keys=OFF').run();
  await db.prepare("INSERT INTO push_follows VALUES('copy-dev',?,5)").bind(id).run();
  for (let i = 0; i < 2; i++) await db.prepare(sql.replace(/^--.*\n/, '')).run();
  const rows = await db
    .prepare("SELECT key,created_at FROM relay_follows WHERE subscription_id='copy-dev'")
    .all();
  assert.deepEqual(rows.results, [{ key: `experiment:${id}`, created_at: 5 }]);
});
