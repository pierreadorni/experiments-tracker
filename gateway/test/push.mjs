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
import { experimentNotification, pushEndpoint } from '../src/push.ts';

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
async function req(url, body, machine = false) {
  return mf.dispatchFetch('https://gateway.test' + url, {
    method: body === undefined ? 'GET' : 'POST',
    headers: {
      Authorization: `Bearer ${machine ? executor : user}`,
      'Content-Type': 'application/json',
    },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
}
async function settled() {
  for (let i = 0; i < 100; i++) {
    const row = await db
      .prepare(
        "SELECT count(*) AS n FROM push_outbox WHERE state='dispatching' OR (state='pending' AND next_attempt_at<=?)",
      )
      .bind(Date.now() / 1000)
      .first();
    if (!row.n) return;
    await new Promise((resolve) => setTimeout(resolve, 30));
  }
  throw new Error('Synthetic notification delivery did not settle');
}
async function create() {
  const r = await req('/api/experiments', {
    schema_version: 1,
    request_id: randomUUID(),
    brief: 'Synthetic notification acceptance experiment.',
    recipe_id: 'toy_compare_v1',
  });
  assert.equal(r.status, 202);
  const { experiment_id } = await r.json();
  return (await req(`/api/experiments/${experiment_id}`)).json();
}
function publish(exp, event = randomUUID(), inbox = []) {
  return req(
    '/api/executor/publish',
    {
      schema_version: 1,
      executor_id: 'acacia',
      messages: [{ schema_version: 1, event_id: event, experiment: exp, inbox }],
    },
    true,
  );
}
async function follow(exp, following = true, request_id = randomUUID()) {
  const payload = { schema_version: 1, request_id, subscription, following };
  const r = await req(`/api/experiments/${exp.id}/push`, payload);
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

test('notification descriptions use saved stage state, review identities and ready report versions', () => {
  const exp = {
    id: randomUUID(),
    revision: 1,
    brief: 'Demo',
    phase: 'training',
    hold_reason: null,
    contract: { synthetic_demo: true },
    stages: [{ id: 'fit', state: 'running' }],
    jobs: [{ stage_id: 'fit', state: 'submitted', scheduler_state: 'PENDING' }],
    snapshot: { plan: { stages: [{ id: 'fit', title: 'Fit model' }] } },
  };
  const queued = experimentNotification(exp);
  assert.match(queued.data.body, /Demo · Fit model · Queued/);
  assert.equal(queued.data.alert, false);
  const parallel = {
    ...exp,
    stages: [{ id: 'finished', state: 'running' }, ...exp.stages],
    jobs: [{ stage_id: 'finished', state: 'complete', scheduler_state: 'COMPLETED' }, ...exp.jobs],
  };
  assert.match(experimentNotification(parallel).data.body, /Fit model · Queued/);
  assert.match(experimentNotification({ ...exp, stages: [] }).data.body, /Fit model · Queued/);
  for (const [scheduler_state, label] of [
    [null, 'Waiting for scheduler'],
    ['CONFIGURING', 'Queued'],
    ['ACCOUNTING_PENDING', 'Waiting for accounting'],
    ['COMPLETING', 'Finishing'],
    ['SUSPENDED', 'Scheduler: suspended'],
  ])
    assert.ok(
      experimentNotification({
        ...exp,
        jobs: [{ ...exp.jobs[0], scheduler_state }],
      }).data.body.includes(label),
    );
  assert.match(
    experimentNotification({
      ...exp,
      jobs: [{ ...exp.jobs[0], state: 'submitting', scheduler_state: null }],
    }).data.body,
    /Submitting/,
  );
  assert.match(
    experimentNotification({
      ...parallel,
      jobs: [
        ...parallel.jobs,
        { stage_id: 'other', state: 'submitted', scheduler_state: 'RUNNING' },
      ],
    }).data.body,
    /other · Running/,
  );
  const running = experimentNotification({
    ...exp,
    jobs: [{ ...exp.jobs[0], scheduler_state: 'RUNNING' }],
  });
  assert.notEqual(queued.fingerprint, running.fingerprint);
  const review = experimentNotification({
    ...exp,
    plan_review: { state: 'awaiting_review', revision: 2 },
  });
  assert.equal(review.data.alert, true);
  assert.notEqual(
    review.fingerprint,
    experimentNotification({ ...exp, plan_review: { state: 'awaiting_review', revision: 3 } })
      .fingerprint,
  );
  const done = { ...exp, phase: 'complete', active_report_id: 'one' };
  assert.notEqual(
    experimentNotification(done).fingerprint,
    experimentNotification({ ...done, active_report_id: 'two' }).fingerprint,
  );
  assert.equal(experimentNotification({ ...exp, phase: 'failed' }).data.alert, true);
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
  const r = await req(`/api/experiments/${exp.id}/push`, {
    request_id: randomUUID(),
    following: true,
    subscription: { ...subscription, endpoint: 'https://localhost/send' },
  });
  assert.equal(r.status, 422);
  assert.equal((await req('/api/push/config', {}, true)).status, 401);
});
test('follow receipts, encrypted delivery, monotonic updates and transaction rollback are idempotent', async () => {
  const exp = await create();
  const before = deliveries.length;
  const payload = await follow(exp);
  assert.equal(deliveries.length, before + 1);
  assert.equal(deliveries.at(-1).experiment_id, exp.id);
  assert.match(deliveries.at(-1).url, new RegExp(exp.id));
  await req(`/api/experiments/${exp.id}/push`, payload);
  await settled();
  assert.equal(deliveries.length, before + 1);
  assert.equal(
    (await req(`/api/experiments/${exp.id}/push`, { ...payload, following: false })).status,
    409,
  );
  const updated = {
    ...exp,
    revision: 1,
    phase: 'preparing',
    plan_review: { state: 'awaiting_review', revision: 1, stages: [] },
  };
  const event = randomUUID();
  assert.equal((await publish(updated, event)).status, 200);
  await settled();
  assert.equal(deliveries.at(-1).alert, true);
  assert.match(deliveries.at(-1).body, /approval/);
  const sent = deliveries.length;
  assert.equal((await publish(updated, event)).status, 200);
  assert.equal((await publish(exp)).status, 200);
  assert.equal(
    (await publish({ ...updated, revision: 2, updated_at: Date.now() / 1000 })).status,
    200,
  );
  await settled();
  assert.equal(deliveries.length, sent);
  const failed = { ...updated, revision: 3, phase: 'failed', plan_review: null };
  assert.equal(
    (
      await publish(failed, randomUUID(), [
        {
          id: randomUUID(),
          experiment_id: randomUUID(),
          kind: 'required_decision',
          text: 'Invalid',
          created_at: 1,
        },
      ])
    ).status,
    400,
  );
  assert.equal(
    (await db.prepare('SELECT revision FROM projections WHERE id=?').bind(exp.id).first()).revision,
    2,
  );
  await settled();
  assert.equal(deliveries.length, sent);
  assert.equal((await publish(failed)).status, 200);
  await settled();
  assert.equal(deliveries.length, sent + 1);
  assert.match(deliveries.at(-1).body, /failed/);
  await follow(exp, false);
  assert.equal(
    (
      await db
        .prepare('SELECT count(*) AS n FROM push_follows WHERE experiment_id=?')
        .bind(exp.id)
        .first()
    ).n,
    0,
  );
  // Replaying an old follow receipt cannot restore a later-removed follow.
  await req(`/api/experiments/${exp.id}/push`, payload);
  await settled();
  assert.equal(
    (
      await db
        .prepare('SELECT count(*) AS n FROM push_follows WHERE experiment_id=?')
        .bind(exp.id)
        .first()
    ).n,
    0,
  );
});
test('transient delivery retries persisted intent; expired endpoints remove all device follows', async () => {
  const exp = await create();
  responseStatus = 503;
  await follow(exp);
  let row = await db
    .prepare('SELECT * FROM push_outbox WHERE experiment_id=?')
    .bind(exp.id)
    .first();
  assert.equal(row.state, 'pending');
  assert.equal(row.attempts, 1);
  assert.equal(row.last_status, 503);
  await db.prepare('UPDATE push_outbox SET next_attempt_at=0 WHERE id=?').bind(row.id).run();
  responseStatus = 201;
  await req('/api/executor/commands', undefined, true);
  await settled();
  row = await db.prepare('SELECT * FROM push_outbox WHERE id=?').bind(row.id).first();
  assert.equal(row.state, 'sent');
  assert.equal(row.attempts, 2);
  const another = await create();
  await follow(another);
  responseStatus = 410;
  await publish({ ...exp, revision: 1, phase: 'failed' });
  await settled();
  assert.equal((await db.prepare('SELECT count(*) AS n FROM push_subscriptions').first()).n, 0);
  assert.equal((await db.prepare('SELECT count(*) AS n FROM push_follows').first()).n, 0);
  responseStatus = 201;
});
test('deletion and sign-out revoke follows and queued intents; subscriptions are bound to their user', async () => {
  const exp = await create();
  await follow(exp);
  await db.prepare("UPDATE push_subscriptions SET owner='sites:another-user'").run();
  assert.deepEqual(
    (await (await req('/api/push/status', { endpoint: subscription.endpoint })).json()).experiments,
    [],
  );
  assert.equal(
    (
      await req(`/api/experiments/${exp.id}/push`, {
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
  assert.equal(
    (await publish({ ...exp, revision: 1, phase: 'failed', deleted_at: Date.now() / 1000 })).status,
    200,
  );
  await settled();
  assert.equal((await db.prepare('SELECT count(*) AS n FROM push_follows').first()).n, 0);
  assert.equal((await db.prepare('SELECT count(*) AS n FROM push_outbox').first()).n, 0);
});
