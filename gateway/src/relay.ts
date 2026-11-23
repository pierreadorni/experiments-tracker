import type { Env } from './index';
import {
  pushConfigured,
  pushEndpointSchema,
  pushFollowSchema,
  pushHash,
  pushOwner,
  relayPushStatements,
  relayTombstoneStatements,
  type RelayNotify,
} from './push';
import { z } from 'zod';

// Relay surface (T5.2a): auth, size, executor identity and monotonic revisions only.
// The main router authenticates the executor and browser roles before calling relayRoute.
const VIEW_MAX = 1024 * 1024,
  COMMAND_MAX = 64 * 1024,
  BLOB_MAX = 10 * 1024 * 1024,
  PENDING_MAX = 500,
  BATCH = 20;
const id = '[A-Za-z0-9_-]{1,100}',
  uuid = '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}';
const KEY = new RegExp(
  `^(experiment:${uuid}|conversation:${uuid}:\\d{10}|projects|status|inbox|knowledge:${id}|knowledge-report:${id}:\\d+|autopilot:${id}|maintenance)$`,
);
const BLOB = /^[A-Za-z0-9_:-]{1,128}$/,
  KIND = /^[a-z_]{1,40}$/,
  SHA = /^[0-9a-f]{64}$/;

class Refusal extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}
const reply = (value: unknown, status = 200, headers: HeadersInit = {}) =>
  new Response(JSON.stringify(value), {
    status,
    headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', ...headers },
  });
const hex = (bytes: ArrayBuffer) =>
  Array.from(new Uint8Array(bytes), (v) => v.toString(16).padStart(2, '0')).join('');
const sha256 = async (value: string | ArrayBuffer) =>
  hex(
    await crypto.subtle.digest(
      'SHA-256',
      typeof value === 'string' ? new TextEncoder().encode(value) : value,
    ),
  );
async function read(request: Request, max: number): Promise<ArrayBuffer> {
  if (Number(request.headers.get('content-length') || 0) > max)
    throw new Refusal(413, 'Payload too large');
  const chunks: Uint8Array[] = [];
  let total = 0;
  if (request.body) {
    const reader = request.body.getReader();
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > max) {
        await reader.cancel();
        throw new Refusal(413, 'Payload too large');
      }
      chunks.push(value);
    }
  }
  const out = new Uint8Array(total);
  let at = 0;
  for (const chunk of chunks) {
    out.set(chunk, at);
    at += chunk.byteLength;
  }
  return out.buffer;
}
async function readJson(request: Request, max: number) {
  const text = new TextDecoder().decode(await read(request, max));
  try {
    const value = JSON.parse(text);
    if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error();
    return { text, value: value as Record<string, unknown> };
  } catch {
    throw new Refusal(400, 'Invalid JSON object');
  }
}
const key = (value: unknown) => {
  if (typeof value !== 'string' || !KEY.test(value)) throw new Refusal(400, 'Invalid key');
  return value;
};
const revision = (value: unknown) => {
  if (!Number.isSafeInteger(value) || (value as number) < 0)
    throw new Refusal(400, 'Invalid revision');
  return value as number;
};
const param = (value: string) => {
  try {
    return decodeURIComponent(value);
  } catch {
    throw new Refusal(400, 'Invalid path');
  }
};
const first = (env: Env, sql: string, ...args: unknown[]) =>
  env.DB.prepare(sql)
    .bind(...args)
    .first<Record<string, unknown>>();
const rows = async (env: Env, sql: string, ...args: unknown[]) =>
  (
    await env.DB.prepare(sql)
      .bind(...args)
      .all<Record<string, unknown>>()
  ).results;
const document = (row: Record<string, unknown>) => ({
  key: row.key,
  revision: row.revision,
  updated_at: row.updated_at,
  document: JSON.parse(String(row.payload)),
});
const receipt = (row: Record<string, unknown>) => ({
  schema_version: 1,
  request_id: row.request_id,
  kind: row.kind,
  target: row.target,
  state: row.state,
  result: row.result == null ? null : JSON.parse(String(row.result)),
  error: row.error,
  received_at: row.received_at,
  settled_at: row.settled_at,
});

async function putProjection(request: Request, env: Env, path: string) {
  const k = key(param(path));
  const { value } = await readJson(request, VIEW_MAX);
  if (value.key !== undefined && value.key !== k) throw new Refusal(400, 'Key mismatch');
  const rev = revision(value.revision);
  // A null document is a tombstone: it hides the key and the blobs it owns.
  const doc = value.document === undefined ? null : value.document;
  const payload = doc === null ? null : JSON.stringify(doc);
  const notify = value.notify as RelayNotify | undefined;
  if (notify !== undefined) {
    const t = notify?.targets;
    if (
      typeof notify !== 'object' ||
      typeof notify?.fingerprint !== 'string' ||
      typeof notify.title !== 'string' ||
      typeof notify.body !== 'string' ||
      typeof notify.url !== 'string' ||
      typeof notify.alert !== 'boolean' ||
      !Array.isArray(t) ||
      t.length > 50 ||
      !t.every((x) => typeof x === 'string' && KEY.test(x))
    )
      throw new Refusal(400, 'Invalid notify');
  }
  const digest = await sha256(payload ?? '\u0000tombstone');
  const now = Date.now() / 1000;
  const prior = await first(
    env,
    'SELECT revision,notify_fingerprint FROM relay_projections WHERE key=?',
    k,
  );
  const newer = !prior || rev > Number(prior.revision);
  const extra = !newer
    ? []
    : doc === null
      ? relayTombstoneStatements(env, k, rev)
      : notify && notify.fingerprint !== prior?.notify_fingerprint
        ? relayPushStatements(env, k, rev, notify)
        : [];
  const [written] = await env.DB.batch([
    env.DB.prepare(
      'INSERT INTO relay_projections(key,executor_id,revision,payload,digest,notify_fingerprint,updated_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET revision=excluded.revision,payload=excluded.payload,digest=excluded.digest,notify_fingerprint=excluded.notify_fingerprint,updated_at=excluded.updated_at WHERE excluded.revision>relay_projections.revision',
    ).bind(k, env.EXECUTOR_ID, rev, payload, digest, notify?.fingerprint ?? null, now),
    ...extra,
  ]);
  if (written.meta.changes)
    return reply({ schema_version: 1, key: k, revision: rev, applied: true });
  const same = await first(env, 'SELECT revision,digest FROM relay_projections WHERE key=?', k);
  if (same && same.revision === rev && same.digest === digest)
    return reply({ schema_version: 1, key: k, revision: rev, applied: false });
  throw new Refusal(409, 'stale revision');
}

const FOLLOW = new RegExp(`^experiment:${uuid}$`);
const followOwner = (request: Request, env: Env) => pushOwner(request, env);

async function followStatus(request: Request, env: Env) {
  const { endpoint } = z
    .object({ endpoint: pushEndpointSchema })
    .strict()
    .parse((await readJson(request, 4096)).value);
  const found = await rows(
    env,
    `SELECT f.key FROM relay_follows f JOIN push_subscriptions s ON s.id=f.subscription_id
    JOIN relay_projections p ON p.key=f.key WHERE s.id=? AND s.owner=? AND p.payload IS NOT NULL`,
    await pushHash(endpoint),
    followOwner(request, env),
  );
  return reply({ schema_version: 1, keys: found.map((f) => f.key) });
}

// Follow or unfollow one projection key. Receipts make a replayed request idempotent.
async function follow(request: Request, env: Env, k: string) {
  if (!pushConfigured(env)) throw new Refusal(503, 'Notifications are not configured');
  if (!FOLLOW.test(k)) throw new Refusal(400, 'Only experiments can be followed');
  const projection = await first(
    env,
    'SELECT revision FROM relay_projections WHERE key=? AND payload IS NOT NULL',
    k,
  );
  if (!projection) throw new Refusal(404, 'Unknown projection');
  const payload = pushFollowSchema.parse((await readJson(request, 8192)).value);
  // Import validates that the subscription point is actually on P-256.
  const publicBytes = Uint8Array.from(
    atob(payload.subscription.keys.p256dh.replaceAll('-', '+').replaceAll('_', '/')),
    (c) => c.charCodeAt(0),
  );
  try {
    await crypto.subtle.importKey(
      'raw',
      publicBytes,
      { name: 'ECDH', namedCurve: 'P-256' },
      false,
      [],
    );
  } catch {
    throw new Refusal(422, 'Invalid subscription public key');
  }
  const owner = followOwner(request, env);
  const id = await pushHash(payload.subscription.endpoint);
  const hash = await pushHash(JSON.stringify([k, payload]));
  const immutable = () => new Refusal(409, 'Notification request identity is immutable');
  const saved = await first(
    env,
    'SELECT owner,payload_hash FROM push_receipts WHERE request_id=?',
    payload.request_id,
  );
  if (saved) {
    if (saved.owner !== owner || saved.payload_hash !== hash) throw immutable();
    return reply({ following: payload.following });
  }
  const prior = await first(env, 'SELECT owner FROM push_subscriptions WHERE id=?', id);
  if (prior && prior.owner !== owner)
    throw new Refusal(409, 'This browser subscription belongs to another signed-in user');
  if (
    !prior &&
    Number(
      (await first(env, 'SELECT count(*) AS n FROM push_subscriptions WHERE owner=?', owner))?.n,
    ) >= 32
  )
    throw new Refusal(409, 'Too many notification devices; remove an old subscription first');
  const now = Date.now() / 1000;
  const guard =
    'EXISTS(SELECT 1 FROM push_receipts WHERE request_id=? AND owner=? AND payload_hash=?)';
  const mine = [payload.request_id, owner, hash];
  const statements = [
    env.DB.prepare(
      'INSERT OR IGNORE INTO push_receipts(request_id,owner,payload_hash) VALUES(?,?,?)',
    ).bind(...mine),
  ];
  if (payload.following) {
    const initial = {
      title: 'ExperimentTracker',
      body: 'You are following this experiment',
      url: `/?experiment=${encodeURIComponent(k.slice(11))}`,
      alert: true,
      revision: projection.revision,
      experiment_id: k.slice(11),
    };
    statements.push(
      env.DB.prepare(
        `INSERT INTO push_subscriptions(id,owner,subscription,updated_at)
        SELECT ?,?,?,? WHERE ${guard} ON CONFLICT(id) DO UPDATE SET subscription=excluded.subscription,updated_at=excluded.updated_at WHERE push_subscriptions.owner=excluded.owner`,
      ).bind(id, owner, JSON.stringify(payload.subscription), now, ...mine),
      env.DB.prepare(
        `INSERT OR IGNORE INTO relay_push_outbox(id,subscription_id,key,revision,payload,next_attempt_at,created_at)
        SELECT ?,?,?,?,?,?,? WHERE ${guard} AND EXISTS(SELECT 1 FROM push_subscriptions WHERE id=? AND owner=?)
        AND NOT EXISTS(SELECT 1 FROM relay_follows WHERE subscription_id=? AND key=?)`,
      ).bind(
        `follow:${payload.request_id}`,
        id,
        k,
        projection.revision,
        JSON.stringify(initial),
        now,
        now,
        ...mine,
        id,
        owner,
        id,
        k,
      ),
      env.DB.prepare(
        `INSERT OR IGNORE INTO relay_follows(subscription_id,key,created_at)
        SELECT ?,?,? WHERE ${guard} AND EXISTS(SELECT 1 FROM push_subscriptions WHERE id=? AND owner=?)`,
      ).bind(id, k, now, ...mine, id, owner),
    );
  } else {
    statements.push(
      env.DB.prepare(
        `DELETE FROM relay_follows WHERE subscription_id=? AND key=? AND ${guard}`,
      ).bind(id, k, ...mine),
      env.DB.prepare(
        `DELETE FROM relay_push_outbox WHERE subscription_id=? AND key=? AND ${guard}`,
      ).bind(id, k, ...mine),
      env.DB.prepare(
        `DELETE FROM push_subscriptions WHERE id=? AND owner=? AND ${guard} AND NOT EXISTS(SELECT 1 FROM relay_follows WHERE subscription_id=?)`,
      ).bind(id, owner, ...mine, id),
    );
  }
  await env.DB.batch(statements);
  const committed = await first(
    env,
    'SELECT owner,payload_hash FROM push_receipts WHERE request_id=?',
    payload.request_id,
  );
  if (committed?.owner !== owner || committed?.payload_hash !== hash) throw immutable();
  if (
    payload.following &&
    (await first(env, 'SELECT owner FROM push_subscriptions WHERE id=?', id))?.owner !== owner
  )
    throw new Refusal(409, 'This browser subscription belongs to another signed-in user');
  return reply({ following: payload.following });
}

async function putBlob(request: Request, env: Env, blobId: string) {
  if (!BLOB.test(blobId)) throw new Refusal(400, 'Invalid blob id');
  const sha = request.headers.get('x-sha256') || '',
    owner = key(request.headers.get('x-owner-key')),
    size = request.headers.get('x-size');
  if (!SHA.test(sha)) throw new Refusal(400, 'x-sha256 required');
  const bytes = await read(request, BLOB_MAX);
  if ((size !== null && Number(size) !== bytes.byteLength) || (await sha256(bytes)) !== sha)
    throw new Refusal(409, 'Blob size or hash mismatch');
  const object = `blobs/${blobId}`;
  const prior = await env.BUCKET.head(object);
  if (prior) {
    if (prior.customMetadata?.sha256 !== sha) throw new Refusal(409, 'Blob id is immutable');
  } else
    await env.BUCKET.put(object, bytes, {
      httpMetadata: {
        contentType: request.headers.get('content-type') || 'application/octet-stream',
      },
      customMetadata: {
        sha256: sha,
        size: String(bytes.byteLength),
        media_type: request.headers.get('content-type') || 'application/octet-stream',
        name: (request.headers.get('x-name') || blobId).slice(0, 200),
        owner_key: owner,
      },
    });
  return reply({ schema_version: 1, id: blobId, sha256: sha, size: bytes.byteLength });
}

async function getBlob(env: Env, blobId: string) {
  if (!BLOB.test(blobId)) throw new Refusal(404, 'Unknown blob');
  const object = await env.BUCKET.get(`blobs/${blobId}`);
  const meta = object?.customMetadata;
  const live =
    meta?.owner_key &&
    (await first(
      env,
      'SELECT 1 AS ok FROM relay_projections WHERE key=? AND payload IS NOT NULL',
      meta.owner_key,
    ));
  if (!object || !live) throw new Refusal(404, 'Unknown blob');
  return new Response(object.body, {
    headers: {
      'Content-Type': meta.media_type || 'application/octet-stream',
      'Content-Disposition': `inline; filename="${(meta.name || blobId).replace(/[^\w.-]/g, '_')}"`,
      'Content-Security-Policy':
        "sandbox allow-same-origin; default-src 'none'; style-src 'unsafe-inline'; img-src 'self'; font-src 'self'; base-uri 'none'",
      'Cache-Control': 'no-store',
    },
  });
}

async function postCommand(request: Request, env: Env) {
  const { text, value } = await readJson(request, COMMAND_MAX);
  const requestId = value.request_id,
    kind = value.kind,
    target = value.target ?? null;
  if (typeof requestId !== 'string' || !new RegExp(`^${uuid}$`).test(requestId))
    throw new Refusal(400, 'request_id must be a lowercase UUID');
  if (typeof kind !== 'string' || !KIND.test(kind)) throw new Refusal(400, 'Invalid kind');
  if (target !== null) key(target);
  const existing = async () =>
    first(env, 'SELECT * FROM relay_commands WHERE request_id=?', requestId);
  const same = (row: Record<string, unknown> | null) => {
    if (!row) return null;
    if (row.payload !== text) throw new Refusal(409, 'request_id was used with different bytes');
    return reply(receipt(row));
  };
  const prior = same(await existing());
  if (prior) return prior;
  // One statement counts and inserts, so concurrent posts cannot pass the cap together.
  const inserted = await env.DB.prepare(
    "INSERT INTO relay_commands(request_id,executor_id,kind,target,payload,state,received_at) SELECT ?,?,?,?,?,'pending',? WHERE (SELECT COUNT(*) FROM relay_commands WHERE executor_id=? AND state='pending')<? ON CONFLICT(request_id) DO NOTHING",
  )
    .bind(
      requestId,
      env.EXECUTOR_ID,
      kind,
      target,
      text,
      Date.now() / 1000,
      env.EXECUTOR_ID,
      PENDING_MAX,
    )
    .run();
  const row = await existing();
  if (!inserted.meta.changes && !row) throw new Refusal(429, 'Too many pending commands');
  return same(row)!;
}

async function postReceipt(request: Request, env: Env) {
  const { value } = await readJson(request, COMMAND_MAX);
  const state = value.state;
  if (state !== 'applied' && state !== 'rejected') throw new Refusal(400, 'Invalid state');
  const result =
    value.result === undefined || value.result === null ? null : JSON.stringify(value.result);
  const error = typeof value.error === 'string' ? value.error.slice(0, 2000) : null;
  const requestId = String(value.request_id);
  const now = Date.now() / 1000;
  await env.DB.prepare(
    "UPDATE relay_commands SET state=?,result=?,error=?,settled_at=? WHERE request_id=? AND executor_id=? AND state='pending'",
  )
    .bind(state, result, error, now, requestId, env.EXECUTOR_ID)
    .run();
  const row = await first(env, 'SELECT * FROM relay_commands WHERE request_id=?', requestId);
  if (!row) throw new Refusal(404, 'Unknown command');
  if (row.state !== state || row.result !== result || row.error !== error)
    throw new Refusal(409, 'Receipt is terminal and differs');
  return reply(receipt(row));
}

async function list(url: URL, env: Env) {
  const prefix = url.searchParams.get('prefix') || '',
    before = url.searchParams.get('before') || '￿',
    limit = Math.min(Math.max(Number(url.searchParams.get('limit')) || 50, 1), 100);
  const found = await rows(
    env,
    'SELECT key,revision,payload,updated_at FROM relay_projections WHERE payload IS NOT NULL AND substr(key,1,?)=? AND key<? ORDER BY key DESC LIMIT ?',
    prefix.length,
    prefix,
    before,
    limit,
  );
  return reply({ schema_version: 1, items: found.map(document) });
}

async function run(request: Request, env: Env, p: string, url: URL): Promise<Response | null> {
  const m = request.method;
  let match: RegExpMatchArray | null;
  if (p.startsWith('/api/executor/relay/')) {
    if (request.headers.get('x-executor-id') !== env.EXECUTOR_ID)
      throw new Refusal(403, 'Executor identity mismatch');
    if ((match = p.match(/^\/api\/executor\/relay\/projections\/([^/]+)$/)) && m === 'PUT')
      return putProjection(request, env, match[1]);
    if ((match = p.match(/^\/api\/executor\/relay\/blobs\/([^/]+)$/)) && m === 'PUT')
      return putBlob(request, env, param(match[1]));
    if (p === '/api/executor/relay/revisions' && m === 'GET') {
      const found = await rows(
        env,
        'SELECT key,revision,digest FROM relay_projections WHERE executor_id=?',
        env.EXECUTOR_ID,
      );
      return reply({
        schema_version: 1,
        revisions: Object.fromEntries(found.map((r) => [r.key, [r.revision, r.digest]])),
      });
    }
    if (p === '/api/executor/relay/commands' && m === 'GET') {
      const found = await rows(
        env,
        "SELECT * FROM relay_commands WHERE executor_id=? AND state='pending' ORDER BY received_at,request_id LIMIT ?",
        env.EXECUTOR_ID,
        BATCH,
      );
      return reply({
        schema_version: 1,
        commands: found.map((r) => ({
          ...JSON.parse(String(r.payload)),
          received_at: r.received_at,
        })),
      });
    }
    if (p === '/api/executor/relay/receipts' && m === 'POST') return postReceipt(request, env);
    throw new Refusal(404, 'Unknown endpoint');
  }
  if (p === '/api/push/follows/status' && m === 'POST') return followStatus(request, env);
  if ((match = p.match(/^\/api\/push\/follows\/([^/]+)$/)) && m === 'POST')
    return follow(request, env, param(match[1]));
  if (p === '/api/session' && m === 'GET')
    return reply({ schema_version: 1, authenticated: true, host: 'gateway' });
  if (p === '/api/projections' && m === 'GET') return list(url, env);
  if ((match = p.match(/^\/api\/projections\/([^/]+)$/)) && m === 'GET') {
    const row = await first(
      env,
      'SELECT * FROM relay_projections WHERE key=? AND payload IS NOT NULL',
      param(match[1]),
    );
    if (!row) throw new Refusal(404, 'Unknown projection');
    return reply(document(row));
  }
  if (p === '/api/commands' && m === 'POST') return postCommand(request, env);
  if (p === '/api/commands' && m === 'GET') {
    if (url.searchParams.get('state') !== 'pending')
      throw new Refusal(400, 'state=pending required');
    const found = await rows(
      env,
      "SELECT * FROM relay_commands WHERE executor_id=? AND state='pending' ORDER BY received_at,request_id LIMIT ?",
      env.EXECUTOR_ID,
      PENDING_MAX,
    );
    return reply({
      schema_version: 1,
      commands: found.map((r) => ({
        ...receipt(r),
        // The saved body, so a reloaded page can show what a pending command says.
        payload: (JSON.parse(String(r.payload)) as { payload?: unknown }).payload ?? null,
      })),
    });
  }
  if ((match = p.match(/^\/api\/commands\/([^/]+)$/)) && m === 'GET') {
    const row = await first(
      env,
      'SELECT * FROM relay_commands WHERE request_id=?',
      param(match[1]),
    );
    // Not a relay command: leave the id to the old receipt route (removed in T5.2i).
    if (!row) return null;
    return reply(receipt(row));
  }
  if ((match = p.match(/^\/api\/blobs\/([^/]+)$/)) && m === 'GET')
    return getBlob(env, param(match[1]));
  throw new Refusal(404, 'Unknown endpoint');
}

export async function relayRoute(request: Request, env: Env, p: string): Promise<Response | null> {
  if (!/^\/api\/(executor\/relay\/|projections|commands|blobs\/|session$|push\/follows)/.test(p))
    return null;
  try {
    return await run(request, env, p, new URL(request.url));
  } catch (error) {
    if (error instanceof Refusal) return reply({ detail: error.message }, error.status);
    throw error;
  }
}
