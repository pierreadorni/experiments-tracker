import { z } from 'zod';
import { buildPushPayload } from '@block65/webcrypto-web-push';
import type { Env } from './index';

export function pushConfigured(env: Env) {
  return Boolean(env.VAPID_PUBLIC_KEY && env.VAPID_PRIVATE_KEY && env.VAPID_SUBJECT);
}
export function pushOwner(request: Request, env: Env) {
  return env.AUTH_MODE === 'sites'
    ? `sites:${request.headers.get('oai-authenticated-user-id')}`
    : 'local-owner';
}
export async function pushHash(value: string) {
  const bytes = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value));
  return Array.from(new Uint8Array(bytes), (b) => b.toString(16).padStart(2, '0')).join('');
}
// These URLs are capabilities, never navigation destinations or log fields.
// Accept only known browser push services to prevent arbitrary outbound requests.
export function pushEndpoint(value: string) {
  try {
    const url = new URL(value);
    return (
      url.protocol === 'https:' &&
      !url.username &&
      !url.password &&
      !url.port &&
      !url.hash &&
      (url.hostname === 'fcm.googleapis.com' ||
        url.hostname === 'updates.push.services.mozilla.com' ||
        url.hostname.endsWith('.push.apple.com') ||
        url.hostname.endsWith('.notify.windows.com'))
    );
  } catch {
    return false;
  }
}
export const pushEndpointSchema = z
  .string()
  .max(2048)
  .refine(pushEndpoint, 'Unsupported browser push service');
const key = (length: number) =>
  z
    .string()
    .regex(/^[A-Za-z0-9_-]+={0,2}$/)
    .refine((s) => {
      try {
        return atob(s.replaceAll('-', '+').replaceAll('_', '/')).length === length;
      } catch {
        return false;
      }
    }, 'Invalid subscription key');
export const pushSubscriptionSchema = z
  .object({
    endpoint: pushEndpointSchema,
    expirationTime: z.number().nullable().optional().default(null),
    keys: z.object({ p256dh: key(65), auth: key(16) }).strict(),
  })
  .strict();
export const pushFollowSchema = z
  .object({
    schema_version: z.literal(1).default(1),
    request_id: z.string().uuid(),
    subscription: pushSubscriptionSchema,
    following: z.boolean(),
  })
  .strict();

// Delivery is driven by the executor's notify envelope on a projection PUT (T5.2f).
// A changed fingerprint queues one delivery per device following any target; a
// tombstone drops the follows of its key. Both are guarded by the stored revision.
export type RelayNotify = {
  fingerprint: string;
  title: string;
  body: string;
  url: string;
  alert: boolean;
  targets: string[];
};
export function relayPushStatements(env: Env, key: string, revision: number, notify: RelayNotify) {
  const live =
    'EXISTS(SELECT 1 FROM relay_projections WHERE key=? AND revision=? AND payload IS NOT NULL)';
  const now = Date.now() / 1000;
  const payload = JSON.stringify({
    title: notify.title.slice(0, 100),
    body: notify.body.slice(0, 240),
    url: notify.url,
    alert: notify.alert,
    revision,
    timestamp: Date.now(),
  });
  const statements = [];
  // Only a notice about its own experiment replaces that experiment's older pending notice.
  if (notify.targets.length === 1 && notify.targets[0] === key)
    statements.push(
      env.DB.prepare(
        `UPDATE relay_push_outbox SET state='superseded' WHERE key=? AND revision<? AND state='pending' AND ${live}`,
      ).bind(key, revision, key, revision),
    );
  statements.push(
    env.DB.prepare(
      `INSERT OR IGNORE INTO relay_push_outbox(id,subscription_id,key,revision,payload,next_attempt_at,created_at)
      SELECT ? || ':' || f.subscription_id,f.subscription_id,MIN(f.key),?,json_set(?,'$.experiment_id',substr(MIN(f.key),12)),?,?
      FROM relay_follows f JOIN relay_projections p ON p.key=f.key
      WHERE f.key IN (SELECT value FROM json_each(?)) AND p.payload IS NOT NULL AND ${live}
      GROUP BY f.subscription_id`,
    ).bind(
      `${key}:${revision}`,
      revision,
      payload,
      now,
      now,
      JSON.stringify(notify.targets),
      key,
      revision,
    ),
  );
  return statements;
}
export function relayTombstoneStatements(env: Env, key: string, revision: number) {
  const gone =
    'EXISTS(SELECT 1 FROM relay_projections WHERE key=? AND revision=? AND payload IS NULL)';
  return [
    env.DB.prepare(`DELETE FROM relay_push_outbox WHERE key=? AND ${gone}`).bind(
      key,
      key,
      revision,
    ),
    env.DB.prepare(`DELETE FROM relay_follows WHERE key=? AND ${gone}`).bind(key, key, revision),
  ];
}

export async function drainPush(env: Env) {
  if (!pushConfigured(env)) return;
  const now = Date.now() / 1000;
  // Expired leases represent uncertain network outcomes. Replay is safe because
  // notifications replace the same experiment tag; never replay execution work.
  const rows = (
    await env.DB.prepare(
      `SELECT o.* FROM relay_push_outbox o
    WHERE o.state IN ('pending','dispatching') AND o.next_attempt_at<=?
    AND NOT EXISTS(SELECT 1 FROM relay_push_outbox busy WHERE busy.subscription_id=o.subscription_id
      AND busy.key=o.key AND busy.state='dispatching' AND busy.next_attempt_at>?)
    ORDER BY o.created_at LIMIT 8`,
    )
      .bind(now, now)
      .all<Record<string, unknown>>()
  ).results;
  await Promise.all(
    rows.map(async (row) => {
      const lease = crypto.randomUUID();
      const claimed = await env.DB.prepare(
        `UPDATE relay_push_outbox SET state='dispatching',lease=?,attempts=attempts+1,next_attempt_at=?
      WHERE id=? AND state IN ('pending','dispatching') AND next_attempt_at<=?
      AND NOT EXISTS(SELECT 1 FROM relay_push_outbox busy WHERE busy.subscription_id=? AND busy.key=?
        AND busy.state='dispatching' AND busy.next_attempt_at>?) RETURNING attempts`,
      )
        .bind(lease, now + 60, row.id, now, row.subscription_id, row.key, now)
        .first<{ attempts: number }>();
      if (!claimed) return;
      const subscription = await env.DB.prepare(
        `SELECT s.subscription FROM push_subscriptions s
      JOIN relay_follows f ON f.subscription_id=s.id JOIN relay_projections p ON p.key=f.key
      WHERE s.id=? AND f.key=? AND p.payload IS NOT NULL`,
      )
        .bind(row.subscription_id, row.key)
        .first<{ subscription: string }>();
      if (!subscription) {
        await env.DB.prepare(
          "UPDATE relay_push_outbox SET state='superseded',lease=NULL WHERE id=? AND lease=?",
        )
          .bind(row.id, lease)
          .run();
        return;
      }
      let status = 0,
        retryAfter = 0;
      try {
        const sub = pushSubscriptionSchema.parse(JSON.parse(subscription.subscription));
        const data = JSON.parse(String(row.payload));
        const payload = await buildPushPayload(
          { data, options: { ttl: 3600, urgency: data.alert ? 'normal' : 'low' } },
          sub,
          {
            publicKey: env.VAPID_PUBLIC_KEY,
            privateKey: env.VAPID_PRIVATE_KEY,
            subject: env.VAPID_SUBJECT,
          },
        );
        const response = await fetch(sub.endpoint, {
          ...payload,
          redirect: 'manual',
          signal: AbortSignal.timeout(8000),
        });
        status = response.status;
        const retry = response.headers.get('retry-after');
        if (retry)
          retryAfter = /^\d+$/.test(retry)
            ? Number(retry)
            : Math.max(0, (Date.parse(retry) - Date.now()) / 1000);
        await response.body?.cancel();
      } catch {
        /* Never log endpoint, payload, keys or a network error containing a URL. */
      }
      if (status === 404 || status === 410) {
        await env.DB.prepare('DELETE FROM push_subscriptions WHERE id=?')
          .bind(row.subscription_id)
          .run();
        return;
      }
      const sent = status >= 200 && status < 300;
      const transient = status === 0 || status === 408 || status === 429 || status >= 500;
      const state = sent ? 'sent' : transient && claimed.attempts < 8 ? 'pending' : 'failed';
      const delay = Math.min(
        86400,
        Math.max(
          Number.isFinite(retryAfter) ? retryAfter : 0,
          15 * 2 ** Math.min(claimed.attempts, 10),
        ),
      );
      await env.DB.prepare(
        'UPDATE relay_push_outbox SET state=?,lease=NULL,last_status=?,next_attempt_at=? WHERE id=? AND lease=?',
      )
        .bind(state, status, Date.now() / 1000 + delay, row.id, lease)
        .run();
    }),
  );
  await env.DB.prepare(
    "DELETE FROM relay_push_outbox WHERE id IN (SELECT id FROM relay_push_outbox WHERE state IN ('sent','failed','superseded') AND created_at<? LIMIT 100)",
  )
    .bind(now - 7 * 86400)
    .run();
}
