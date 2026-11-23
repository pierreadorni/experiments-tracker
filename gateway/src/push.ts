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

// Only explicit published state changes affect this description. Activity deltas,
// observation times, elapsed timers and scheduler limits are not progress/ETA.
type PushExperiment = {
  id: string;
  revision: number;
  phase: string;
  hold_reason: string | null;
  title?: string | null;
  brief: string;
  deleted_at?: number | null;
  project_id?: string;
  contract?: { synthetic_demo?: boolean; project?: { id: string } };
  analysis?: { title?: string } | null;
  plan_review?: {
    state: string;
    revision: number;
    stages?: { id: string; title: string }[];
  } | null;
  pending_request?: { id: string; expired?: boolean; answer?: string } | null;
  stages?: { id: string; state: string }[];
  jobs?: { stage_id: string; scheduler_state: string | null; state: string }[];
  snapshot?: { plan?: { stages?: { id: string; title: string }[] } } | null;
  active_report_id?: string | null;
  report_versions?: { version: number; state: string }[];
};
const pretty = (s: string) => s.replaceAll('_', ' ');
export function experimentNotification(exp: PushExperiment) {
  let status = pretty(exp.phase),
    alert = false,
    identity = '';
  if (
    exp.pending_request &&
    !exp.pending_request.expired &&
    !exp.pending_request.answer &&
    !['failed', 'cancelled'].includes(exp.phase)
  ) {
    status = 'Your response is needed';
    alert = true;
    identity = exp.pending_request.id;
  } else if (exp.phase === 'complete') {
    status = 'Report ready';
    alert = true;
    identity =
      exp.active_report_id ||
      String(
        Math.max(
          1,
          ...(exp.report_versions || []).filter((r) => r.state === 'ready').map((r) => r.version),
        ),
      );
  } else if (exp.phase === 'failed') {
    status = 'Experiment failed';
    alert = true;
  } else if (exp.phase === 'cancelled') status = 'Experiment cancelled';
  else if (exp.plan_review?.state === 'awaiting_review') {
    status = 'Plan ready for your approval';
    alert = true;
    identity = String(exp.plan_review.revision);
  } else if (exp.hold_reason) status = `On hold · ${pretty(exp.hold_reason)}`;
  else {
    const activeJobs = (exp.jobs || []).filter((j) =>
      ['submitting', 'submitted'].includes(j.state),
    );
    // A finished branch may still have a running stage while outputs are checked.
    // Prefer actual running compute, then queued work, over those stage rows.
    const job =
      activeJobs.find((j) => j.scheduler_state === 'RUNNING') ||
      activeJobs.find((j) => j.scheduler_state === 'COMPLETING') ||
      activeJobs.find((j) => j.state === 'submitting') ||
      activeJobs.find((j) => ['PENDING', 'CONFIGURING'].includes(j.scheduler_state || '')) ||
      activeJobs.find((j) => !j.scheduler_state) ||
      activeJobs[0];
    const stage =
      exp.stages?.find((s) => s.id === job?.stage_id) ||
      exp.stages?.find((s) => ['running', 'dispatching', 'submitting'].includes(s.state));
    if (stage || job) {
      const stageId = job?.stage_id || stage!.id;
      const title =
        exp.snapshot?.plan?.stages?.find((s) => s.id === stageId)?.title ||
        exp.plan_review?.stages?.find((s) => s.id === stageId)?.title ||
        pretty(stageId);
      const state = job?.scheduler_state;
      const execution = !job
        ? (exp.jobs || []).some((j) => j.stage_id === stageId && j.state === 'complete')
          ? 'Checking outputs'
          : 'Running'
        : job.state === 'submitting'
          ? 'Submitting'
          : !state
            ? 'Waiting for scheduler'
            : state === 'PENDING' || state === 'CONFIGURING'
              ? 'Queued'
              : state === 'ACCOUNTING_PENDING' || state === 'COMPLETED'
                ? 'Waiting for accounting'
                : state === 'COMPLETING'
                  ? 'Finishing'
                  : state === 'RUNNING'
                    ? 'Running'
                    : `Scheduler: ${pretty(state.toLowerCase())}`;
      status = `${title} · ${execution}`;
    }
  }
  const stages = exp.stages || [];
  const count =
    stages.length && exp.phase !== 'complete'
      ? ` · ${stages.filter((s) => s.state === 'complete').length}/${stages.length} stages complete`
      : '';
  const body = `${exp.contract?.synthetic_demo ? 'Demo · ' : ''}${status}${count}`.slice(0, 240);
  const title = (exp.title || exp.analysis?.title || exp.brief || 'ExperimentTracker').slice(
    0,
    100,
  );
  const project = exp.project_id || exp.contract?.project?.id || 'test-project';
  return {
    fingerprint: JSON.stringify([body, identity]),
    data: {
      title,
      body,
      experiment_id: exp.id,
      revision: exp.revision,
      alert,
      url: `/?project=${encodeURIComponent(project)}&experiment=${encodeURIComponent(exp.id)}`,
      timestamp: Date.now(),
    },
  };
}
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
