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
export function pushStatements(env: Env, exp: PushExperiment, previous: PushExperiment | null) {
  if (previous && previous.revision >= exp.revision) return [];
  if (exp.deleted_at)
    return [
      env.DB.prepare(
        'DELETE FROM push_outbox WHERE experiment_id=? AND EXISTS(SELECT 1 FROM projections WHERE id=? AND revision=? AND deleted_at IS NOT NULL)',
      ).bind(exp.id, exp.id, exp.revision),
      env.DB.prepare(
        'DELETE FROM push_follows WHERE experiment_id=? AND EXISTS(SELECT 1 FROM projections WHERE id=? AND revision=? AND deleted_at IS NOT NULL)',
      ).bind(exp.id, exp.id, exp.revision),
    ];
  const notice = experimentNotification(exp);
  if (previous && experimentNotification(previous).fingerprint === notice.fingerprint) return [];
  const now = Date.now() / 1000;
  return [
    env.DB.prepare(
      "UPDATE push_outbox SET state='superseded' WHERE experiment_id=? AND revision<? AND state='pending' AND EXISTS(SELECT 1 FROM projections WHERE id=? AND revision=?)",
    ).bind(exp.id, exp.revision, exp.id, exp.revision),
    env.DB.prepare(
      `INSERT OR IGNORE INTO push_outbox(id,subscription_id,experiment_id,revision,payload,next_attempt_at,created_at)
      SELECT ? || ':' || f.subscription_id,f.subscription_id,f.experiment_id,?,?,?,?
      FROM push_follows f JOIN projections p ON p.id=f.experiment_id
      WHERE p.id=? AND p.revision=? AND p.deleted_at IS NULL`,
    ).bind(
      `${exp.id}:${exp.revision}`,
      exp.revision,
      JSON.stringify(notice.data),
      now,
      now,
      exp.id,
      exp.revision,
    ),
  ];
}

export async function drainPush(env: Env) {
  if (!pushConfigured(env)) return;
  const now = Date.now() / 1000;
  // Expired leases represent uncertain network outcomes. Replay is safe because
  // notifications replace the same experiment tag; never replay execution work.
  const rows = (
    await env.DB.prepare(
      `SELECT o.* FROM push_outbox o
    WHERE o.state IN ('pending','dispatching') AND o.next_attempt_at<=?
    AND NOT EXISTS(SELECT 1 FROM push_outbox busy WHERE busy.subscription_id=o.subscription_id
      AND busy.experiment_id=o.experiment_id AND busy.state='dispatching' AND busy.next_attempt_at>?)
    ORDER BY o.created_at LIMIT 8`,
    )
      .bind(now, now)
      .all<Record<string, unknown>>()
  ).results;
  await Promise.all(
    rows.map(async (row) => {
      const lease = crypto.randomUUID();
      const claimed = await env.DB.prepare(
        `UPDATE push_outbox SET state='dispatching',lease=?,attempts=attempts+1,next_attempt_at=?
      WHERE id=? AND state IN ('pending','dispatching') AND next_attempt_at<=?
      AND NOT EXISTS(SELECT 1 FROM push_outbox busy WHERE busy.subscription_id=? AND busy.experiment_id=?
        AND busy.state='dispatching' AND busy.next_attempt_at>?) RETURNING attempts`,
      )
        .bind(lease, now + 60, row.id, now, row.subscription_id, row.experiment_id, now)
        .first<{ attempts: number }>();
      if (!claimed) return;
      const subscription = await env.DB.prepare(
        `SELECT s.subscription FROM push_subscriptions s
      JOIN push_follows f ON f.subscription_id=s.id JOIN projections p ON p.id=f.experiment_id
      WHERE s.id=? AND p.id=? AND p.deleted_at IS NULL`,
      )
        .bind(row.subscription_id, row.experiment_id)
        .first<{ subscription: string }>();
      if (!subscription) {
        await env.DB.prepare(
          "UPDATE push_outbox SET state='superseded',lease=NULL WHERE id=? AND lease=?",
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
        'UPDATE push_outbox SET state=?,lease=NULL,last_status=?,next_attempt_at=? WHERE id=? AND lease=?',
      )
        .bind(state, status, Date.now() / 1000 + delay, row.id, lease)
        .run();
    }),
  );
  await env.DB.prepare(
    "DELETE FROM push_outbox WHERE id IN (SELECT id FROM push_outbox WHERE state IN ('sent','failed','superseded') AND created_at<? LIMIT 100)",
  )
    .bind(now - 7 * 86400)
    .run();
}
