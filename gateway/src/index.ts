import { z } from 'zod';
import { maintenanceRoute } from './maintenance';
import { assets } from './generated-assets';
import {
  pushConfigured,
  pushOwner,
  pushHash,
  pushEndpointSchema,
  pushFollowSchema,
  pushStatements,
  experimentNotification,
  drainPush,
} from './push';

export interface Env {
  DB: D1Database;
  BUCKET: R2Bucket;
  AUTH_MODE: 'sites' | 'token';
  USER_TOKEN?: string;
  EXECUTOR_TOKEN: string;
  EXECUTOR_ID: string;
  VAPID_PUBLIC_KEY?: string;
  VAPID_PRIVATE_KEY?: string;
  VAPID_SUBJECT?: string;
}
const uuid = z.string().uuid();
const base = { schema_version: z.literal(1).default(1), request_id: uuid };
const projectId = z.string().regex(/^[a-z][a-z0-9_-]{0,63}$/);
const executionSettingsSchema = z
  .object({
    context: z.enum(['acacia', 'jean-zay']),
    ssh_host: z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,99}$/),
    remote_project: z.string().max(1000),
    rsync_up: z.string().max(8000),
    rsync_down: z.string().max(8000),
    poll_seconds: z.number().int().min(600).max(86400),
  })
  .strict();
const publicProject = z
  .object({
    id: projectId,
    name: z.string().min(1).max(100),
    available: z.boolean(),
    writable: z.boolean().optional(),
    supports_worktrees: z.boolean().optional(),
    non_empty: z.boolean().optional(),
    folder: z.string().min(1).max(1000).optional(),
    instructions: z.string().max(8000).optional(),
    execution: executionSettingsSchema.optional(),
    revision: z.number().int().nonnegative().optional(),
  })
  .strict();
const createProjectSchema = z
  .object({
    ...base,
    name: z
      .string()
      .min(1)
      .max(80)
      .regex(/^[a-zA-Z0-9][a-zA-Z0-9 _.-]*$/),
    mode: z.enum(['new', 'existing']),
    folder: z.string().min(1).max(1000).optional(),
    allow_edits: z.literal(true),
    instructions: z.string().max(8000).optional(),
  })
  .strict()
  .refine(
    (p) => (p.mode === 'existing' ? Boolean(p.folder) : !p.folder),
    'Select an existing folder or create a new one',
  );
const projectSettingsSchema = z
  .object({
    delete: z.boolean().optional(),
    ...base,
    instructions: z.string().max(8000),
    execution: executionSettingsSchema.optional(),
  })
  .strict();
const projectSettingsResultSchema = z
  .object({
    deleted: z.boolean().optional(),
    instructions: z.string().max(8000),
    execution: executionSettingsSchema.optional(),
    revision: z.number().int().nonnegative(),
  })
  .strict();
const createSchema = z
  .object({
    ...base,
    project_id: projectId.optional(),
    execution: executionSettingsSchema.optional(),
    workspace_mode: z.enum(['auto', 'shared', 'worktree']).optional(),
    brief: z.string().min(10).max(4000),
    recipe_id: z.enum(['toy_compare_v1', 'agent_experiment_v1']),
    model: z
      .string()
      .regex(/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,99}$/)
      .optional(),
    reasoning_effort: z
      .string()
      .regex(/^[a-z][a-z0-9_-]{0,29}$/)
      .optional(),
    reference_experiment_id: uuid
      .nullable()
      .optional()
      .transform((value) => value ?? undefined),
  })
  .strict();
const deleteSchema = z.object(base).strict();
const noteSchema = z.object({ ...base, text: z.string().min(1).max(10000) }).strict();
const discussSchema = z
  .object({
    ...base,
    text: z
      .string()
      .min(1)
      .max(4000)
      .refine((text) => Boolean(text.trim()), 'Message must contain text'),
    model: createSchema.shape.model.nullable().optional(),
    reasoning_effort: createSchema.shape.reasoning_effort.nullable().optional(),
  })
  .strict();
const discussionSchema = z
  .object({
    id: uuid,
    seq: z.number().int().positive().safe(),
    text: z.string().min(1).max(4000),
    model: z.string().max(100).nullable(),
    reasoning_effort: z.string().max(30).nullable(),
    state: z.enum(['queued', 'waiting', 'running', 'completed', 'failed', 'uncertain']),
    hold_reason: z.string().max(100).nullable(),
    error: z.string().max(2000).nullable(),
    reply: z.string().min(1).max(8000).nullable(),
    evidence_ids: z.array(uuid).max(10),
    artifact_ids: z.array(uuid).optional(),
    followup_experiment_id: uuid.nullable().optional(),
    report_version: z.number().int().min(2).nullable(),
    created_at: z.number(),
    updated_at: z.number(),
    activity: z
      .array(z.object({ summary: z.string().max(1200), state: z.string().max(30) }).strict())
      .max(20),
  })
  .strict();
const actionSchema = z
  .object({
    ...base,
    action: z.enum([
      'pause_agent',
      'resume_agent',
      'pause_new_compute',
      'resume_new_compute',
      'summarize_title',
      'retry_agent',
      'cancel',
    ]),
  })
  .strict();
const answerSchema = z
  .object({
    ...base,
    pending_request_id: z.string().min(1).max(200),
    answer: z.string().min(1).max(4000),
  })
  .strict();
const planReviewSchema = z
  .object({
    ...base,
    decision: z.enum(['approve', 'revise', 'cancel']),
    feedback: z.string().max(4000).optional(),
    plan_revision: z.number().int().min(1).safe(),
  })
  .strict()
  .refine((value) => value.decision !== 'revise' || Boolean(value.feedback?.trim()), {
    message: 'Revision feedback is required',
    path: ['feedback'],
  });
const steeringSchema = z
  .object({
    ...base,
    text: z
      .string()
      .min(1)
      .max(50000)
      .refine((value) => Boolean(value.trim())),
    mode: z.enum(['later', 'restart']).default('later'),
  })
  .strict();
const reserveSchema = z
  .object({
    ...base,
    five_hour_percent: z.number().int().min(0).max(90),
    weekly_percent: z.number().int().min(0).max(90),
  })
  .strict();
const artifactSchema = z
  .object({
    id: uuid,
    name: z
      .string()
      .min(1)
      .max(200)
      .regex(/^[a-zA-Z0-9_][a-zA-Z0-9._-]*(?:\/[a-zA-Z0-9_][a-zA-Z0-9._-]*)*$/),
    sha256: z.string().regex(/^[a-f0-9]{64}$/),
    size: z
      .number()
      .int()
      .min(0)
      .max(10 * 1024 * 1024),
    media_type: z.enum([
      'text/html',
      'image/svg+xml',
      'application/json',
      'application/octet-stream',
      'image/png',
      'image/jpeg',
      'text/plain',
      'text/csv',
      'text/x-python',
    ]),
    local_valid: z.union([z.boolean(), z.literal(0), z.literal(1)]),
    remote_published: z.union([z.boolean(), z.literal(0), z.literal(1)]),
  })
  .passthrough();
const tensorboardSchema = z
  .object({
    scalar: z
      .object({
        tag: z.string().min(1).max(200),
        points: z.array(z.tuple([z.number().finite().nonnegative(), z.number().finite()])).max(160),
      })
      .strict()
      .nullable(),
    image: z
      .object({
        tag: z.string().min(1).max(200),
        step: z.number().finite().nonnegative(),
        wall_time: z.number().finite(),
        src: z
          .string()
          .max(214000)
          .regex(/^data:image\/jpeg;base64,[A-Za-z0-9+/]+={0,2}$/),
      })
      .strict()
      .nullable(),
    image_pending: z.boolean().optional(),
    updated_at: z.number().finite(),
    limited: z.boolean(),
  })
  .strict();
const projectionSchema = z
  .object({
    schema_version: z.literal(1),
    id: uuid,
    brief: z.string().max(4000),
    recipe_id: z.enum(['toy_compare_v1', 'agent_experiment_v1']),
    project_id: projectId.optional(),
    contract: z
      .object({
        project: z
          .object({
            id: projectId,
            name: z.string().min(1).max(100),
            writable: z.boolean().optional(),
          })
          .strict()
          .optional(),
      })
      .passthrough()
      .optional(),
    revision: z.number().int().min(0),
    phase: z.enum([
      'preparing',
      'training',
      'evaluating',
      'validating',
      'interpreting',
      'publishing',
      'complete',
      'failed',
      'cancelled',
      'waiting_for_executor',
    ]),
    hold_reason: z.string().nullable(),
    artifacts: z.array(artifactSchema).max(1000),
    jobs: z
      .array(
        z
          .object({
            stage_id: z.string(),
            state: z.string(),
            scheduler_state: z.string().nullable(),
            tensorboard: tensorboardSchema.nullable().optional(),
          })
          .passthrough(),
      )
      .max(1000)
      .optional(),
    partial_results: z
      .object({
        tasks: z.record(
          z.string(),
          z
            .object({
              intent_id: uuid,
              metric_count: z.number().int().nonnegative().optional(),
              metrics_truncated: z.boolean().optional(),
              result: z
                .object({
                  schema_version: z.literal(1),
                  protocol: z.string().max(8000).optional(),
                  metrics: z
                    .array(
                      z
                        .object({
                          name: z.string().max(200),
                          variant: z.string().max(200),
                          value: z.number().finite(),
                          unit: z.string().max(100),
                        })
                        .strict(),
                    )
                    .max(200),
                  sample_count: z.number().int().positive().nullable().optional(),
                })
                .strict(),
            })
            .strict(),
        ),
      })
      .strict()
      .nullable()
      .optional(),
    output_refs: z
      .array(
        z
          .object({
            id: uuid,
            intent_id: uuid,
            name: z.string().max(300),
            kind: z.enum(['file', 'directory']),
            created_at: z.number().finite(),
          })
          .strict(),
      )
      .optional(),
    conversation: z.array(discussionSchema).max(20).optional(),
    conversation_total: z.number().int().nonnegative().optional(),
    active_report_id: uuid.nullable().optional(),
    report_versions: z
      .array(
        z
          .object({
            version: z.number().int().positive(),
            state: z.enum(['queued', 'building', 'publishing', 'ready', 'failed']),
            artifact_ids: z.array(uuid).max(10),
            report_artifact_id: uuid.nullable(),
            error: z.string().max(2000).nullable(),
            created_at: z.number(),
            updated_at: z.number(),
          })
          .strict(),
      )
      .max(50)
      .optional(),
    created_at: z.number(),
    updated_at: z.number(),
    deleted_at: z.number().nonnegative().nullable().optional(),
  })
  .passthrough();
const inboxSchema = z
  .object({
    id: uuid,
    experiment_id: uuid,
    kind: z.enum([
      'result_ready',
      'required_decision',
      'recovery_exhausted',
      'command_rejected',
      'report_ready',
      'report_failed',
    ]),
    text: z.string().max(5000),
    created_at: z.number(),
  })
  .strict();
const messageSchema = z
  .object({
    schema_version: z.literal(1),
    event_id: uuid,
    experiment: projectionSchema,
    inbox: z.array(inboxSchema).max(20).default([]),
  })
  .strict();
const publishSchema = z
  .object({
    schema_version: z.literal(1),
    executor_id: z.string(),
    messages: z.array(messageSchema).max(20),
    heartbeat: z.record(z.string(), z.unknown()).optional(),
  })
  .strict();
const json = (value: unknown, status = 200, headers: HeadersInit = {}) =>
  new Response(JSON.stringify(value), {
    status,
    headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', ...headers },
  });
class HttpError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}
async function digest(value: string | ArrayBuffer) {
  const bytes = typeof value === 'string' ? new TextEncoder().encode(value) : value;
  return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256', bytes)))
    .map((v) => v.toString(16).padStart(2, '0'))
    .join('');
}
async function equal(a: string, b: string) {
  return (await digest(a)) === (await digest(b));
}
function cookie(request: Request, name: string) {
  return (
    request.headers
      .get('cookie')
      ?.split(';')
      .map((v) => v.trim())
      .find((v) => v.startsWith(name + '='))
      ?.slice(name.length + 1) || ''
  );
}
async function authorize(request: Request, env: Env, machine: boolean) {
  const bearer = request.headers.get('authorization')?.replace(/^Bearer /, '') || '';
  if (machine) {
    if (!env.EXECUTOR_TOKEN || !(await equal(bearer, env.EXECUTOR_TOKEN)))
      throw new HttpError(401, 'Executor credential required');
    return;
  }
  if (env.AUTH_MODE === 'sites') {
    if (!request.headers.get('oai-authenticated-user-id'))
      throw new HttpError(401, 'ChatGPT sign-in required');
  } else if (env.AUTH_MODE === 'token') {
    if (!env.USER_TOKEN || !(await equal(bearer || cookie(request, 'et_session'), env.USER_TOKEN)))
      throw new HttpError(401, 'User credential required');
  } else throw new HttpError(503, 'Authentication is not configured');
  if (
    !['GET', 'HEAD', 'OPTIONS'].includes(request.method) &&
    (env.AUTH_MODE === 'sites' || !bearer)
  ) {
    if (request.headers.get('origin') !== new URL(request.url).origin)
      throw new HttpError(403, 'Same-origin request required');
  }
}
async function boundedBytes(request: Request, max: number): Promise<ArrayBuffer> {
  if (Number(request.headers.get('content-length') || 0) > max)
    throw new HttpError(413, 'Payload too large');
  if (!request.body) return new ArrayBuffer(0);
  const reader = request.body.getReader(),
    chunks: Uint8Array[] = [],
    lengths: number[] = [];
  let total = 0;
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > max) {
        await reader.cancel();
        throw new HttpError(413, 'Payload too large');
      }
      chunks.push(value);
      lengths.push(value.byteLength);
    }
  } finally {
    reader.releaseLock();
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (let i = 0; i < chunks.length; i++) {
    bytes.set(chunks[i], offset);
    offset += lengths[i];
  }
  return bytes.buffer;
}
async function body(request: Request, max = 1024 * 1024) {
  const bytes = await boundedBytes(request, max);
  try {
    return JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    throw new HttpError(400, 'Invalid JSON');
  }
}
async function first(env: Env, sql: string, ...args: unknown[]) {
  return env.DB.prepare(sql)
    .bind(...args)
    .first<Record<string, unknown>>();
}
async function all(env: Env, sql: string, ...args: unknown[]) {
  return (
    await env.DB.prepare(sql)
      .bind(...args)
      .all<Record<string, unknown>>()
  ).results;
}
async function requiredProjection(env: Env, id: string) {
  const row = await first(env, 'SELECT * FROM projections WHERE id=?', id);
  if (!row) throw new HttpError(404, 'Unknown experiment');
  if (row.deleted_at) throw new HttpError(410, 'Experiment deleted');
  return row;
}
function waiting(
  id: string,
  brief: string,
  referenceId?: string,
  recipeId = 'agent_experiment_v1',
  agent?: { model?: string; reasoning_effort?: string },
  project = { id: 'test-project', name: 'Test project' },
) {
  const now = Date.now() / 1000;
  return {
    schema_version: 1,
    id,
    brief,
    project_id: project.id,
    recipe_id: recipeId,
    reference_experiment_id: referenceId || null,
    phase: 'waiting_for_executor',
    hold_reason: 'waiting_for_executor',
    revision: 0,
    evidence_revision: 1,
    created_at: now,
    updated_at: now,
    observed_at: null,
    pause_agent: false,
    pause_compute: false,
    error: null,
    model: null,
    scientific_outcome: null,
    pending_request: null,
    analysis: null,
    contract: {
      schema_version: 1,
      project: { id: project.id, name: project.name },
      brief_driven: recipeId === 'agent_experiment_v1',
      ...(agent ? { agent } : {}),
    },
    snapshot: null,
    stages: (recipeId === 'agent_experiment_v1'
      ? ['prepare', 'validate', 'interpret', 'publish']
      : [
          'prepare',
          'train_linear',
          'train_quadratic',
          'evaluate_and_plot',
          'validate',
          'interpret',
          'publish',
        ]
    ).map((id) => ({ id, state: 'pending', error: null, updated_at: now })),
    jobs: [],
    artifacts: [],
    notes: [],
    events: [],
    agent_actions: [],
  };
}
async function view(env: Env, row: Record<string, unknown>) {
  const projection = JSON.parse(String(row.payload));
  const gatewayNotes = await all(
    env,
    'SELECT * FROM notes WHERE experiment_id=? ORDER BY created_at',
    row.id,
  );
  const combined = new Map((projection.notes || []).map((n: { id: string }) => [n.id, n]));
  for (const note of gatewayNotes) combined.set(note.id, note);
  projection.notes = Array.from(combined.values()).sort(
    (a: any, b: any) => a.created_at - b.created_at,
  );
  const registered = await all(
    env,
    'SELECT id,available FROM artifacts WHERE experiment_id=?',
    row.id,
  );
  const available = new Map(registered.map((a) => [a.id, Boolean(a.available)]));
  projection.artifacts = (projection.artifacts || []).map((a: any) => ({
    ...a,
    remote_published: available.get(a.id) || false,
  }));
  projection.pending_commands = (
    await all(
      env,
      "SELECT request_id,kind,payload,received_at FROM commands WHERE experiment_id=? AND state='pending' ORDER BY received_at",
      row.id,
    )
  ).map((command) => ({
    request_id: command.request_id,
    kind: command.kind,
    received_at: command.received_at,
    action: command.kind === 'action' ? JSON.parse(String(command.payload)).action : null,
  }));
  projection.pending_messages = (
    await all(
      env,
      "SELECT request_id,payload,received_at FROM commands WHERE experiment_id=? AND kind='discuss' AND state='pending' ORDER BY received_at",
      row.id,
    )
  ).map((command) => ({
    id: command.request_id,
    text: JSON.parse(String(command.payload)).text,
    created_at: command.received_at,
  }));
  return projection;
}
async function projectCatalog(env: Env) {
  const row = await first(env, 'SELECT payload FROM executor_status WHERE id=?', env.EXECUTOR_ID);
  const published = z
    .array(publicProject)
    .max(50)
    .parse(
      row
        ? JSON.parse(String(row.payload)).projects || [
            { id: 'test-project', name: 'Test project', available: true },
          ]
        : [{ id: 'test-project', name: 'Test project', available: true }],
    );
  const saved = await all(
    env,
    "SELECT result FROM project_requests WHERE executor_id=? AND state='applied' ORDER BY received_at",
    env.EXECUTOR_ID,
  );
  const projects = new Map(
    saved.map((p) => {
      const project = publicProject.parse(JSON.parse(String(p.result)));
      return [project.id, project] as const;
    }),
  );
  for (const project of published) projects.set(project.id, project);
  const settings = await all(
    env,
    "SELECT project_id,result FROM project_settings_requests WHERE executor_id=? AND state='applied' ORDER BY received_at",
    env.EXECUTOR_ID,
  );
  for (const row of settings) {
    const current = projects.get(String(row.project_id));
    if (!current) continue;
    const value = projectSettingsResultSchema.parse(JSON.parse(String(row.result)));
    if (value.deleted) {
      projects.delete(current.id);
      continue;
    }
    if (value.revision >= (current.revision || 0))
      projects.set(current.id, { ...current, ...value });
  }
  return Array.from(projects.values());
}
function projectReceipt(row: Record<string, unknown>) {
  return {
    schema_version: 1,
    request_id: row.request_id,
    project_id: row.project_id,
    state: row.state === 'pending' ? 'waiting_for_executor' : row.state,
    error: row.error || null,
    project: row.result ? JSON.parse(String(row.result)) : null,
  };
}
function projectSettingsReceipt(row: Record<string, unknown>) {
  return {
    schema_version: 1,
    request_id: row.request_id,
    project_id: row.project_id,
    state: row.state === 'pending' ? 'waiting_for_executor' : row.state,
    error: row.error || null,
    settings: row.result ? JSON.parse(String(row.result)) : null,
  };
}
const experimentProject = (exp: any) =>
  exp.project_id || exp.contract?.project?.id || 'test-project';
async function enqueue(env: Env, kind: string, payload: Record<string, unknown>, eid?: string) {
  if (await first(env, 'SELECT 1 FROM settings_commands WHERE request_id=?', payload.request_id))
    throw new HttpError(409, 'request_id was already used for another command');
  if (
    await first(
      env,
      'SELECT 1 FROM project_settings_requests WHERE request_id=?',
      payload.request_id,
    )
  )
    throw new HttpError(409, 'request_id was already used for another command');
  if (await first(env, 'SELECT 1 FROM project_requests WHERE request_id=?', payload.request_id))
    throw new HttpError(409, 'request_id was already used for another command');
  const prior = await first(env, 'SELECT * FROM commands WHERE request_id=?', payload.request_id);
  const wire = JSON.stringify(payload);
  if (prior) {
    if (prior.kind !== kind || prior.payload !== wire || (eid && prior.experiment_id !== eid))
      throw new HttpError(409, 'request_id already used for another command');
    if (prior.state === 'rejected') throw new HttpError(409, String(prior.error));
    return {
      schema_version: 1,
      experiment_id: prior.experiment_id,
      request_id: payload.request_id,
      state: prior.state === 'acked' ? 'applied' : 'waiting_for_executor',
    };
  }
  const id = eid || crypto.randomUUID(),
    now = Date.now() / 1000;
  const statements = [];
  if (kind === 'create')
    statements.push(
      env.DB.prepare(
        'INSERT INTO projections(id,revision,payload,updated_at) VALUES(?,0,?,?)',
      ).bind(
        id,
        JSON.stringify(
          waiting(
            id,
            String(payload.brief),
            payload.reference_experiment_id as string | undefined,
            String(payload.recipe_id),
            {
              model: payload.model as string | undefined,
              reasoning_effort: payload.reasoning_effort as string | undefined,
            },
            (await projectCatalog(env)).find(
              (p) => p.id === (payload.project_id || 'test-project'),
            ),
          ),
        ),
        now,
      ),
    );
  else {
    const row = await requiredProjection(env, id);
    const exp = JSON.parse(String(row.payload));
    if (
      kind === 'action' &&
      payload.action === 'retry_agent' &&
      exp.hold_reason !== 'agent_session_unavailable' &&
      (exp.phase !== 'failed' ||
        exp.recipe_id !== 'agent_experiment_v1' ||
        !(
          (exp.snapshot &&
            ['compute_failed', 'validation_failed', 'agent_recovery_exhausted'].includes(
              exp.hold_reason,
            )) ||
          [
            'agent_recovery_exhausted',
            'agent_correction_stalled',
            'agent_budget_exhausted',
          ].includes(exp.hold_reason)
        ))
    )
      throw new HttpError(
        409,
        'Agent retry requires a held session or confirmed failed agent/compute work',
      );
    if (kind === 'action' && payload.action === 'cancel') {
      const owner = await first(
        env,
        'SELECT payload FROM executor_status WHERE id=?',
        env.EXECUTOR_ID,
      );
      if (!owner || !JSON.parse(String(owner.payload)).experiment_cancellation)
        throw new HttpError(409, 'Install the executor update to enable cancellation');
      if (exp.phase === 'complete')
        throw new HttpError(409, 'A completed experiment cannot be cancelled');
    }
    if (kind === 'discuss' && exp.phase !== 'complete')
      throw new HttpError(409, 'Discussion is available when the experiment is complete');
    if (
      kind === 'plan_review' &&
      (exp.plan_review?.state !== 'awaiting_review' ||
        exp.plan_review?.revision !== payload.plan_revision)
    )
      throw new HttpError(409, 'Plan review has changed or is no longer awaiting a decision');
    if (
      kind === 'steer' &&
      ['complete', 'failed', 'cancelled'].includes(exp.phase) &&
      !(exp.phase === 'failed' && payload.mode === 'restart')
    )
      throw new HttpError(
        409,
        'Steering requires an active experiment or a restart of a failed run',
      );
    if (kind === 'delete') {
      if (
        ['cancellation_pending', 'cancellation_unconfirmed'].includes(exp.hold_reason) ||
        !['complete', 'failed', 'cancelled'].includes(exp.phase) ||
        exp.jobs?.some((job: any) => ['submitting', 'submitted'].includes(job.state)) ||
        exp.report_versions?.some((report: any) =>
          ['queued', 'building', 'publishing'].includes(report.state),
        ) ||
        exp.agent_actions?.some((action: any) =>
          ['dispatching', 'running', 'uncertain'].includes(action.state),
        ) ||
        exp.conversation?.some((message: any) =>
          ['running', 'uncertain'].includes(message.state),
        ) ||
        (await first(
          env,
          "SELECT 1 FROM commands WHERE experiment_id=? AND kind='discuss' AND state='pending'",
          id,
        ))
      )
        throw new HttpError(409, 'Finish or reconcile active work before deleting the experiment');
    }
  }
  statements.push(
    env.DB.prepare(
      "INSERT INTO commands(request_id,experiment_id,executor_id,kind,payload,state,received_at) VALUES(?,?,?,?,?,'pending',?)",
    ).bind(payload.request_id, id, env.EXECUTOR_ID, kind, wire, now),
  );
  if (kind === 'note')
    statements.push(
      env.DB.prepare(
        'INSERT INTO notes(id,experiment_id,author,text,created_at) VALUES(?,?,?,?,?)',
      ).bind(payload.request_id, id, 'researcher', payload.text, now),
    );
  try {
    await env.DB.batch(statements);
  } catch (error) {
    // Two concurrent retries may both see no prior row. Re-read the unique receipt after the batch loser rolls back.
    const saved = await first(env, 'SELECT * FROM commands WHERE request_id=?', payload.request_id);
    if (
      saved &&
      saved.kind === kind &&
      saved.payload === wire &&
      (!eid || saved.experiment_id === eid)
    )
      return {
        schema_version: 1,
        experiment_id: saved.experiment_id,
        request_id: payload.request_id,
        state:
          saved.state === 'acked'
            ? 'applied'
            : saved.state === 'rejected'
              ? 'rejected'
              : 'waiting_for_executor',
      };
    throw error;
  }
  return {
    schema_version: 1,
    experiment_id: id,
    request_id: payload.request_id,
    state: 'waiting_for_executor',
  };
}
async function enqueueReserve(env: Env, payload: z.infer<typeof reserveSchema>) {
  if (
    (await first(env, 'SELECT 1 FROM commands WHERE request_id=?', payload.request_id)) ||
    (await first(env, 'SELECT 1 FROM project_requests WHERE request_id=?', payload.request_id)) ||
    (await first(
      env,
      'SELECT 1 FROM project_settings_requests WHERE request_id=?',
      payload.request_id,
    ))
  )
    throw new HttpError(409, 'request_id was already used for another command');
  const wire = JSON.stringify(payload);
  const receipt = (row: Record<string, unknown>) => ({
    schema_version: 1,
    request_id: row.request_id,
    state:
      row.state === 'acked'
        ? 'applied'
        : row.state === 'pending'
          ? 'waiting_for_executor'
          : row.state,
    error: row.error || null,
  });
  let saved = await first(
    env,
    'SELECT * FROM settings_commands WHERE request_id=?',
    payload.request_id,
  );
  if (saved) {
    if (saved.kind !== 'reserve' || saved.payload !== wire)
      throw new HttpError(409, 'request_id already used for another command');
    return receipt(saved);
  }
  try {
    await env.DB.prepare(
      "INSERT INTO settings_commands(request_id,executor_id,kind,payload,state,received_at) VALUES(?,?,?,?,'pending',?)",
    )
      .bind(payload.request_id, env.EXECUTOR_ID, 'reserve', wire, Date.now() / 1000)
      .run();
  } catch (error) {
    saved = await first(
      env,
      'SELECT * FROM settings_commands WHERE request_id=?',
      payload.request_id,
    );
    if (!saved || saved.kind !== 'reserve' || saved.payload !== wire) throw error;
    return receipt(saved);
  }
  return receipt({ request_id: payload.request_id, state: 'pending', error: null });
}
async function publish(env: Env, value: z.infer<typeof publishSchema>) {
  if (value.executor_id !== env.EXECUTOR_ID) throw new HttpError(403, 'Executor identity mismatch');
  const statements = [],
    accepted = [];
  for (const message of value.messages) {
    const exp = message.experiment;
    const priorEvent = await first(
      env,
      'SELECT experiment_id,revision FROM events WHERE id=?',
      message.event_id,
    );
    if (
      priorEvent &&
      (priorEvent.experiment_id !== exp.id || Number(priorEvent.revision) !== exp.revision)
    )
      throw new HttpError(409, 'Event identity is immutable');
    if (
      new Set(exp.artifacts.map((a) => a.id)).size !== exp.artifacts.length ||
      (exp.contract?.execution_contract !== 3 &&
        new Set(exp.artifacts.map((a) => a.name)).size !== exp.artifacts.length)
    )
      throw new HttpError(422, 'Artifact identities and names must be unique');
    const existing = await first(
      env,
      'SELECT revision,deleted_at,payload FROM projections WHERE id=?',
      exp.id,
    );
    if (exp.project_id && exp.contract?.project?.id && exp.project_id !== exp.contract.project.id)
      throw new HttpError(409, 'Project identity mismatch');
    if (
      existing &&
      experimentProject(JSON.parse(String(existing.payload))) !== experimentProject(exp)
    )
      throw new HttpError(409, 'Experiment project is immutable');
    const derived = new Set<string>();
    // Analysis output registration precedes its upload while the scientific result stays complete.
    // The namespace plus saved stage identity distinguishes these from original scientific evidence,
    // including old discussion outputs whose messages have moved beyond the latest projection page.
    for (const artifact of exp.artifacts) {
      const analysis = /^discussion-([a-f0-9-]{36})-[a-zA-Z0-9][a-zA-Z0-9_.-]*$/.exec(
        artifact.name,
      );
      if (analysis && artifact.stage_id === `discuss:${analysis[1]}`) derived.add(artifact.id);
    }
    const reportVersions = exp.report_versions || [];
    if (new Set(reportVersions.map((r) => r.version)).size !== reportVersions.length)
      throw new HttpError(422, 'Duplicate report versions');
    const reportAvailability = new Set(
      (await all(env, 'SELECT id,available FROM artifacts WHERE experiment_id=?', exp.id))
        .filter((a) => a.available)
        .map((a) => a.id),
    );
    const readyReports = [];
    for (const report of reportVersions) {
      if (
        report.artifact_ids.length &&
        (!report.report_artifact_id || !report.artifact_ids.includes(report.report_artifact_id))
      )
        throw new HttpError(422, 'Report manifest does not include its HTML');
      for (const id of report.artifact_ids) {
        const artifact = exp.artifacts.find((a) => a.id === id);
        const prefix = `report-v${report.version}`;
        if (
          !artifact ||
          !(
            artifact.name === `${prefix}.json` ||
            artifact.name === `${prefix}.html` ||
            (report.version === 1 && artifact.name === 'report.html') ||
            new RegExp(`^${prefix}-figure-[1-8]\\.svg$`).test(artifact.name)
          )
        )
          throw new HttpError(422, 'Report output must have its versioned identity');
        if (
          id === report.report_artifact_id &&
          artifact.name !== (report.version === 1 ? 'report.html' : `${prefix}.html`)
        )
          throw new HttpError(422, 'Report HTML identity differs');
        if (report.version > 1) derived.add(id);
      }
      if (report.state === 'ready') {
        if (
          !report.artifact_ids.length ||
          !report.artifact_ids.every((id) => reportAvailability.has(id))
        )
          throw new HttpError(409, 'Report outputs are not published');
        readyReports.push(report);
      }
    }
    readyReports.sort((a, b) => a.version - b.version);
    if (exp.active_report_id && exp.active_report_id !== readyReports.at(-1)?.report_artifact_id)
      throw new HttpError(409, 'Active report must reference the latest ready version');
    if (
      !exp.deleted_at &&
      exp.phase === 'complete' &&
      (!existing || exp.revision > Number(existing.revision))
    ) {
      const registered = await all(
        env,
        'SELECT id,name,available FROM artifacts WHERE experiment_id=?',
        exp.id,
      );
      const available = new Set(registered.filter((a) => a.available).map((a) => a.id));
      const names = new Set(exp.artifacts.filter((a) => available.has(a.id)).map((a) => a.name));
      if (!exp.artifacts.filter((a) => !derived.has(a.id)).every((a) => available.has(a.id)))
        throw new HttpError(409, 'Referenced artifacts are not published');
      let required = [
        'linear.json',
        'quadratic.json',
        'results.json',
        'predictions.json',
        'predictions.svg',
        'analysis.json',
        'report.html',
      ];
      if (exp.recipe_id === 'agent_experiment_v1' && exp.contract?.execution_contract !== 3) {
        const snapshot = z
          .object({
            files: z.record(z.string(), z.string().regex(/^[a-f0-9]{64}$/)),
            plan: z.object({
              stages: z
                .array(
                  z.object({
                    id: z.string().regex(/^[a-z][a-z0-9_]{0,47}$/),
                    outputs: z
                      .array(z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,149}$/))
                      .max(100),
                    script: z
                      .string()
                      .regex(/^[a-zA-Z0-9][a-zA-Z0-9._-]*\.sh$/)
                      .nullable()
                      .optional(),
                    local_outputs: z
                      .array(z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9._/-]*$/))
                      .max(100)
                      .optional(),
                  }),
                )
                .min(1)
                .max(256),
            }),
          })
          .parse(exp.snapshot);
        if (
          snapshot.plan.stages.some(
            (s) => !(s.script ? s.script in snapshot.files : 'recipe.py' in snapshot.files),
          )
        )
          throw new HttpError(422, 'Stage entrypoint is absent from frozen sources');
        required = [
          'requirements.txt',
          'manifest.json',
          'preparation.json',
          'results.json',
          'validation.json',
          'analysis.json',
          'report.html',
          ...Object.keys(snapshot.files),
          ...snapshot.plan.stages.flatMap((s) => s.outputs),
          ...snapshot.plan.stages
            .filter((s) => s.local_outputs?.length)
            .map((s) => `local-output-${s.id}.json`),
        ];
      }
      if (exp.contract?.execution_contract === 3) {
        const plan = z
          .object({
            stages: z
              .array(
                z.object({
                  id: z.string(),
                  requested: z.boolean().optional(),
                  outputs: z.array(z.string()),
                }),
              )
              .max(256),
          })
          .parse((exp.snapshot as { plan: unknown }).plan);
        required = [
          'preparation.json',
          'results.json',
          'analysis.json',
          'report.html',
          ...plan.stages.filter((s) => s.requested !== false).flatMap((s) => s.outputs),
        ];
      }
      if (!required.every((n) => names.has(n)))
        throw new HttpError(409, 'Required artifacts are not published');
    }
    if (existing?.deleted_at && !exp.deleted_at && exp.revision > Number(existing.revision))
      throw new HttpError(409, 'Deleted experiments cannot be restored by a later projection');
    for (const [stage, task] of Object.entries(exp.partial_results?.tasks || {})) {
      if (
        !exp.jobs?.some(
          (job) =>
            job.intent_id === task.intent_id && job.stage_id === stage && job.state === 'complete',
        )
      )
        throw new HttpError(422, 'Metric result must identify its successful producing job');
    }
    // Never store absolute executor locations in the hosted projection.
    for (const artifact of exp.artifacts) {
      if ('path' in artifact) throw new HttpError(400, 'Executor paths must not be published');
      const prior = await first(
        env,
        'SELECT experiment_id,name,sha256,size,media_type FROM artifacts WHERE id=?',
        artifact.id,
      );
      if (
        prior &&
        (prior.experiment_id !== exp.id ||
          prior.name !== artifact.name ||
          prior.media_type !== artifact.media_type ||
          prior.sha256 !== artifact.sha256 ||
          Number(prior.size) !== artifact.size)
      )
        throw new HttpError(409, 'Artifact identity is immutable');
    }
    statements.push(
      env.DB.prepare(
        'INSERT INTO projections(id,revision,payload,updated_at,deleted_at) VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET revision=excluded.revision,payload=excluded.payload,updated_at=excluded.updated_at,deleted_at=COALESCE(projections.deleted_at,excluded.deleted_at) WHERE excluded.revision>projections.revision',
      ).bind(exp.id, exp.revision, JSON.stringify(exp), Date.now() / 1000, exp.deleted_at ?? null),
    );
    statements.push(
      env.DB.prepare(
        'INSERT OR IGNORE INTO events(id,experiment_id,revision,received_at) VALUES(?,?,?,?)',
      ).bind(message.event_id, exp.id, exp.revision, Date.now() / 1000),
    );
    statements.push(
      ...pushStatements(env, exp, existing ? JSON.parse(String(existing.payload)) : null),
    );
    if (!existing || exp.revision > Number(existing.revision)) {
      const messages = exp.conversation || [];
      if (
        new Set(messages.map((m) => m.id)).size !== messages.length ||
        new Set(messages.map((m) => m.seq)).size !== messages.length
      )
        throw new HttpError(422, 'Conversation message identities must be unique');
      for (const message of messages) {
        if (message.state === 'completed' && !message.reply)
          throw new HttpError(422, 'Completed discussion needs its saved reply');
        if (!message.evidence_ids.every((id) => exp.artifacts.some((a) => a.id === id)))
          throw new HttpError(422, 'Discussion cites unknown evidence');
        if (!(message.artifact_ids || []).every((id) => exp.artifacts.some((a) => a.id === id)))
          throw new HttpError(422, 'Discussion contains unknown generated artifacts');
        if (message.followup_experiment_id === exp.id)
          throw new HttpError(422, 'Follow-up experiment must have a separate identity');
        if (
          message.report_version &&
          !reportVersions.some((r) => r.version === message.report_version)
        )
          throw new HttpError(422, 'Discussion report version is missing');
        const prior = await first(
          env,
          'SELECT experiment_id,seq,payload FROM conversation WHERE id=?',
          message.id,
        );
        if (prior) {
          const saved = JSON.parse(String(prior.payload));
          if (
            prior.experiment_id !== exp.id ||
            prior.seq !== message.seq ||
            saved.text !== message.text ||
            saved.created_at !== message.created_at
          )
            throw new HttpError(409, 'Conversation identity is immutable');
          if (
            saved.state === 'completed' &&
            (message.state !== 'completed' ||
              saved.reply !== message.reply ||
              saved.report_version !== message.report_version ||
              JSON.stringify(saved.evidence_ids) !== JSON.stringify(message.evidence_ids) ||
              JSON.stringify(saved.artifact_ids || []) !==
                JSON.stringify(message.artifact_ids || []) ||
              (saved.followup_experiment_id || null) !== (message.followup_experiment_id || null))
          )
            throw new HttpError(409, 'Completed discussion is immutable');
        }
        const position = await first(
          env,
          'SELECT id FROM conversation WHERE experiment_id=? AND seq=?',
          exp.id,
          message.seq,
        );
        if (position && position.id !== message.id)
          throw new HttpError(409, 'Conversation order is immutable');
        statements.push(
          env.DB.prepare(
            'INSERT INTO conversation(id,experiment_id,seq,revision,payload) VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET revision=excluded.revision,payload=excluded.payload WHERE excluded.revision>conversation.revision',
          ).bind(message.id, exp.id, message.seq, exp.revision, JSON.stringify(message)),
        );
      }
    }
    for (const artifact of exp.artifacts)
      statements.push(
        env.DB.prepare(
          'INSERT OR IGNORE INTO artifacts(id,experiment_id,name,sha256,size,media_type,object_key,available) VALUES(?,?,?,?,?,?,?,0)',
        ).bind(
          artifact.id,
          exp.id,
          artifact.name,
          artifact.sha256,
          artifact.size,
          artifact.media_type,
          `${exp.id}/${artifact.sha256}/${artifact.id}`,
        ),
      );
    for (const entry of message.inbox) {
      if (entry.experiment_id !== exp.id) throw new HttpError(400, 'Inbox experiment mismatch');
      if (entry.kind === 'result_ready' && exp.phase !== 'complete') continue;
      if (existing && exp.revision < Number(existing.revision)) continue;
      statements.push(
        env.DB.prepare(
          'INSERT OR IGNORE INTO inbox(id,experiment_id,kind,text,created_at) VALUES(?,?,?,?,?)',
        ).bind(entry.id, exp.id, entry.kind, entry.text, entry.created_at),
      );
    }
    accepted.push(message.event_id);
  }
  if (value.heartbeat)
    statements.push(
      env.DB.prepare(
        'INSERT INTO executor_status(id,payload,received_at) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload,received_at=excluded.received_at',
      ).bind(env.EXECUTOR_ID, JSON.stringify(value.heartbeat), Date.now() / 1000),
    );
  if (statements.length) await env.DB.batch(statements);
  return { schema_version: 1, accepted };
}
const memoryCommandSchema = z
  .object({
    schema_version: z.literal(1).default(1),
    request_id: uuid,
    kind: z.enum(['synthesize', 'chat', 'schedule', 'dismiss_idea']),
    text: z.string().max(8000).default(''),
    experiment_id: uuid.optional(),
    weekly: z.boolean().nullable().default(null),
    model: z
      .string()
      .regex(/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,99}$/)
      .nullable()
      .default(null),
    reasoning_effort: z
      .string()
      .regex(/^[a-z][a-z0-9_-]{0,29}$/)
      .nullable()
      .default(null),
  })
  .strict()
  .refine(
    (v) =>
      (v.kind !== 'chat' || v.text.trim().length > 0) &&
      (v.kind === 'schedule') === (v.weekly !== null) &&
      (v.kind === 'dismiss_idea') === (v.experiment_id !== undefined) &&
      (v.kind !== 'dismiss_idea' || v.text.trim().length > 0),
  );
const memoryMetricReferenceSchema = z
  .object({
    experiment_id: uuid,
    name: z.string().min(1).max(200),
    variant: z.string().min(1).max(200),
    decimals: z.number().int().min(0).max(8),
  })
  .strict();
const memoryBlockSchema = z
  .object({
    kind: z.enum(['paragraph', 'table', 'figure']),
    text: z.string().min(1).max(6000).nullable().optional(),
    caption: z.string().min(1).max(1500).nullable().optional(),
    alt: z.string().min(1).max(500).nullable().optional(),
    experiment_id: uuid.nullable().optional(),
    artifact_id: uuid.nullable().optional(),
    columns: z.array(z.string()).min(2).max(6).nullable().optional(),
    rows: z
      .array(
        z
          .object({
            cells: z
              .array(
                z
                  .object({
                    text: z.string().max(300).nullable().optional(),
                    metric: memoryMetricReferenceSchema.nullable().optional(),
                    display: z.string().nullable().optional(),
                  })
                  .strict(),
              )
              .min(1)
              .max(6),
            experiment_ids: z.array(uuid).min(1).max(20),
          })
          .strict(),
      )
      .min(1)
      .max(20)
      .nullable()
      .optional(),
  })
  .strict();
const memoryReportSchema = z
  .object({
    project_id: projectId,
    version: z.number().int().positive(),
    request_id: uuid,
    document: z
      .object({
        title: z.string().min(1).max(200),
        overview: z.string().min(1).max(16000),
        sections: z
          .array(
            z
              .object({
                title: z.string().min(1).max(200),
                body: z.string().min(1).max(16000),
                experiment_ids: z.array(uuid).max(500),
                blocks: z.array(memoryBlockSchema).max(20).optional(),
              })
              .strict(),
          )
          .min(1)
          .max(50),
        open_questions: z.array(z.string().max(16000)).max(50),
        source_scope: z.enum(['synthetic', 'mixed', 'research']).optional(),
      })
      .strict(),
    sources: z.record(uuid, z.string().regex(/^[a-f0-9]{64}$/)),
    created_at: z.number(),
  })
  .strict();
const memorySchema = z
  .object({
    schema_version: z.literal(1),
    project_id: projectId,
    revision: z.number().int().nonnegative(),
    weekly: z.boolean(),
    next_due: z.number().nullable(),
    report: memoryReportSchema.nullable(),
    summaries: z
      .array(
        z
          .object({
            id: uuid,
            title: z.string(),
            brief: z.string(),
            tldr: z.string(),
            limitations: z.array(z.string()),
            next_ideas: z.array(z.string()),
            synthetic: z.boolean(),
            evidence_revision: z.number().int().positive(),
            report_version: z.number().int().positive(),
            evidence: z.array(
              z
                .object({
                  id: z.string(),
                  name: z.string(),
                  sha256: z.string(),
                  size: z.number(),
                  media_type: z.string().optional(),
                })
                .strict(),
            ),
            metrics: z
              .array(
                z
                  .object({
                    name: z.string(),
                    variant: z.string(),
                    value: z.number().finite(),
                    unit: z.string(),
                  })
                  .strict(),
              )
              .max(100)
              .optional(),
            metrics_truncated: z.boolean().optional(),
            fingerprint: z.string().regex(/^[a-f0-9]{64}$/),
          })
          .strict(),
      )
      .max(500),
    dismissed_ideas: z
      .array(z.object({ experiment_id: uuid, text: z.string().max(8000) }).strict())
      .optional(),
    pending_experiment_ids: z.array(uuid).max(500),
    report_versions: z.array(z.number().int().positive()),
    messages: z
      .array(
        z
          .object({
            id: uuid,
            schema_version: z.literal(1),
            kind: z.enum(['synthesize', 'chat', 'schedule', 'dismiss_idea']),
            text: z.string().max(8000),
            experiment_id: uuid.optional(),
            weekly: z.boolean().nullable(),
            model: z.string().nullable(),
            reasoning_effort: z.string().nullable(),
            state: z.enum([
              'queued',
              'waiting',
              'dispatching',
              'running',
              'uncertain',
              'received',
              'completed',
              'failed',
            ]),
            hold_reason: z.string().nullable(),
            error: z.string().nullable(),
            reply: z.string().nullable(),
            report_version: z.number().int().positive().nullable(),
            created_at: z.number(),
            updated_at: z.number(),
          })
          .strict(),
      )
      .max(50),
  })
  .strict();

async function route(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url),
    p = url.pathname;
  if (p === '/healthz') return json({ ok: true });
  if (!p.startsWith('/api/')) {
    if (env.AUTH_MODE === 'sites' && !request.headers.get('oai-authenticated-user-id'))
      return Response.redirect(url.origin + '/signin-with-chatgpt?return_to=%2F', 302);
    const item = assets[p === '/' ? '/index.html' : p];
    if (!item) return new Response('Not found', { status: 404 });
    const assetBody =
      item.encoding === 'base64'
        ? Uint8Array.from(atob(item.body), (character) => character.charCodeAt(0))
        : item.body;
    return new Response(assetBody, {
      headers: {
        'Content-Type': item.type,
        'Cache-Control':
          p === '/'
            ? 'no-store'
            : ['/sw.js', '/manifest.webmanifest'].includes(p)
              ? 'no-cache'
              : 'public,max-age=3600',
        'Content-Security-Policy':
          "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'",
      },
    });
  }
  const machine = p.startsWith('/api/executor/');
  await authorize(request, env, machine);
  const maintenance = await maintenanceRoute(request, env, p);
  if (maintenance) return maintenance;
  const memoryPath = p.match(/^\/api\/projects\/([^/]+)\/knowledge(?:\/reports\/(\d+))?$/);
  if (memoryPath) {
    const pid = projectId.parse(memoryPath[1]);
    if (!(await projectCatalog(env)).some((project) => project.id === pid))
      throw new HttpError(404, 'Unknown project');
    if (request.method === 'GET') {
      if (memoryPath[2]) {
        const row = await first(
          env,
          'SELECT payload FROM project_memory_reports WHERE identity=?',
          `${pid}:${Number(memoryPath[2])}`,
        );
        if (!row) throw new HttpError(404, 'Unknown working report version');
        return json({ schema_version: 1, report: JSON.parse(String(row.payload)) });
      }
      const row = await first(
        env,
        'SELECT payload FROM project_memory WHERE project_id=? AND executor_id=?',
        pid,
        env.EXECUTOR_ID,
      );
      const value = row
        ? JSON.parse(String(row.payload))
        : {
            schema_version: 1,
            project_id: pid,
            revision: 0,
            weekly: true,
            next_due: null,
            report: null,
            summaries: [],
            pending_experiment_ids: [],
            report_versions: [],
            messages: [],
          };
      const commands = await all(
        env,
        'SELECT * FROM project_memory_commands WHERE project_id=? AND executor_id=? ORDER BY received_at DESC LIMIT 50',
        pid,
        env.EXECUTOR_ID,
      );
      for (const command of commands.reverse()) {
        const intent = JSON.parse(String(command.payload));
        if (intent.kind === 'dismiss_idea' && command.state !== 'rejected') {
          value.dismissed_ideas ??= [];
          if (
            !value.dismissed_ideas.some(
              (idea: any) =>
                idea.experiment_id === intent.experiment_id && idea.text === intent.text,
            )
          )
            value.dismissed_ideas.push({ experiment_id: intent.experiment_id, text: intent.text });
        }
        if (
          !value.messages.some((m: any) => m.id === command.request_id) &&
          command.state !== 'accepted'
        )
          value.messages.push({
            ...JSON.parse(String(command.payload)),
            id: command.request_id,
            state: command.state === 'rejected' ? 'failed' : 'queued',
            hold_reason: command.state === 'pending' ? 'waiting_for_executor' : null,
            error: command.error,
            reply: null,
            report_version: null,
            created_at: command.received_at,
            updated_at: command.received_at,
          });
      }
      return json(value);
    }
    if (request.method === 'POST' && !memoryPath[2]) {
      const payload = memoryCommandSchema.parse(await body(request));
      const wire = JSON.stringify(payload);
      const prior = await first(
        env,
        'SELECT * FROM project_memory_commands WHERE request_id=?',
        payload.request_id,
      );
      if (prior && (prior.payload !== wire || prior.project_id !== pid))
        throw new HttpError(409, 'Request identity is immutable');
      if (!prior) {
        const count = await first(
          env,
          "SELECT COUNT(*) AS n FROM project_memory_commands WHERE project_id=? AND state='pending'",
          pid,
        );
        if (Number(count?.n) >= 10)
          throw new HttpError(409, 'Wait for the executor to accept queued project commands');
        await env.DB.prepare(
          'INSERT INTO project_memory_commands(request_id,project_id,executor_id,payload,received_at) VALUES(?,?,?,?,?)',
        )
          .bind(payload.request_id, pid, env.EXECUTOR_ID, wire, Date.now() / 1000)
          .run();
      }
      return json(
        {
          schema_version: 1,
          request_id: payload.request_id,
          state: prior?.state || 'waiting_for_executor',
        },
        202,
      );
    }
  }
  if (p === '/api/executor/project-memory-commands' && request.method === 'GET') {
    const rows = await all(
      env,
      "SELECT request_id,project_id,payload FROM project_memory_commands WHERE executor_id=? AND state='pending' ORDER BY received_at LIMIT 20",
      env.EXECUTOR_ID,
    );
    return json({
      schema_version: 1,
      commands: rows.map((row) => ({ ...row, payload: JSON.parse(String(row.payload)) })),
    });
  }
  if (p === '/api/executor/project-memory-commands' && request.method === 'POST') {
    const batch = z
      .object({
        schema_version: z.literal(1),
        results: z
          .array(
            z
              .object({
                request_id: uuid,
                state: z.enum(['accepted', 'rejected']),
                error: z.string().max(1000).nullable(),
              })
              .strict(),
          )
          .max(20),
      })
      .strict()
      .parse(await body(request));
    for (const result of batch.results) {
      const row = await first(
        env,
        'SELECT * FROM project_memory_commands WHERE request_id=? AND executor_id=?',
        result.request_id,
        env.EXECUTOR_ID,
      );
      if (
        !row ||
        (row.state !== 'pending' && (row.state !== result.state || row.error !== result.error))
      )
        throw new HttpError(409, 'Project command outcome is immutable');
      await env.DB.prepare(
        "UPDATE project_memory_commands SET state=?,error=? WHERE request_id=? AND state='pending'",
      )
        .bind(result.state, result.error, result.request_id)
        .run();
    }
    return json({ schema_version: 1, accepted: batch.results.map((r) => r.request_id) });
  }
  if (p === '/api/executor/project-memory' && request.method === 'POST') {
    const value = memorySchema.parse(await body(request, 2 * 1024 * 1024));
    const pid = value.project_id;
    if (!(await projectCatalog(env)).some((project) => project.id === pid))
      throw new HttpError(409, 'Unknown project');
    if (value.report && value.report.project_id !== pid)
      throw new HttpError(409, 'Report project mismatch');
    // Never accept foreign project evidence, even through an executor projection.
    for (const summary of value.summaries) {
      const experiment = await first(env, 'SELECT payload FROM projections WHERE id=?', summary.id);
      if (!experiment || experimentProject(JSON.parse(String(experiment.payload))) !== pid)
        throw new HttpError(409, 'Project source is not published in this project');
    }
    const wire = JSON.stringify(value);
    const current = await first(
      env,
      'SELECT revision,payload FROM project_memory WHERE project_id=?',
      pid,
    );
    if (current && Number(current.revision) === value.revision && current.payload !== wire)
      throw new HttpError(409, 'Project revision is immutable');
    const statements = [];
    if (value.report) {
      const identity = `${pid}:${value.report.version}`;
      const reportWire = JSON.stringify(value.report);
      const prior = await first(
        env,
        'SELECT payload FROM project_memory_reports WHERE identity=?',
        identity,
      );
      if (prior && prior.payload !== reportWire)
        throw new HttpError(409, 'Working report version is immutable');
      statements.push(
        env.DB.prepare(
          'INSERT OR IGNORE INTO project_memory_reports(identity,project_id,version,payload) VALUES(?,?,?,?)',
        ).bind(identity, pid, value.report.version, reportWire),
      );
    }
    statements.push(
      env.DB.prepare(
        'INSERT INTO project_memory(project_id,executor_id,revision,payload) VALUES(?,?,?,?) ON CONFLICT(project_id) DO UPDATE SET revision=excluded.revision,payload=excluded.payload WHERE excluded.revision>project_memory.revision',
      ).bind(pid, env.EXECUTOR_ID, value.revision, wire),
    );
    await env.DB.batch(statements);
    return json({ schema_version: 1, project_id: pid, revision: value.revision });
  }

  if (p === '/api/push/config' && request.method === 'GET')
    return json({
      available: pushConfigured(env),
      public_key: pushConfigured(env) ? env.VAPID_PUBLIC_KEY : null,
    });
  if (p === '/api/push/status' && request.method === 'POST') {
    const payload = z
      .object({ endpoint: pushEndpointSchema })
      .strict()
      .parse(await body(request, 4096));
    const id = await pushHash(payload.endpoint);
    const follows = await all(
      env,
      `SELECT f.experiment_id FROM push_follows f
      JOIN push_subscriptions s ON s.id=f.subscription_id JOIN projections p ON p.id=f.experiment_id
      WHERE s.id=? AND s.owner=? AND p.deleted_at IS NULL`,
      id,
      pushOwner(request, env),
    );
    return json({ experiments: follows.map((f) => f.experiment_id) });
  }
  if (p === '/api/push/forget' && request.method === 'POST') {
    const payload = z
      .object({ endpoint: pushEndpointSchema })
      .strict()
      .parse(await body(request, 4096));
    await env.DB.prepare('DELETE FROM push_subscriptions WHERE id=? AND owner=?')
      .bind(await pushHash(payload.endpoint), pushOwner(request, env))
      .run();
    return json({ removed: true });
  }
  const follow = p.match(/^\/api\/experiments\/([^/]+)\/push$/);
  if (follow && request.method === 'POST') {
    if (!pushConfigured(env)) throw new HttpError(503, 'Notifications are not configured');
    const eid = uuid.parse(follow[1]);
    const projection = await requiredProjection(env, eid);
    const payload = pushFollowSchema.parse(await body(request, 8192));
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
      throw new HttpError(422, 'Invalid subscription public key');
    }
    const owner = pushOwner(request, env);
    const id = await pushHash(payload.subscription.endpoint);
    const hash = await pushHash(JSON.stringify([eid, payload]));
    const receipt = await first(
      env,
      'SELECT owner,payload_hash FROM push_receipts WHERE request_id=?',
      payload.request_id,
    );
    if (receipt) {
      if (receipt.owner !== owner || receipt.payload_hash !== hash)
        throw new HttpError(409, 'Notification request identity is immutable');
      return json({ following: payload.following });
    }
    const prior = await first(env, 'SELECT owner FROM push_subscriptions WHERE id=?', id);
    if (prior && prior.owner !== owner)
      throw new HttpError(409, 'This browser subscription belongs to another signed-in user');
    if (
      !prior &&
      Number(
        (await first(env, 'SELECT count(*) AS n FROM push_subscriptions WHERE owner=?', owner))?.n,
      ) >= 32
    )
      throw new HttpError(409, 'Too many notification devices; remove an old subscription first');
    const now = Date.now() / 1000;
    const guard =
      'EXISTS(SELECT 1 FROM push_receipts WHERE request_id=? AND owner=? AND payload_hash=?)';
    const saved = JSON.parse(String(projection.payload));
    const initial = experimentNotification(saved).data;
    initial.alert = true;
    const statements = [
      env.DB.prepare(
        'INSERT OR IGNORE INTO push_receipts(request_id,owner,payload_hash) VALUES(?,?,?)',
      ).bind(payload.request_id, owner, hash),
    ];
    if (payload.following) {
      statements.push(
        env.DB.prepare(
          `INSERT INTO push_subscriptions(id,owner,subscription,updated_at)
        SELECT ?,?,?,? WHERE ${guard} ON CONFLICT(id) DO UPDATE SET subscription=excluded.subscription,updated_at=excluded.updated_at WHERE push_subscriptions.owner=excluded.owner`,
        ).bind(
          id,
          owner,
          JSON.stringify(payload.subscription),
          now,
          payload.request_id,
          owner,
          hash,
        ),
      );
      statements.push(
        env.DB.prepare(
          `INSERT OR IGNORE INTO push_outbox(id,subscription_id,experiment_id,revision,payload,next_attempt_at,created_at)
        SELECT ?,?,?,?,?,?,? WHERE ${guard} AND EXISTS(SELECT 1 FROM push_subscriptions WHERE id=? AND owner=?)
        AND NOT EXISTS(SELECT 1 FROM push_follows WHERE subscription_id=? AND experiment_id=?)`,
        ).bind(
          `follow:${payload.request_id}`,
          id,
          eid,
          saved.revision,
          JSON.stringify(initial),
          now,
          now,
          payload.request_id,
          owner,
          hash,
          id,
          owner,
          id,
          eid,
        ),
      );
      statements.push(
        env.DB.prepare(
          `INSERT OR IGNORE INTO push_follows(subscription_id,experiment_id,created_at)
        SELECT ?,?,? WHERE ${guard} AND EXISTS(SELECT 1 FROM push_subscriptions WHERE id=? AND owner=?)`,
        ).bind(id, eid, now, payload.request_id, owner, hash, id, owner),
      );
    } else {
      statements.push(
        env.DB.prepare(
          `DELETE FROM push_follows WHERE subscription_id=? AND experiment_id=? AND ${guard}`,
        ).bind(id, eid, payload.request_id, owner, hash),
      );
      statements.push(
        env.DB.prepare(
          `DELETE FROM push_outbox WHERE subscription_id=? AND experiment_id=? AND ${guard}`,
        ).bind(id, eid, payload.request_id, owner, hash),
      );
      statements.push(
        env.DB.prepare(
          `DELETE FROM push_subscriptions WHERE id=? AND owner=? AND ${guard} AND NOT EXISTS(SELECT 1 FROM push_follows WHERE subscription_id=?)`,
        ).bind(id, owner, payload.request_id, owner, hash, id),
      );
    }
    await env.DB.batch(statements);
    const committed = await first(
      env,
      'SELECT owner,payload_hash FROM push_receipts WHERE request_id=?',
      payload.request_id,
    );
    if (committed?.owner !== owner || committed?.payload_hash !== hash)
      throw new HttpError(409, 'Notification request identity is immutable');
    if (
      payload.following &&
      (await first(env, 'SELECT owner FROM push_subscriptions WHERE id=?', id))?.owner !== owner
    )
      throw new HttpError(409, 'This browser subscription belongs to another signed-in user');
    return json({ following: payload.following });
  }
  if (p === '/api/session' && request.method === 'POST')
    return json(
      { authenticated: true, schema_version: 1 },
      200,
      env.AUTH_MODE === 'token'
        ? {
            'Set-Cookie': `et_session=${env.USER_TOKEN}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=43200`,
          }
        : {},
    );
  if (p === '/api/session' && request.method === 'DELETE')
    return json({ authenticated: false }, 200, {
      'Set-Cookie': 'et_session=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0',
    });
  if (p === '/api/projects' && request.method === 'POST') {
    const payload = createProjectSchema.parse(await body(request));
    const wire = JSON.stringify(payload);
    let row = await first(
      env,
      'SELECT * FROM project_requests WHERE request_id=?',
      payload.request_id,
    );
    if (row && row.payload !== wire)
      throw new HttpError(409, 'request_id was already used for another project');
    if (!row) {
      if (
        (await first(env, 'SELECT 1 FROM commands WHERE request_id=?', payload.request_id)) ||
        (await first(
          env,
          'SELECT 1 FROM settings_commands WHERE request_id=?',
          payload.request_id,
        )) ||
        (await first(
          env,
          'SELECT 1 FROM project_settings_requests WHERE request_id=?',
          payload.request_id,
        ))
      )
        throw new HttpError(409, 'request_id was already used for another command');
      const pending = await first(
        env,
        "SELECT COUNT(*) AS count FROM project_requests WHERE executor_id=? AND state='pending'",
        env.EXECUTOR_ID,
      );
      if ((await projectCatalog(env)).length + Number(pending?.count || 0) >= 50)
        throw new HttpError(409, 'At most 50 projects are supported');
      await env.DB.prepare(
        'INSERT INTO project_requests(request_id,project_id,executor_id,payload,received_at) VALUES(?,?,?,?,?)',
      )
        .bind(
          payload.request_id,
          'project-' + payload.request_id.replaceAll('-', ''),
          env.EXECUTOR_ID,
          wire,
          Date.now() / 1000,
        )
        .run();
      row = await first(
        env,
        'SELECT * FROM project_requests WHERE request_id=?',
        payload.request_id,
      );
    }
    return json(projectReceipt(row!), 202);
  }
  const projectSettings = p.match(/^\/api\/projects\/([^/]+)\/settings$/);
  if (projectSettings && request.method === 'PATCH') {
    const project_id = projectId.parse(projectSettings[1]);
    const payload = projectSettingsSchema.parse(await body(request));
    const wire = JSON.stringify(payload);
    let row = await first(
      env,
      'SELECT * FROM project_settings_requests WHERE request_id=?',
      payload.request_id,
    );
    if (row) {
      if (row.project_id !== project_id || row.payload !== wire)
        throw new HttpError(409, 'request_id was already used for another project setting');
      return json(projectSettingsReceipt(row), 202);
    }
    if (
      (await first(env, 'SELECT 1 FROM commands WHERE request_id=?', payload.request_id)) ||
      (await first(
        env,
        'SELECT 1 FROM settings_commands WHERE request_id=?',
        payload.request_id,
      )) ||
      (await first(env, 'SELECT 1 FROM project_requests WHERE request_id=?', payload.request_id))
    )
      throw new HttpError(409, 'request_id was already used for another command');
    if (!(await projectCatalog(env)).some((project) => project.id === project_id))
      throw new HttpError(404, 'Unknown project');
    try {
      await env.DB.prepare(
        'INSERT INTO project_settings_requests(request_id,project_id,executor_id,payload,received_at) VALUES(?,?,?,?,?)',
      )
        .bind(payload.request_id, project_id, env.EXECUTOR_ID, wire, Date.now() / 1000)
        .run();
    } catch (error) {
      row = await first(
        env,
        'SELECT * FROM project_settings_requests WHERE request_id=?',
        payload.request_id,
      );
      if (!row || row.project_id !== project_id || row.payload !== wire) throw error;
      return json(projectSettingsReceipt(row), 202);
    }
    return json(
      projectSettingsReceipt({ request_id: payload.request_id, project_id, state: 'pending' }),
      202,
    );
  }
  const projectSettingsRequest = p.match(/^\/api\/projects\/settings\/requests\/([^/]+)$/);
  if (projectSettingsRequest && request.method === 'GET') {
    const row = await first(
      env,
      'SELECT * FROM project_settings_requests WHERE request_id=?',
      uuid.parse(projectSettingsRequest[1]),
    );
    if (!row) throw new HttpError(404, 'Unknown project settings request');
    return json(projectSettingsReceipt(row));
  }
  const projectRequest = p.match(/^\/api\/projects\/requests\/([^/]+)$/);
  if (projectRequest && request.method === 'GET') {
    const row = await first(
      env,
      'SELECT * FROM project_requests WHERE request_id=?',
      uuid.parse(projectRequest[1]),
    );
    if (!row) throw new HttpError(404, 'Unknown project request');
    return json(projectReceipt(row));
  }
  if (p === '/api/executor/project-commands' && request.method === 'GET') {
    const rows = await all(
      env,
      "SELECT request_id,project_id,payload FROM project_requests WHERE executor_id=? AND state='pending' ORDER BY received_at LIMIT 20",
      env.EXECUTOR_ID,
    );
    return json({
      schema_version: 1,
      commands: rows.map((row) => ({ ...row, payload: JSON.parse(String(row.payload)) })),
    });
  }
  if (p === '/api/executor/project-settings-commands' && request.method === 'GET') {
    const rows = await all(
      env,
      "SELECT request_id,project_id,payload FROM project_settings_requests WHERE executor_id=? AND state='pending' ORDER BY received_at LIMIT 20",
      env.EXECUTOR_ID,
    );
    return json({
      schema_version: 1,
      commands: rows.map((row) => ({ ...row, payload: JSON.parse(String(row.payload)) })),
    });
  }
  if (p === '/api/executor/project-settings-results' && request.method === 'POST') {
    const value = z
      .object({
        schema_version: z.literal(1),
        results: z
          .array(
            z
              .object({
                request_id: uuid,
                project_id: projectId,
                state: z.enum(['applied', 'rejected']),
                error: z.string().max(1000).nullable(),
                settings: projectSettingsResultSchema.nullable(),
              })
              .strict(),
          )
          .max(20),
      })
      .strict()
      .parse(await body(request));
    const accepted: string[] = [];
    for (const result of value.results) {
      const row = await first(
        env,
        'SELECT * FROM project_settings_requests WHERE request_id=? AND executor_id=?',
        result.request_id,
        env.EXECUTOR_ID,
      );
      if (!row || row.project_id !== result.project_id)
        throw new HttpError(409, 'Project settings request identity mismatch');
      if ((result.state === 'applied') !== Boolean(result.settings))
        throw new HttpError(422, 'Applied project settings require a saved result');
      const wire = result.settings ? JSON.stringify(result.settings) : null;
      if (
        row.state !== 'pending' &&
        (row.state !== result.state || row.result !== wire || row.error !== result.error)
      )
        throw new HttpError(409, 'Project settings outcome is immutable');
      await env.DB.prepare(
        "UPDATE project_settings_requests SET state=?,error=?,result=? WHERE request_id=? AND state='pending'",
      )
        .bind(result.state, result.error, wire, result.request_id)
        .run();
      accepted.push(result.request_id);
    }
    return json({ schema_version: 1, accepted });
  }
  if (p === '/api/executor/project-results' && request.method === 'POST') {
    const payload = z
      .object({
        schema_version: z.literal(1),
        results: z
          .array(
            z
              .object({
                request_id: uuid,
                project_id: projectId,
                state: z.enum(['applied', 'rejected']),
                error: z.string().max(1000).nullable(),
                project: publicProject.nullable(),
              })
              .strict(),
          )
          .max(20),
      })
      .strict()
      .parse(await body(request));
    const accepted: string[] = [];
    for (const result of payload.results) {
      const row = await first(
        env,
        'SELECT * FROM project_requests WHERE request_id=? AND executor_id=?',
        result.request_id,
        env.EXECUTOR_ID,
      );
      if (
        !row ||
        row.project_id !== result.project_id ||
        (result.project && result.project.id !== result.project_id)
      )
        throw new HttpError(409, 'Project request identity mismatch');
      if ((result.state === 'applied') !== Boolean(result.project))
        throw new HttpError(422, 'Applied projects require a saved project');
      const wire = result.project ? JSON.stringify(result.project) : null;
      if (
        row.state !== 'pending' &&
        (row.state !== result.state || row.result !== wire || row.error !== result.error)
      )
        throw new HttpError(409, 'Project outcome is immutable');
      await env.DB.prepare(
        "UPDATE project_requests SET state=?,error=?,result=? WHERE request_id=? AND state='pending'",
      )
        .bind(result.state, result.error, wire, result.request_id)
        .run();
      accepted.push(result.request_id);
    }
    return json({ schema_version: 1, accepted });
  }
  if (p === '/api/experiments' && request.method === 'POST') {
    const payload = createSchema.parse(await body(request));
    if (await first(env, 'SELECT 1 FROM project_requests WHERE request_id=?', payload.request_id))
      throw new HttpError(409, 'request_id was already used for another command');
    const prior = await first(
      env,
      'SELECT request_id FROM commands WHERE request_id=?',
      payload.request_id,
    );
    if (!prior) {
      const project = (await projectCatalog(env)).find(
        (p) => p.id === (payload.project_id || 'test-project'),
      );
      if (!project?.available)
        throw new HttpError(409, 'Project is unknown or its checkout is unavailable');
    }
    if (payload.reference_experiment_id) {
      const reference = await requiredProjection(env, payload.reference_experiment_id);
      const view = JSON.parse(String(reference.payload));
      if (experimentProject(view) !== (payload.project_id || 'test-project'))
        throw new HttpError(409, 'Reference must belong to the selected project');
      if (view.phase !== 'complete' || !view.snapshot)
        throw new HttpError(409, 'Reference must be a completed experiment with a frozen protocol');
    }
    return json(await enqueue(env, 'create', payload), 202);
  }
  if (p === '/api/quota/reserve' && request.method === 'POST')
    return json(await enqueueReserve(env, reserveSchema.parse(await body(request))), 202);
  if (p === '/api/experiments' && request.method === 'GET')
    return json({
      schema_version: 1,
      experiments: await Promise.all(
        (
          await all(
            env,
            'SELECT * FROM projections WHERE deleted_at IS NULL ORDER BY updated_at DESC',
          )
        ).map((row) => view(env, row)),
      ),
    });
  const receipt = p.match(/^\/api\/commands\/([^/]+)$/);
  if (receipt && request.method === 'GET') {
    const id = uuid.parse(receipt[1]);
    const row =
      (await first(
        env,
        'SELECT request_id,experiment_id,kind,state,error,received_at FROM commands WHERE request_id=?',
        id,
      )) ||
      (await first(
        env,
        'SELECT request_id,kind,state,error,received_at FROM settings_commands WHERE request_id=?',
        id,
      ));
    if (!row) throw new HttpError(404, 'Unknown command');
    return json({
      ...row,
      schema_version: 1,
      state:
        row.state === 'acked'
          ? 'applied'
          : row.state === 'pending'
            ? 'waiting_for_executor'
            : row.state,
    });
  }
  const experiment = p.match(
    /^\/api\/experiments\/([^/]+)(?:\/(notes|actions|answers|delete|conversation|plan-review|steering))?$/,
  );
  if (experiment) {
    const id = uuid.parse(experiment[1]);
    if (!experiment[2] && request.method === 'GET')
      return json(await view(env, await requiredProjection(env, id)));
    if (experiment[2] === 'conversation' && request.method === 'GET') {
      await requiredProjection(env, id);
      const cursor = url.searchParams.get('before');
      const before = cursor === null ? Number.MAX_SAFE_INTEGER : Number(cursor);
      if (
        !Number.isSafeInteger(before) ||
        before < 1 ||
        (cursor !== null && !/^[0-9]+$/.test(cursor))
      )
        throw new HttpError(422, 'Invalid conversation cursor');
      const rows = await all(
        env,
        'SELECT payload,seq FROM conversation WHERE experiment_id=? AND seq<? ORDER BY seq DESC LIMIT 21',
        id,
        before,
      );
      return json({
        schema_version: 1,
        messages: rows
          .slice(0, 20)
          .reverse()
          .map((r) => JSON.parse(String(r.payload))),
        has_more: rows.length > 20,
      });
    }
    if (experiment[2] && request.method === 'POST') {
      const kind =
        experiment[2] === 'notes'
          ? 'note'
          : experiment[2] === 'plan-review'
            ? 'plan_review'
            : experiment[2] === 'steering'
              ? 'steer'
              : experiment[2] === 'conversation'
                ? 'discuss'
                : experiment[2] === 'actions'
                  ? 'action'
                  : experiment[2] === 'delete'
                    ? 'delete'
                    : 'answer';
      const payload = (
        kind === 'note'
          ? noteSchema
          : kind === 'plan_review'
            ? planReviewSchema
            : kind === 'steer'
              ? steeringSchema
              : kind === 'discuss'
                ? discussSchema
                : kind === 'action'
                  ? actionSchema
                  : kind === 'delete'
                    ? deleteSchema
                    : answerSchema
      ).parse(await body(request));
      if (kind === 'answer') {
        const row = await requiredProjection(env, id),
          pending = JSON.parse(String(row.payload)).pending_request;
        if (
          !pending ||
          pending.id !== (payload as z.infer<typeof answerSchema>).pending_request_id ||
          pending.expired
        )
          throw new HttpError(409, 'Stale or mismatched pending request');
        const answer = (payload as z.infer<typeof answerSchema>).answer;
        if (
          ['item/commandExecution/requestApproval', 'item/fileChange/requestApproval'].includes(
            pending.method,
          ) &&
          !['accept', 'decline', 'cancel'].includes(answer)
        )
          throw new HttpError(422, 'Decision must be accept, decline, or cancel');
      }
      return json(await enqueue(env, kind, payload, id), 202);
    }
  }
  if (p === '/api/inbox' && request.method === 'GET')
    return json({
      schema_version: 1,
      entries: await all(
        env,
        'SELECT i.* FROM inbox i JOIN projections p ON p.id=i.experiment_id WHERE p.deleted_at IS NULL ORDER BY i.created_at DESC',
      ),
    });
  if (p === '/api/status' && request.method === 'GET') {
    const row = await first(env, 'SELECT * FROM executor_status WHERE id=?', env.EXECUTOR_ID);
    const status = row
      ? JSON.parse(String(row.payload))
      : {
          last_tick: null,
          agent_ready: false,
          agent_error: null,
          quota: {
            admitted: false,
            reason: 'Executor not observed',
            remaining: {},
            observed_at: null,
          },
        };
    return json({
      ...status,
      projects: await projectCatalog(env),
      schema_version: 1,
      mode: 'gateway',
      auth_mode: env.AUTH_MODE,
      executor_id: env.EXECUTOR_ID,
      last_tick: status.last_tick ?? null,
      heartbeat_received_at: row ? Number(row.received_at) : null,
    });
  }
  const artifact = p.match(/^\/api\/artifacts\/([^/]+)$/);
  if (artifact && request.method === 'GET') {
    const row = await first(
      env,
      'SELECT * FROM artifacts WHERE id=? AND available=1',
      uuid.parse(artifact[1]),
    );
    if (!row) throw new HttpError(404, 'Artifact unavailable');
    await requiredProjection(env, String(row.experiment_id));
    const object = await env.BUCKET.get(String(row.object_key));
    if (!object) throw new HttpError(503, 'Published artifact storage unavailable');
    return new Response(object.body, {
      headers: {
        'Content-Type': String(row.media_type),
        'Content-Length': String(row.size),
        'Content-Disposition': `inline; filename="${row.name}"`,
        'Cache-Control': 'no-store',
        'Content-Security-Policy':
          "sandbox allow-same-origin; default-src 'none'; style-src 'unsafe-inline'; img-src 'self'; font-src 'self'; base-uri 'none'",
      },
    });
  }
  if (p === '/api/executor/commands' && request.method === 'GET') {
    const rows = await all(
      env,
      "SELECT * FROM commands WHERE executor_id=? AND state='pending' ORDER BY received_at LIMIT 20",
      env.EXECUTOR_ID,
    );
    const settings = await all(
      env,
      "SELECT * FROM settings_commands WHERE executor_id=? AND state='pending' ORDER BY received_at LIMIT 20",
      env.EXECUTOR_ID,
    );
    return json({
      schema_version: 1,
      commands: [...rows, ...settings]
        .sort((a, b) => Number(a.received_at) - Number(b.received_at))
        .slice(0, 20)
        .map((row) => ({
          ...row,
          experiment_id: row.experiment_id || null,
          payload: JSON.parse(String(row.payload)),
        })),
    });
  }
  if (p === '/api/executor/ack' && request.method === 'POST') {
    const value = z
      .object({ schema_version: z.literal(1), request_ids: z.array(uuid).max(20) })
      .strict()
      .parse(await body(request));
    if (value.request_ids.length)
      await env.DB.batch(
        value.request_ids.flatMap((id) => [
          env.DB.prepare(
            "UPDATE commands SET state='acked' WHERE request_id=? AND executor_id=? AND state!='rejected'",
          ).bind(id, env.EXECUTOR_ID),
          env.DB.prepare(
            "UPDATE settings_commands SET state='acked' WHERE request_id=? AND executor_id=? AND state!='rejected'",
          ).bind(id, env.EXECUTOR_ID),
        ]),
      );
    return json({ schema_version: 1, acknowledged: value.request_ids });
  }
  if (p === '/api/executor/reject' && request.method === 'POST') {
    const value = z
      .object({
        schema_version: z.literal(1),
        rejections: z
          .array(z.object({ request_id: uuid, reason: z.string().min(1).max(1000) }).strict())
          .max(20),
      })
      .strict()
      .parse(await body(request));
    const rejected: string[] = [];
    for (const refusal of value.rejections) {
      const row =
        (await first(
          env,
          'SELECT * FROM commands WHERE request_id=? AND executor_id=?',
          refusal.request_id,
          env.EXECUTOR_ID,
        )) ||
        (await first(
          env,
          'SELECT * FROM settings_commands WHERE request_id=? AND executor_id=?',
          refusal.request_id,
          env.EXECUTOR_ID,
        ));
      if (!row || row.state === 'acked') continue;
      if (!row.experiment_id) {
        await env.DB.prepare(
          "UPDATE settings_commands SET state='rejected',error=COALESCE(error,?) WHERE request_id=? AND state!='acked'",
        )
          .bind(refusal.reason, refusal.request_id)
          .run();
        rejected.push(refusal.request_id);
        continue;
      }
      await env.DB.batch([
        env.DB.prepare(
          "UPDATE commands SET state='rejected',error=COALESCE(error,?) WHERE request_id=? AND state!='acked'",
        ).bind(refusal.reason, refusal.request_id),
        env.DB.prepare(
          "INSERT OR IGNORE INTO inbox(id,experiment_id,kind,text,created_at) VALUES(?,?,'command_rejected',?,?)",
        ).bind(
          refusal.request_id,
          row.experiment_id,
          `Command could not be applied: ${String(row.error || refusal.reason)}`,
          Date.now() / 1000,
        ),
      ]);
      rejected.push(refusal.request_id);
    }
    return json({ schema_version: 1, rejected });
  }
  if (p === '/api/executor/publish' && request.method === 'POST')
    return json(await publish(env, publishSchema.parse(await body(request))));
  if (p === '/api/executor/events' && request.method === 'POST') {
    const value = z
      .object({
        schema_version: z.literal(1),
        executor_id: z.string(),
        events: z
          .array(
            z
              .object({
                event_id: uuid,
                experiment_id: uuid,
                revision: z.number().int().positive(),
              })
              .strict(),
          )
          .min(1)
          .max(500),
      })
      .strict()
      .parse(await body(request));
    if (value.executor_id !== env.EXECUTOR_ID)
      throw new HttpError(403, 'Executor identity mismatch');
    if (new Set(value.events.map((e) => e.event_id)).size !== value.events.length)
      throw new HttpError(422, 'Duplicate event identities');
    const wire = JSON.stringify(value.events);
    const projections = new Map(
      (
        await all(
          env,
          "SELECT id,revision FROM projections WHERE id IN (SELECT json_extract(value,'$.experiment_id') FROM json_each(?))",
          wire,
        )
      ).map((r) => [String(r.id), Number(r.revision)]),
    );
    const prior = new Map(
      (
        await all(
          env,
          "SELECT id,experiment_id,revision FROM events WHERE id IN (SELECT json_extract(value,'$.event_id') FROM json_each(?))",
          wire,
        )
      ).map((r) => [String(r.id), r]),
    );
    for (const event of value.events) {
      const revision = projections.get(event.experiment_id);
      if (revision === undefined || event.revision > revision)
        throw new HttpError(
          409,
          'Publish the covering projection before replaying event identities',
        );
      const saved = prior.get(event.event_id);
      if (
        saved &&
        (saved.experiment_id !== event.experiment_id || Number(saved.revision) !== event.revision)
      )
        throw new HttpError(409, 'Event identity is immutable');
    }
    // Historical identities cannot change a projection, evidence readiness or notifications.
    await env.DB.prepare(
      "INSERT OR IGNORE INTO events(id,experiment_id,revision,received_at) SELECT json_extract(value,'$.event_id'),json_extract(value,'$.experiment_id'),json_extract(value,'$.revision'),? FROM json_each(?)",
    )
      .bind(Date.now() / 1000, wire)
      .run();
    return json({ schema_version: 1, accepted: value.events.map((e) => e.event_id) });
  }
  const upload = p.match(/^\/api\/executor\/artifacts\/([^/]+)$/);
  if (upload && request.method === 'PUT') {
    const row = await first(env, 'SELECT * FROM artifacts WHERE id=?', uuid.parse(upload[1]));
    if (!row) throw new HttpError(404, 'Register artifact before upload');
    if (Number(request.headers.get('content-length') || 0) > 10 * 1024 * 1024)
      throw new HttpError(413, 'Artifact exceeds 10 MiB');
    const bytes = await boundedBytes(request, 10 * 1024 * 1024);
    if (bytes.byteLength !== Number(row.size) || (await digest(bytes)) !== row.sha256)
      throw new HttpError(409, 'Artifact size or hash mismatch');
    await env.BUCKET.put(String(row.object_key), bytes, {
      httpMetadata: { contentType: String(row.media_type) },
      customMetadata: { sha256: String(row.sha256) },
    });
    const verified = await env.BUCKET.head(String(row.object_key));
    if (!verified || verified.size !== Number(row.size))
      throw new HttpError(503, 'Artifact upload verification failed');
    await env.DB.prepare('UPDATE artifacts SET available=1 WHERE id=?').bind(row.id).run();
    return json({
      schema_version: 1,
      artifact_id: row.id,
      available: true,
      sha256: row.sha256,
      size: row.size,
    });
  }
  throw new HttpError(404, 'Unknown endpoint');
}
export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    try {
      const response = await route(request, env);
      if (
        response.ok &&
        pushConfigured(env) &&
        (new URL(request.url).pathname.startsWith('/api/executor/') ||
          /\/push$/.test(new URL(request.url).pathname))
      )
        ctx.waitUntil(
          drainPush(env).catch(() => {
            console.error('Notification delivery unavailable');
          }),
        );
      response.headers.set('X-Content-Type-Options', 'nosniff');
      response.headers.set('Referrer-Policy', 'no-referrer');
      return response;
    } catch (error) {
      if (error instanceof HttpError) return json({ detail: error.message }, error.status);
      if (error instanceof z.ZodError)
        return json(
          {
            detail: 'Invalid version-1 request',
            issues: error.issues.map((i) => ({ path: i.path, message: i.message })),
          },
          422,
        );
      console.error(
        'gateway storage or execution failure',
        error instanceof Error ? error.message : 'unknown',
      );
      return json(
        { detail: 'Gateway temporarily unavailable; retry with the same request ID' },
        503,
      );
    }
  },
};
