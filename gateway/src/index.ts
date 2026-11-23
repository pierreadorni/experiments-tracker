import { autopilotRoute } from './autopilot';
import { relayRoute } from './relay';
import { z } from 'zod';
import { maintenanceRoute } from './maintenance';
import { assets } from './generated-assets';
import { pushConfigured, pushOwner, pushHash, pushEndpointSchema, drainPush } from './push';

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
    accounting_lag_seconds: z.number().int().min(60).max(86400).optional(),
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
    recipe_id: z.literal('agent_experiment_v1'),
    execution_contract: z.literal(4).optional(),
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
const actionSchema = z
  .object({
    ...base,
    action: z.enum([
      'pause_agent',
      'resume_agent',
      'pause_new_compute',
      'resume_new_compute',
      'summarize_title',
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
const integrateSchema = z
  .object({
    ...base,
    reason: z
      .string()
      .max(2000)
      .refine((value) => Boolean(value.trim()), 'Integration reason must contain text')
      .optional(),
  })
  .strict();
const revertIntegrationSchema = z.object(base).strict();
const approveIntegrationSchema = z
  .object({
    ...base,
    integration_request_id: z
      .string()
      .min(1)
      .max(200)
      .regex(/^[^:\s]+$/),
    candidate_commit: z.string().regex(/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/),
    action: z.literal('approve'),
  })
  .strict();
const dontMergeSchema = z.object({ ...base, value: z.boolean() }).strict();
const experimentCommandRoutes: Record<
  string,
  { kind: string; schema: z.ZodType<Record<string, unknown>> }
> = {
  notes: { kind: 'note', schema: noteSchema },
  steering: { kind: 'steer', schema: steeringSchema },
  conversation: { kind: 'discuss', schema: discussSchema },
  actions: { kind: 'action', schema: actionSchema },
  delete: { kind: 'delete', schema: deleteSchema },
  answers: { kind: 'answer', schema: answerSchema },
  integrate: { kind: 'integrate', schema: integrateSchema },
  'dont-merge': { kind: 'dont_merge', schema: dontMergeSchema },
  'integrations/revert': { kind: 'revert_integration', schema: revertIntegrationSchema },
  'integrations/approve': { kind: 'approve_integration', schema: approveIntegrationSchema },
};
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
  agent?: { model?: string; reasoning_effort?: string },
  project = { id: 'test-project', name: 'Test project' },
  executionContract = 3,
) {
  const now = Date.now() / 1000;
  return {
    schema_version: 1,
    id,
    brief,
    project_id: project.id,
    recipe_id: 'agent_experiment_v1',
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
      brief_driven: true,
      execution_contract: executionContract,
      ...(agent ? { agent } : {}),
    },
    snapshot: null,
    stages: ['plan', 'prepare', 'interpret', 'publish'].map((id) => ({
      id,
      state: 'pending',
      error: null,
      updated_at: now,
    })),
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
const durableContractFourAnswer = (exp: any, pending: any) => {
  if (
    exp.contract?.execution_contract !== 4 ||
    exp.deleted_at ||
    ['complete', 'failed', 'cancelled'].includes(exp.phase) ||
    !pending ||
    pending.rpc_id != null ||
    pending.method !==
      (pending.kind === 'approval'
        ? 'item/commandExecution/requestApproval'
        : 'item/tool/requestUserInput') ||
    !pending.action_id
  )
    return false;
  return (exp.agent_actions || []).some(
    (action: any) => action.id === pending.action_id && action.phase === 'research',
  );
};
async function executorSupports(env: Env, capability: string) {
  const row = await first(env, 'SELECT payload FROM executor_status WHERE id=?', env.EXECUTOR_ID);
  return Boolean(row && JSON.parse(String(row.payload))[capability]);
}
async function executorRecentlySupportsCreateContractFour(env: Env) {
  const row = await first(
    env,
    'SELECT payload,received_at FROM executor_status WHERE id=?',
    env.EXECUTOR_ID,
  );
  if (!row || typeof row.received_at !== 'number' || !Number.isFinite(row.received_at))
    return false;
  const age = Date.now() / 1000 - row.received_at;
  if (age < 0 || age > 90) return false;
  try {
    return JSON.parse(String(row.payload)).execution_contract_create === 4;
  } catch {
    return false;
  }
}
async function executorDefaultsToCreateContractFour(env: Env) {
  // Going offline does not revert the last advertised create policy.
  const row = await first(env, 'SELECT payload FROM executor_status WHERE id=?', env.EXECUTOR_ID);
  try {
    return Boolean(row && JSON.parse(String(row.payload)).execution_contract_create_default === 4);
  } catch {
    return false;
  }
}
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
  if (
    kind === 'create' &&
    payload.execution_contract === undefined &&
    (await executorDefaultsToCreateContractFour(env))
  )
    throw new HttpError(409, 'This executor requires an explicit execution contract 4 create');
  if (kind === 'create' && payload.execution_contract === 4) {
    if (!(await executorRecentlySupportsCreateContractFour(env)))
      throw new HttpError(
        409,
        'Install or refresh the executor update to create execution contract 4 experiments',
      );
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
            {
              model: payload.model as string | undefined,
              reasoning_effort: payload.reasoning_effort as string | undefined,
            },
            (await projectCatalog(env)).find(
              (p) => p.id === (payload.project_id || 'test-project'),
            ),
            payload.execution_contract === 4 ? 4 : 3,
          ),
        ),
        now,
      ),
    );
  else {
    const row = await requiredProjection(env, id);
    const exp = JSON.parse(String(row.payload));
    const contract = exp.contract?.execution_contract;
    const research = contract === 4;
    const archivedContractThree =
      contract === 3 && (await executorSupports(env, 'execution_contract_3_archived'));
    const stageExecutionAvailable = contract === 3 && !archivedContractThree;
    const archiveActive =
      !stageExecutionAvailable &&
      !exp.deleted_at &&
      !['complete', 'failed', 'cancelled'].includes(exp.phase);
    const researchAction =
      research &&
      ['pause_agent', 'resume_agent', 'pause_new_compute', 'resume_new_compute'].includes(
        String(payload.action),
      );
    const durableAnswer = kind === 'answer' && durableContractFourAnswer(exp, exp.pending_request);
    if (
      archiveActive &&
      ((kind === 'action' && payload.action !== 'cancel' && !researchAction) ||
        (['answer', 'steer'].includes(kind) && !durableAnswer && !(research && kind === 'steer')))
    )
      throw new HttpError(
        409,
        research
          ? 'This mutation is unavailable for research sessions'
          : 'Legacy execution is archived; only cancellation is available',
      );
    if (
      !stageExecutionAvailable &&
      ((kind === 'action' && payload.action === 'summarize_title') ||
        (kind === 'steer' && !research))
    )
      throw new HttpError(409, 'Legacy execution is archived; this mutation is unavailable');
    if (kind === 'action' && payload.action === 'cancel') {
      if (!(await executorSupports(env, 'experiment_cancellation')))
        throw new HttpError(409, 'Install the executor update to enable cancellation');
      if (exp.phase === 'complete')
        throw new HttpError(409, 'A completed experiment cannot be cancelled');
    }
    if (['integrate', 'revert_integration', 'dont_merge', 'approve_integration'].includes(kind)) {
      if (archivedContractThree)
        throw new HttpError(409, 'Legacy execution is archived; integration is unavailable');
      if (!(await executorSupports(env, 'branch_integration')))
        throw new HttpError(409, 'Install the executor update to enable branch integration');
    }
    if (
      kind === 'approve_integration' &&
      (exp.integration?.stage !== 'awaiting_approval' ||
        exp.integration?.request_id !== payload.integration_request_id ||
        exp.integration?.candidate_commit !== payload.candidate_commit)
    )
      throw new HttpError(
        409,
        'Integration approval has changed or is no longer awaiting approval',
      );
    if (kind === 'discuss' && exp.phase !== 'complete')
      throw new HttpError(409, 'Discussion is available when the experiment is complete');
    if (research && kind === 'steer') {
      if (payload.mode === 'restart')
        throw new HttpError(409, 'Restart steering is not supported for research sessions');
      if (new TextEncoder().encode(String(payload.text).trim()).length > 1000)
        throw new HttpError(409, 'Research steering must be at most 1000 UTF-8 bytes');
    }
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
  const relay = await relayRoute(request, env, p);
  if (relay) return relay;
  const autopilot = await autopilotRoute(request, env, p);
  if (autopilot) return autopilot;
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
  if (p === '/api/push/config' && request.method === 'GET')
    return json({
      available: pushConfigured(env),
      public_key: pushConfigured(env) ? env.VAPID_PUBLIC_KEY : null,
    });
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
    /^\/api\/experiments\/([^/]+)(?:\/(notes|actions|answers|delete|conversation|plan-review|steering|integrate|dont-merge|integrations\/(?:revert|approve)))?$/,
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
      const route = experimentCommandRoutes[experiment[2]];
      const payload = route.schema.parse(await body(request));
      const kind = route.kind;
      if (kind === 'answer') {
        const prior = await first(
          env,
          'SELECT request_id FROM commands WHERE request_id=?',
          payload.request_id,
        );
        if (prior) return json(await enqueue(env, kind, payload, id), 202);
        const row = await requiredProjection(env, id);
        const projected = JSON.parse(String(row.payload));
        const pending = projected.pending_request;
        if (
          !pending ||
          pending.id !== (payload as z.infer<typeof answerSchema>).pending_request_id ||
          pending.expired
        )
          throw new HttpError(409, 'Stale or mismatched pending request');
        const savedAction = pending.action_id
          ? (projected.agent_actions || []).find((action: any) => action.id === pending.action_id)
          : null;
        const stageExecutionAvailable =
          projected.contract?.execution_contract === 3 &&
          !(await executorSupports(env, 'execution_contract_3_archived'));
        if (
          !stageExecutionAvailable &&
          !durableContractFourAnswer(projected, pending) &&
          !(
            projected.phase === 'complete' &&
            savedAction?.phase?.startsWith('discuss:') &&
            ['dispatching', 'running'].includes(savedAction.state)
          )
        )
          throw new HttpError(
            409,
            'Legacy execution is archived; only a live discussion reply is available',
          );
        const answer = (payload as z.infer<typeof answerSchema>).answer;
        const approval =
          pending.kind === 'approval' ||
          ['item/commandExecution/requestApproval', 'item/fileChange/requestApproval'].includes(
            pending.method,
          );
        if (approval && !['accept', 'decline', 'cancel'].includes(answer))
          throw new HttpError(422, 'Decision must be accept, decline, or cancel');
        if (projected.contract?.execution_contract === 4 && pending.kind === 'approval') {
          const earlier = await first(
            env,
            "SELECT request_id FROM commands WHERE experiment_id=? AND kind='answer' AND state!='rejected' AND json_extract(payload,'$.pending_request_id')=?",
            id,
            pending.id,
          );
          if (pending.answer || earlier)
            throw new HttpError(409, 'Research approval already has an owner decision');
        }
      }
      return json(await enqueue(env, kind, payload, id), 202);
    }
  }
  if (p === '/api/inbox' && request.method === 'GET') {
    const projectNotices = await all(
      env,
      'SELECT project_id,payload FROM autopilot_views WHERE executor_id=?',
      env.EXECUTOR_ID,
    );
    return json({
      schema_version: 1,
      entries: await all(
        env,
        'SELECT i.* FROM inbox i JOIN projections p ON p.id=i.experiment_id WHERE p.deleted_at IS NULL ORDER BY i.created_at DESC',
      ).then((entries) =>
        [
          ...entries,
          ...projectNotices.flatMap((row) =>
            (JSON.parse(String(row.payload)).notices || [])
              .filter((n: any) => !n.resolved_at)
              .map((n: any) => ({ ...n, experiment_id: '', project_id: row.project_id })),
          ),
        ].sort((a: any, b: any) => b.created_at - a.created_at),
      ),
    });
  }
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
          /^\/api\/push\/follows\//.test(new URL(request.url).pathname))
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
