import { z } from 'zod';
import type { Env } from './index';

const commit = z.string().regex(/^[0-9a-f]{40}$/);
const install = z
  .object({
    schema_version: z.literal(1).default(1),
    request_id: z.string().uuid(),
    release_id: commit,
  })
  .strict();
const operation = z
  .object({
    request_id: z.string().uuid(),
    release_id: commit,
    state: z.enum([
      'waiting',
      'backing_up',
      'verifying_backup',
      'restarting',
      'starting',
      'verifying',
      'complete',
      'failed',
      'blocked',
      'cancelled',
    ]),
    error: z.string().max(1000).nullable(),
    updated_at: z.number(),
  })
  .strict();
const status = z
  .object({
    current_commit: commit.nullable(),
    available: z.boolean(),
    release: z
      .object({
        commit_id: commit,
        summary: z.string().max(2000),
        state: z.enum(['preparing', 'testing', 'ready', 'failed']),
        error: z.string().max(1000).nullable(),
      })
      .strict()
      .nullable(),
    operation: operation.nullable(),
    blockers: z.array(z.string().max(200)).max(20),
    running_jobs: z.number().int().nonnegative(),
    observed_at: z.number().positive(),
  })
  .strict();
const response = (value: unknown, code = 200) =>
  Response.json(
    { schema_version: 1, ...(value as object) },
    { status: code, headers: { 'Cache-Control': 'no-store' } },
  );

// Called only after the main router's user/executor authentication and Origin check.
export async function maintenanceRoute(
  request: Request,
  env: Env,
  path: string,
): Promise<Response | null> {
  if (!['/api/maintenance', '/api/maintenance/install'].includes(path)) return null;
  const read = async () => {
    const text = await request.text();
    if (new TextEncoder().encode(text).byteLength > 16384)
      throw new Error('Maintenance request too large');
    return JSON.parse(text);
  };
  const latest = async () => {
    const row = await env.DB.prepare('SELECT payload FROM maintenance_status WHERE executor_id=?')
      .bind(env.EXECUTOR_ID)
      .first<{ payload: string }>();
    return row ? status.parse(JSON.parse(row.payload)) : null;
  };
  if (path === '/api/maintenance' && request.method === 'GET') {
    const pending = await env.DB.prepare(
      'SELECT request_id,release_id,state,error FROM maintenance_commands WHERE executor_id=? ORDER BY received_at DESC LIMIT 1',
    )
      .bind(env.EXECUTOR_ID)
      .first();
    return response({
      ...(await latest()),
      pending: pending?.state === 'accepted' ? null : pending,
    });
  }
  if (path === '/api/maintenance/install' && request.method === 'POST') {
    const value = install.parse(await read());
    const prior = await env.DB.prepare('SELECT * FROM maintenance_commands WHERE request_id=?')
      .bind(value.request_id)
      .first();
    if (prior) {
      if (prior.executor_id !== env.EXECUTOR_ID || prior.release_id !== value.release_id)
        return response({ detail: 'Maintenance request identity is immutable' }, 409);
      return response(
        { request_id: value.request_id, state: prior.state, error: prior.error },
        202,
      );
    }
    const saved = await latest();
    if (
      !saved?.available ||
      saved.release?.state !== 'ready' ||
      saved.release.commit_id !== value.release_id ||
      saved.current_commit === value.release_id
    )
      return response({ detail: 'This release is not available for installation' }, 409);
    if (saved.operation && !['complete', 'failed', 'cancelled'].includes(saved.operation.state))
      return response({ detail: 'An installation is already in progress' }, 409);
    // The unique pending index resolves simultaneous clicks transactionally.
    try {
      await env.DB.prepare(
        "INSERT INTO maintenance_commands(request_id,executor_id,release_id,state,received_at) VALUES(?,?,?,'pending',?)",
      )
        .bind(value.request_id, env.EXECUTOR_ID, value.release_id, Date.now() / 1000)
        .run();
    } catch {
      const repeated = await env.DB.prepare(
        'SELECT release_id FROM maintenance_commands WHERE request_id=?',
      )
        .bind(value.request_id)
        .first();
      if (repeated?.release_id !== value.release_id)
        return response({ detail: 'Another installation request is pending' }, 409);
    }
    return response({ request_id: value.request_id, state: 'pending', error: null }, 202);
  }
  return response({ detail: 'Method not allowed' }, 405);
}
