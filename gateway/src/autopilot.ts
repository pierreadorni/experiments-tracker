import { zipSync, strToU8 } from 'fflate';
import { z } from 'zod';
import type { Env } from './index';

const project = z.string().regex(/^[a-zA-Z0-9_-]{1,100}$/);
const command = z
  .object({
    schema_version: z.literal(1).default(1),
    request_id: z.string().uuid(),
    kind: z.enum([
      'configure',
      'activate',
      'revise',
      'pause',
      'resume',
      'stop',
      'complete',
      'message',
      'answer',
      'control',
    ]),
    expected_revision: z.number().int().nonnegative(),
    policy: z.record(z.string(), z.unknown()).optional(),
    text: z.string().max(16000).optional(),
    experiment_id: z.string().uuid().optional(),
    control: z.enum(['defer', 'resume', 'abandon', 'steer']).optional(),
    input_id: z.string().max(100).optional(),
  })
  .strict();
const logicalName = z.enum([
  'research_plan',
  'literature',
  'protocol',
  'claims',
  'backlog',
  'paper.tex',
  'references.bib',
  'weekly_report',
  'handoff',
  'guidelines',
]);
const reply = (value: object, status = 200) =>
  Response.json(
    { schema_version: 1, ...value },
    { status, headers: { 'Cache-Control': 'no-store' } },
  );
const docIdentity = (run: string, name: string, version: number) => `${run}:${name}:${version}`;

// The main router authenticates browser and machine roles and validates Origin first.
export async function autopilotRoute(
  request: Request,
  env: Env,
  path: string,
): Promise<Response | null> {
  const match = path.match(
    /^\/api\/projects\/([^/]+)\/autopilot(?:\/(commands|handoff)|\/documents\/([^/]+)\/versions\/(\d+))?$/,
  );
  const filePath = path.match(/^\/api\/projects\/([^/]+)\/autopilot\/files\/([^/]+)$/);
  if (!match && !filePath) return null;
  const read = async () => {
    const text = await request.text();
    if (new TextEncoder().encode(text).byteLength > 1024 * 1024)
      throw new Error('Autopilot payload too large');
    return JSON.parse(text);
  };
  const capability = async () => {
    const row = await env.DB.prepare('SELECT payload FROM executor_status WHERE id=?')
      .bind(env.EXECUTOR_ID)
      .first<{ payload: string }>();
    return row ? JSON.parse(row.payload).autopilot?.available === true : false;
  };
  if (filePath && request.method === 'GET') {
    const pid = project.parse(filePath[1]),
      id = z.string().uuid().parse(filePath[2]);
    const row = await env.DB.prepare(
      'SELECT object_key FROM autopilot_documents WHERE identity=? AND project_id=?',
    )
      .bind(id, pid)
      .first<{ object_key: string }>();
    const object = row && (await env.BUCKET.get(row.object_key));
    if (!object) return reply({ detail: 'Compiled paper is not published' }, 404);
    return new Response(object.body, {
      headers: {
        'Content-Type': 'application/pdf',
        'Content-Disposition': 'attachment; filename="paper.pdf"',
        'Cache-Control': 'private,max-age=3600',
      },
    });
  }
  if (match) {
    const pid = project.parse(match[1]);
    const saved = await env.DB.prepare(
      'SELECT * FROM autopilot_views WHERE project_id=? AND executor_id=?',
    )
      .bind(pid, env.EXECUTOR_ID)
      .first<{ payload: string; run_id: string; revision: number; observed_at: number }>();
    if (request.method === 'GET' && !match[2] && !match[3]) {
      const pending = await env.DB.prepare(
        'SELECT request_id,state,error,payload,received_at FROM autopilot_commands WHERE project_id=? AND executor_id=? ORDER BY received_at DESC LIMIT 30',
      )
        .bind(pid, env.EXECUTOR_ID)
        .all();
      return reply({
        ...(saved ? JSON.parse(saved.payload) : { run: null }),
        available: await capability(),
        pending_commands: pending.results,
      });
    }
    if (request.method === 'POST' && match[2] === 'commands') {
      if (!(await capability()))
        return reply(
          { detail: 'Install an Autopilot-capable executor before using this feature' },
          409,
        );
      const payload = command.parse(await read()),
        wire = JSON.stringify(payload);
      const prior = await env.DB.prepare('SELECT * FROM autopilot_commands WHERE request_id=?')
        .bind(payload.request_id)
        .first();
      if (prior) {
        if (
          prior.project_id !== pid ||
          prior.executor_id !== env.EXECUTOR_ID ||
          prior.payload !== wire
        )
          return reply({ detail: 'Command identity is immutable' }, 409);
        return reply(
          { request_id: payload.request_id, state: prior.state, error: prior.error },
          202,
        );
      }
      const count = await env.DB.prepare(
        "SELECT COUNT(*) AS n FROM autopilot_commands WHERE project_id=? AND executor_id=? AND state='pending'",
      )
        .bind(pid, env.EXECUTOR_ID)
        .first<{ n: number }>();
      if ((count?.n || 0) >= 20) return reply({ detail: 'Wait for queued project commands' }, 409);
      await env.DB.prepare("INSERT INTO autopilot_commands VALUES(?,?,?,?,'pending',NULL,?)")
        .bind(payload.request_id, pid, env.EXECUTOR_ID, wire, Date.now() / 1000)
        .run();
      return reply({ request_id: payload.request_id, state: 'received' }, 202);
    }
    if (request.method === 'GET' && match[3]) {
      if (!saved) return reply({ detail: 'No project run' }, 404);
      const run = z
        .string()
        .uuid()
        .parse(new URL(request.url).searchParams.get('run_id') || saved.run_id);
      const name = logicalName.parse(decodeURIComponent(match[3])),
        version = Number(match[4]);
      const row = await env.DB.prepare(
        'SELECT object_key FROM autopilot_documents WHERE identity=? AND project_id=?',
      )
        .bind(docIdentity(run, name, version), pid)
        .first<{ object_key: string }>();
      const object = row && (await env.BUCKET.get(row.object_key));
      if (!object) return reply({ detail: 'Document is not published' }, 404);
      return new Response(object.body, {
        headers: {
          'Content-Type': 'text/plain; charset=utf-8',
          'Content-Disposition': `attachment; filename="${name}"`,
          'Cache-Control': 'private,max-age=3600',
        },
      });
    }
    if (request.method === 'GET' && match[2] === 'handoff') {
      if (!saved) return reply({ detail: 'No project run' }, 404);
      const value = JSON.parse(saved.payload);
      const entries: Record<string, Uint8Array> = {};
      const handoff = value.run.handoff ? JSON.parse(value.run.handoff) : null;
      entries['handoff.json'] = strToU8(
        JSON.stringify(
          value.run.handoff ? JSON.parse(value.run.handoff) : { incomplete: true },
          null,
          2,
        ),
      );
      for (const doc of handoff?.documents || value.documents || []) {
        const row = await env.DB.prepare(
          'SELECT object_key FROM autopilot_documents WHERE identity=? AND project_id=?',
        )
          .bind(docIdentity(saved.run_id, doc.name, doc.version), pid)
          .first<{ object_key: string }>();
        const object = row && (await env.BUCKET.get(row.object_key));
        if (!object) return reply({ detail: 'Handoff document delivery is pending' }, 409);
        entries[logicalName.parse(doc.name)] = new Uint8Array(await object.arrayBuffer());
      }
      const paperFile = (handoff?.files || value.files)?.[0];
      if (paperFile) {
        const row = await env.DB.prepare(
          'SELECT object_key FROM autopilot_documents WHERE identity=? AND project_id=?',
        )
          .bind(paperFile.id, pid)
          .first<{ object_key: string }>();
        const object = row && (await env.BUCKET.get(row.object_key));
        if (object) entries['paper.pdf'] = new Uint8Array(await object.arrayBuffer());
      }
      entries['BUILD.txt'] = strToU8(
        'Compile paper.tex with pdflatex -no-shell-escape. Referenced figure assets remain linked published experiment artifacts. Missing results remain TODO; researcher review is required.',
      );
      return new Response(zipSync(entries) as unknown as BodyInit, {
        headers: {
          'Content-Type': 'application/zip',
          'Content-Disposition': 'attachment; filename="autopilot-handoff.zip"',
          'Cache-Control': 'no-store',
        },
      });
    }
    return reply({ detail: 'Method not allowed' }, 405);
  }
  return reply({ detail: 'Method not allowed' }, 405);
}
