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
const document = z
  .object({
    project_id: project,
    run_id: z.string().uuid(),
    name: logicalName,
    version: z.number().int().positive(),
    content: z.string().min(1).max(200000),
    sources: z.array(z.record(z.string(), z.unknown())).max(100),
    author: z.string().max(200),
    created_at: z.number().finite(),
  })
  .strict();
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
  if (!match && !filePath && !path.startsWith('/api/executor/autopilot')) return null;
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
  const upload = path.match(/^\/api\/executor\/autopilot-files\/([^/]+)$/);
  if (upload && request.method === 'POST') {
    const id = z.string().uuid().parse(upload[1]),
      pid = project.parse(request.headers.get('X-Project-Id')),
      run = z.string().uuid().parse(request.headers.get('X-Run-Id'));
    const bytes = await request.arrayBuffer();
    if (
      bytes.byteLength > 10 * 1024 * 1024 ||
      new TextDecoder().decode(bytes.slice(0, 5)) !== '%PDF-'
    )
      return reply({ detail: 'Invalid or oversized PDF' }, 422);
    const key = 'autopilot-pdf/' + id,
      old = await env.BUCKET.get(key);
    if (old) {
      const previous = new Uint8Array(await old.arrayBuffer()),
        next = new Uint8Array(bytes);
      if (previous.length !== next.length || previous.some((byte, i) => byte !== next[i]))
        return reply({ detail: 'Published PDF is immutable' }, 409);
    } else await env.BUCKET.put(key, bytes, { httpMetadata: { contentType: 'application/pdf' } });
    await env.DB.prepare('INSERT OR IGNORE INTO autopilot_documents VALUES(?,?,?,?,?,?)')
      .bind(id, pid, run, 'paper.pdf', 1, key)
      .run();
    return reply({ accepted: id });
  }
  if (path === '/api/executor/autopilot-commands' && request.method === 'GET') {
    const rows = await env.DB.prepare(
      "SELECT request_id,project_id,payload FROM autopilot_commands WHERE executor_id=? AND state='pending' ORDER BY received_at LIMIT 20",
    )
      .bind(env.EXECUTOR_ID)
      .all();
    return reply({
      commands: rows.results.map((row) => ({ ...row, payload: JSON.parse(String(row.payload)) })),
    });
  }
  if (path === '/api/executor/autopilot-commands' && request.method === 'POST') {
    const result = z
      .object({
        request_id: z.string().uuid(),
        state: z.enum(['accepted', 'rejected']),
        error: z.string().max(1000).nullable(),
      })
      .strict()
      .parse(await read());
    const row = await env.DB.prepare(
      'SELECT state,error FROM autopilot_commands WHERE request_id=? AND executor_id=?',
    )
      .bind(result.request_id, env.EXECUTOR_ID)
      .first();
    if (
      !row ||
      (row.state !== 'pending' && (row.state !== result.state || row.error !== result.error))
    )
      return reply({ detail: 'Command receipt identity/outcome changed' }, 409);
    await env.DB.prepare(
      "UPDATE autopilot_commands SET state=?,error=? WHERE request_id=? AND state='pending'",
    )
      .bind(result.state, result.error, result.request_id)
      .run();
    return reply({ accepted: result.request_id });
  }
  if (path === '/api/executor/autopilot-documents' && request.method === 'POST') {
    const value = document.parse(await read()),
      id = docIdentity(value.run_id, value.name, value.version),
      key = 'autopilot/' + id;
    const old = await env.BUCKET.get(key);
    if (old && (await old.text()) !== value.content)
      return reply({ detail: 'Published document bytes are immutable' }, 409);
    if (!old)
      await env.BUCKET.put(key, value.content, {
        httpMetadata: { contentType: 'text/plain; charset=utf-8' },
      });
    await env.DB.prepare('INSERT OR IGNORE INTO autopilot_documents VALUES(?,?,?,?,?,?)')
      .bind(id, value.project_id, value.run_id, value.name, value.version, key)
      .run();
    const row = await env.DB.prepare('SELECT project_id FROM autopilot_documents WHERE identity=?')
      .bind(id)
      .first();
    if (row?.project_id !== value.project_id)
      return reply({ detail: 'Document belongs to another project' }, 409);
    return reply({ accepted: id });
  }
  if (path === '/api/executor/autopilot' && request.method === 'POST') {
    const value = z
      .object({
        project_id: project,
        run_id: z.string().uuid(),
        revision: z.number().int().nonnegative(),
        observed_at: z.number().finite(),
        view: z.record(z.string(), z.unknown()),
      })
      .strict()
      .parse(await read());
    const view = value.view as any;
    if (
      view.run?.id !== value.run_id ||
      view.run?.project_id !== value.project_id ||
      view.run?.revision !== value.revision
    )
      return reply({ detail: 'Project projection identity mismatch' }, 409);
    for (const doc of view.documents || []) {
      const row = await env.DB.prepare(
        'SELECT object_key FROM autopilot_documents WHERE identity=? AND project_id=?',
      )
        .bind(docIdentity(value.run_id, doc.name, doc.version), value.project_id)
        .first<{ object_key: string }>();
      if (!row || !(await env.BUCKET.head(row.object_key)))
        return reply({ detail: 'Publish documents before readiness' }, 409);
    }
    for (const file of view.files || []) {
      const row = await env.DB.prepare(
        'SELECT object_key FROM autopilot_documents WHERE identity=? AND project_id=?',
      )
        .bind(z.string().uuid().parse(file.id), value.project_id)
        .first<{ object_key: string }>();
      if (!row || !(await env.BUCKET.head(row.object_key)))
        return reply({ detail: 'Publish paper files before readiness' }, 409);
    }
    const old = await env.DB.prepare(
      'SELECT run_id,revision,observed_at,payload FROM autopilot_views WHERE project_id=? AND executor_id=?',
    )
      .bind(value.project_id, env.EXECUTOR_ID)
      .first<{ run_id: string; revision: number; observed_at: number; payload: string }>();
    if (old && old.run_id !== value.run_id && old.observed_at >= value.observed_at)
      return reply({ accepted: true });
    const statements: D1PreparedStatement[] = [
      env.DB.prepare(
        'INSERT INTO autopilot_views VALUES(?,?,?,?,?,?) ON CONFLICT(project_id) DO UPDATE SET executor_id=excluded.executor_id,run_id=excluded.run_id,revision=excluded.revision,payload=excluded.payload,observed_at=excluded.observed_at WHERE (excluded.run_id=autopilot_views.run_id AND excluded.revision>=autopilot_views.revision AND excluded.observed_at>autopilot_views.observed_at) OR (excluded.run_id!=autopilot_views.run_id AND excluded.observed_at>autopilot_views.observed_at)',
      ).bind(
        value.project_id,
        env.EXECUTOR_ID,
        value.run_id,
        value.revision,
        JSON.stringify(view),
        value.observed_at,
      ),
    ];
    // Durable attention-only notifications reuse devices already following a linked experiment.
    // Stable notice IDs prevent repeated pushes on unchanged budget observations.
    const priorNotices = new Set(
      old ? (JSON.parse(old.payload).notices || []).map((n: any) => n.id) : [],
    );
    if (
      view.policy?.notifications &&
      (!old ||
        (value.observed_at > old.observed_at &&
          (value.run_id !== old.run_id || value.revision >= old.revision)))
    ) {
      const experimentIds = (view.experiments || []).map((e: any) =>
        z.string().uuid().parse(e.experiment_id),
      );
      for (const notice of view.notices || []) {
        if (
          priorNotices.has(notice.id) ||
          notice.resolved_at ||
          !['researcher_input', 'handoff'].includes(notice.kind)
        )
          continue;
        const payload = JSON.stringify({
          title: 'Research Autopilot',
          body: String(notice.text).slice(0, 240),
          alert: true,
          url: `/?project=${encodeURIComponent(value.project_id)}&view=memory`,
          timestamp: Date.now(),
        });
        statements.push(
          env.DB.prepare(
            `INSERT OR IGNORE INTO push_outbox(id,subscription_id,experiment_id,revision,payload,next_attempt_at,created_at)
          SELECT ? || ':' || f.subscription_id,f.subscription_id,MIN(f.experiment_id),?,json_set(?,'$.experiment_id',MIN(f.experiment_id)),?,?
          FROM push_follows f JOIN projections p ON p.id=f.experiment_id
          WHERE f.experiment_id IN (SELECT value FROM json_each(?)) AND p.deleted_at IS NULL GROUP BY f.subscription_id`,
          ).bind(
            'autopilot:' + notice.id,
            value.revision,
            payload,
            Date.now() / 1000,
            Date.now() / 1000,
            JSON.stringify(experimentIds),
          ),
        );
      }
    }
    await env.DB.batch(statements);
    return reply({ accepted: true });
  }
  return reply({ detail: 'Method not allowed' }, 405);
}
