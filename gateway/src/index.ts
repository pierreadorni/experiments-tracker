import { relayRoute } from './relay';
import { z } from 'zod';
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
