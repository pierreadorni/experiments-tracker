export class RequestError extends Error {
  constructor(
    message: string,
    public status: number,
  ) {
    super(message);
  }
}
export class Unauthorized extends Error {}
const pending = new Map<string, string>();
export async function api<T>(path: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(path, {
    signal: AbortSignal.timeout(15000),
    ...options,
    credentials: 'same-origin',
    headers: { 'Content-Type': 'application/json', ...options.headers },
  });
  if (response.status === 401) throw new Unauthorized('Sign in to continue');
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new RequestError(
      typeof body.detail === 'string' ? body.detail : `Request failed (${response.status})`,
      response.status,
    );
  }
  return response.json();
}
export async function command<T>(path: string, payload: Record<string, unknown>): Promise<T> {
  const key = path + JSON.stringify(payload);
  const request_id = pending.get(key) || crypto.randomUUID();
  pending.set(key, request_id);
  const result = await api<T>(path, {
    method: 'POST',
    body: JSON.stringify({ schema_version: 1, request_id, ...payload }),
  });
  pending.delete(key);
  return result;
}
