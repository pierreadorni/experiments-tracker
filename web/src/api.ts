export class RequestError extends Error {
  constructor(
    message: string,
    public status: number,
  ) {
    super(message);
  }
}
export class Unauthorized extends Error {}
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
