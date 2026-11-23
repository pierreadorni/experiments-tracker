import { useSyncExternalStore } from 'react';
import { api, RequestError } from './api';
import type { Discussion, Experiment, Status } from './types';

// Reads come from projections, writes are commands, and every command has a receipt.
// Hosted refusals are asynchronous: the receipt settles after the executor applies or refuses.
export type Receipt = {
  request_id: string;
  kind: string;
  target: string | null;
  state: 'pending' | 'applied' | 'rejected';
  result: Record<string, any> | null; // eslint-disable-line @typescript-eslint/no-explicit-any
  error: string | null;
  received_at: number;
};
export type PendingCommand = Receipt & { payload?: Record<string, unknown> };
type Projection<T> = { key: string; revision: number; updated_at: number; document: T };
export type Host = 'local' | 'gateway';

export const experimentKey = (id: string) => `experiment:${id}`;
export const projectKey = (kind: 'knowledge' | 'autopilot', id: string) => `${kind}:${id}`;
export const reportKey = (project: string, version: number | string) =>
  `knowledge-report:${project}:${version}`;
const conversationKey = (id: string, seq: number) =>
  `conversation:${id}:${String(seq).padStart(10, '0')}`;

let host: Host | null = null;
export async function loadHost(): Promise<Host> {
  if (host) return host;
  host = (await api<{ host: Host }>('/api/session')).host === 'local' ? 'local' : 'gateway';
  return host;
}

const seconds = (value: number) => (value > 1e11 ? value / 1000 : value);
export async function getProjection<T>(key: string): Promise<Projection<T>> {
  return api<Projection<T>>(`/api/projections/${encodeURIComponent(key)}`);
}
export async function listProjections<T>(prefix: string): Promise<Projection<T>[]> {
  const found: Projection<T>[] = [];
  let before = '';
  for (let page = 0; page < 20; page++) {
    const result = await api<{ items?: Projection<T>[]; projections?: Projection<T>[] }>(
      `/api/projections?prefix=${encodeURIComponent(prefix)}&limit=100${before}`,
    );
    const items = result.items || result.projections || [];
    found.push(...items);
    // Only the hosted list pages (newest key first); the local list returns everything.
    if (!result.items || items.length < 100) break;
    before = `&before=${encodeURIComponent(items[items.length - 1].key)}`;
  }
  return found;
}
// One page of messages older than `before`; each message is its own document.
export async function olderMessages(id: string, before: number, size = 20) {
  const found = await api<{ items?: Projection<Discussion>[] }>(
    `/api/projections?prefix=${encodeURIComponent(`conversation:${id}:`)}&limit=${size + 1}&before=${encodeURIComponent(conversationKey(id, before))}`,
  );
  const items = found.items || [];
  return { messages: items.slice(0, size).map((i) => i.document), has_more: items.length > size };
}
export async function loadStatus(): Promise<Status> {
  const [where, status] = await Promise.all([loadHost(), getProjection<Status>('status')]);
  // The publication time stands in for the Worker's heartbeat stamp.
  return { ...status.document, host: where, heartbeat_received_at: seconds(status.updated_at) };
}

// The hosted pending list carries each saved payload; this page also remembers its own.
const sent = new Map<string, Record<string, unknown>>();
const ids = new Map<string, string>();
let pending: PendingCommand[] = [];
const listeners = new Set<() => void>();
const rejections = new Set<(receipt: Receipt) => void>();
const publish = (next: PendingCommand[]) => {
  pending = next;
  listeners.forEach((fn) => fn());
};
export const onRejection = (fn: (receipt: Receipt) => void) => {
  rejections.add(fn);
  return () => void rejections.delete(fn);
};
export async function refreshPending() {
  const { commands } = await api<{ commands: PendingCommand[] }>('/api/commands?state=pending');
  publish(commands.map((c) => ({ ...c, payload: c.payload ?? sent.get(c.request_id) })));
}
export function usePendingCommands() {
  return useSyncExternalStore(
    (fn) => (listeners.add(fn), () => void listeners.delete(fn)),
    () => pending,
  );
}
export function withPending(experiments: Experiment[], commands: PendingCommand[]): Experiment[] {
  return experiments.map((exp) => {
    const own = commands.filter((c) => c.target === experimentKey(exp.id));
    if (!own.length) return exp;
    return {
      ...exp,
      pending_commands: own.map((c) => ({
        request_id: c.request_id,
        kind: c.kind,
        action: typeof c.payload?.action === 'string' ? c.payload.action : null,
        received_at: c.received_at,
      })),
      pending_messages: own
        .filter((c) => c.kind === 'discuss' && typeof c.payload?.text === 'string')
        .map((c) => ({
          id: c.request_id,
          text: String(c.payload!.text),
          created_at: c.received_at,
        })),
    };
  });
}

const settle = (receipt: Receipt) => {
  publish(pending.filter((c) => c.request_id !== receipt.request_id));
  if (receipt.state === 'rejected') rejections.forEach((fn) => fn(receipt));
};
async function watch(id: string) {
  for (let attempt = 0; attempt < 300; attempt++) {
    await new Promise((resolve) => setTimeout(resolve, 2000));
    try {
      const receipt = await api<Receipt>(`/api/commands/${id}`);
      if (receipt.state !== 'pending') return settle(receipt);
    } catch (error) {
      // An unknown id has nothing to wait for.
      if (error instanceof RequestError && error.status === 404) break;
    }
  }
  publish(pending.filter((c) => c.request_id !== id));
}

// The request id is reused while the same command is retried, so a lost response is safe.
export async function send(
  kind: string,
  target: string | null,
  payload: Record<string, unknown> = {},
  chosen?: string,
): Promise<Receipt> {
  const identity = JSON.stringify([kind, target, payload]);
  const request_id = chosen || ids.get(identity) || crypto.randomUUID();
  ids.set(identity, request_id);
  const receipt = await api<Receipt>('/api/commands', {
    method: 'POST',
    body: JSON.stringify({
      request_id,
      kind,
      target,
      payload: { schema_version: 1, request_id, ...payload },
    }),
  });
  ids.delete(identity);
  if (receipt.state === 'rejected') throw new RequestError(receipt.error || 'Command refused', 409);
  if (receipt.state === 'pending') {
    sent.set(request_id, payload);
    publish([...pending.filter((c) => c.request_id !== request_id), { ...receipt, payload }]);
    void watch(request_id);
  }
  return receipt;
}
