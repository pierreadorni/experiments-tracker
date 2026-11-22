import type { Experiment } from './types';

export const terminal = ['complete', 'failed', 'cancelled'];
export const pretty = (value: string) => value.replaceAll('_', ' ');
export function researchRequestQuestion(params: unknown) {
  if (!params || typeof params !== 'object') return null;
  const value = params as { question?: unknown; questions?: { question?: unknown }[] };
  if (typeof value.question === 'string') return value.question;
  const first = Array.isArray(value.questions) ? value.questions[0] : null;
  return first && typeof first.question === 'string' ? first.question : null;
}
export const experimentTitle = (exp: Experiment) => {
  if (exp.title || exp.analysis?.title) return exp.title || exp.analysis!.title;
  if (
    exp.contract.execution_contract !== 4 &&
    exp.contract.planning_version &&
    !exp.plan_review &&
    !terminal.includes(exp.phase)
  )
    return 'Planning experiment…';
  return exp.brief.length <= 100 ? exp.brief : `${exp.brief.slice(0, 97).replace(/\s+\S*$/, '')}…`;
};
export const date = (seconds: number) =>
  new Date(seconds * 1000).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
export function age(seconds: number | null) {
  if (!seconds) return 'Not observed';
  const n = Math.max(0, Math.floor(Date.now() / 1000 - seconds));
  return n < 60
    ? `${n}s ago`
    : n < 3600
      ? `${Math.floor(n / 60)}m ago`
      : `${Math.floor(n / 3600)}h ago`;
}
