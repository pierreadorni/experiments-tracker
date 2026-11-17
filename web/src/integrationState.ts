import type { Experiment, Status } from './types';

const IN_FLIGHT = new Set(
  'queued claimed conflict check_failed awaiting_approval ready'.split(' '),
);
const STAGE_LABELS: Record<string, string> = {
  queued: 'Queued for integration',
  claimed: 'Preparing the merge',
  conflict: 'Needs conflict resolution',
  check_failed: 'Project check failed',
  awaiting_approval: 'Waiting for owner approval',
  ready: 'Ready to apply',
  applied: 'Integration applied',
  abandoned: 'Integration abandoned',
};
const SETTLED = new Set(['integration_applied', 'integration_reverted']);

export function exactCommit(value?: string | null): value is string {
  return typeof value === 'string' && [40, 64].includes(value.length) && /^[0-9a-f]+$/.test(value);
}

export function shortCommit(value?: string | null) {
  return exactCommit(value) ? value.slice(0, 7) : '';
}

export function integrationStageLabel(stage: string) {
  return STAGE_LABELS[stage] || `Integration ${stage.replaceAll('_', ' ')}`;
}

export function integrationInFlight(exp: Experiment) {
  return Boolean(exp.integration && IN_FLIGHT.has(exp.integration.stage));
}

export function integrationSettlement(exp: Experiment): 'merged' | 'reverted' | null {
  const settled = exp.events.filter((event) => SETTLED.has(event.kind)).at(-1);
  if (!settled) return null;
  return settled.kind === 'integration_applied' ? 'merged' : 'reverted';
}

export function dontMerge(exp: Experiment) {
  return exp.contract.dont_merge === true;
}

function supported(exp: Experiment, status: Status | null) {
  if (!status?.branch_integration)
    return 'Install the executor update to enable branch integration';
  if (exp.contract.execution_contract !== 3)
    return 'Branch integration applies to contract-3 experiments';
  if (!exp.branch || !exactCommit(exp.head_commit))
    return 'This experiment has no saved Git branch';
  return null;
}

function pendingRequest(exp: Experiment, kind: string) {
  return exp.pending_commands?.some((command) => command.kind === kind) || false;
}

export function integrateBlockedReason(exp: Experiment, status: Status | null) {
  const unavailable = supported(exp, status);
  if (unavailable) return unavailable;
  if (pendingRequest(exp, 'integrate')) return 'Integration request is waiting for the executor';
  if (integrationInFlight(exp)) return integrationStageLabel(exp.integration!.stage);
  if (integrationSettlement(exp) === 'merged') return 'This branch is already integrated';
  if (dontMerge(exp)) return 'This experiment is marked Don’t merge';
  return null;
}

export function revertBlockedReason(exp: Experiment, status: Status | null) {
  const unavailable = supported(exp, status);
  if (unavailable) return unavailable;
  if (pendingRequest(exp, 'revert_integration'))
    return 'Revert request is waiting for the executor';
  if (integrationInFlight(exp)) return integrationStageLabel(exp.integration!.stage);
  if (integrationSettlement(exp) !== 'merged') return 'No applied integration to revert';
  return null;
}

export function dontMergeBlockedReason(exp: Experiment, status: Status | null) {
  const unavailable = supported(exp, status);
  if (unavailable) return unavailable;
  if (pendingRequest(exp, 'dont_merge')) return 'Don’t merge update is waiting for the executor';
  if (integrationInFlight(exp))
    return 'Wait for this integration to finish before changing the flag';
  return null;
}

export function integrationHeadline(exp: Experiment) {
  if (pendingRequest(exp, 'revert_integration')) return 'Revert request queued';
  if (pendingRequest(exp, 'integrate')) return 'Integration request queued';
  if (integrationInFlight(exp)) return integrationStageLabel(exp.integration!.stage);
  const settlement = integrationSettlement(exp);
  if (settlement) return settlement === 'merged' ? 'Integrated' : 'Integration reverted';
  if (exp.integration) return integrationStageLabel(exp.integration.stage);
  if (dontMerge(exp)) return 'Branch active · Don’t merge';
  return 'Branch active';
}
