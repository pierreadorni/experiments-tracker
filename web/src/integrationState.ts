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

export function integrationMerged(exp: Experiment) {
  if (exp.branch_state) return exp.branch_state === 'merged';
  return integrationSettlement(exp) === 'merged';
}

export type IntegrationApproval = {
  integration_request_id: string;
  candidate_commit: string;
};

export function approvalIdentity(exp: Experiment): IntegrationApproval | null {
  const integration = exp.integration;
  if (
    integration?.stage !== 'awaiting_approval' ||
    !integration.request_id ||
    !exactCommit(integration.candidate_commit)
  )
    return null;
  return {
    integration_request_id: integration.request_id,
    candidate_commit: integration.candidate_commit,
  };
}

export function dontMerge(exp: Experiment) {
  return exp.contract.dont_merge === true;
}

function supported(exp: Experiment, status: Status | null) {
  if (!status?.branch_integration)
    return 'Install the executor update to enable branch integration';
  const contract = exp.contract.execution_contract;
  if (contract !== 4 && (contract !== 3 || status.execution_contract_3_archived))
    return 'Branch integration is unavailable for archived experiments';
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
  if (integrationMerged(exp)) return 'This branch is already integrated';
  if (dontMerge(exp)) return 'This experiment is marked Don’t merge';
  return null;
}

export function revertBlockedReason(exp: Experiment, status: Status | null) {
  const unavailable = supported(exp, status);
  if (unavailable) return unavailable;
  if (pendingRequest(exp, 'revert_integration'))
    return 'Revert request is waiting for the executor';
  if (integrationInFlight(exp)) return integrationStageLabel(exp.integration!.stage);
  if (!integrationMerged(exp)) return 'No applied integration to revert';
  return null;
}

export function approvalBlockedReason(exp: Experiment, status: Status | null) {
  const unavailable = supported(exp, status);
  if (unavailable) return unavailable;
  if (pendingRequest(exp, 'approve_integration')) return 'Approval is waiting for the executor';
  if (!approvalIdentity(exp)) return 'No integration is awaiting approval';
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
  if (pendingRequest(exp, 'approve_integration')) return 'Approval request queued';
  if (pendingRequest(exp, 'integrate')) return 'Integration request queued';
  if (integrationInFlight(exp)) return integrationStageLabel(exp.integration!.stage);
  const settled = integrationSettlement(exp);
  if (integrationMerged(exp)) return 'Integrated';
  if (settled === 'reverted') return 'Integration reverted';
  if (exp.integration) return integrationStageLabel(exp.integration.stage);
  if (dontMerge(exp)) return 'Branch active · Don’t merge';
  return 'Branch active';
}
