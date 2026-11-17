import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  dontMergeBlockedReason,
  exactCommit,
  integrateBlockedReason,
  integrationHeadline,
  integrationSettlement,
  integrationStageLabel,
  revertBlockedReason,
  shortCommit,
} from '../src/integrationState.ts';

const head = 'b'.repeat(40);
const event = (kind, seq) => ({ seq, kind, created_at: seq, payload: '{}' });
const pending = (kind) => [{ request_id: 'r', kind, action: null, received_at: 1 }];
const integration = (stage) => ({ stage, request_id: 'r', candidate_commit: null });
const experiment = (overrides = {}) => ({
  id: 'experiment',
  events: [],
  contract: { execution_contract: 3 },
  branch: 'exp/20260101-experiment',
  head_commit: head,
  ...overrides,
});
const status = { branch_integration: true };
const merged = { events: [event('integration_applied', 1)] };
const reverted = { events: [event('integration_applied', 1), event('integration_reverted', 2)] };

test('only exact projected commits render', () => {
  assert.equal(exactCommit(head), true);
  assert.equal(exactCommit('a'.repeat(64)), true);
  assert.equal(exactCommit(head.slice(0, 12)), false);
  assert.equal(shortCommit(head), 'bbbbbbb');
});

test('integrate needs a capable executor, a live branch and no pending or applied request', () => {
  assert.equal(integrateBlockedReason(experiment(), status), null);
  assert.equal(
    integrateBlockedReason(experiment(), null),
    'Install the executor update to enable branch integration',
  );
  assert.match(
    integrateBlockedReason(experiment({ contract: { execution_contract: 2 } }), status),
    /contract-3/,
  );
  assert.match(
    integrateBlockedReason(experiment({ head_commit: 'abc' }), status),
    /no saved Git branch/,
  );
  assert.match(integrateBlockedReason(experiment(merged), status), /already integrated/);
  assert.match(
    integrateBlockedReason(experiment({ integration: integration('claimed') }), status),
    /Preparing the merge/,
  );
  assert.match(
    integrateBlockedReason(experiment({ pending_commands: pending('integrate') }), status),
    /waiting for the executor/,
  );
});

test('revert needs the applied merge event and no request in flight', () => {
  assert.match(revertBlockedReason(experiment(), status), /No applied integration/);
  assert.equal(revertBlockedReason(experiment(merged), status), null);
  assert.match(revertBlockedReason(experiment(reverted), status), /No applied integration/);
  assert.match(
    revertBlockedReason(experiment({ ...merged, integration: integration('queued') }), status),
    /Queued for integration/,
  );
  assert.equal(
    revertBlockedReason(
      experiment({ ...merged, pending_commands: pending('revert_integration') }),
      status,
    ),
    'Revert request is waiting for the executor',
  );
  assert.equal(integrationSettlement(experiment(merged)), 'merged');
  assert.equal(integrationSettlement(experiment(reverted)), 'reverted');
  assert.equal(
    integrationSettlement(
      experiment({ events: [event('integration_reverted', 1), event('integration_applied', 2)] }),
    ),
    'merged',
  );
});

test('the Don’t merge flag is a capability-gated toggle', () => {
  assert.equal(dontMergeBlockedReason(experiment(), status), null);
  assert.match(
    dontMergeBlockedReason(experiment({ integration: integration('ready') }), status),
    /Wait for this integration/,
  );
  assert.match(
    dontMergeBlockedReason(experiment({ pending_commands: pending('dont_merge') }), status),
    /waiting for the executor/,
  );
});

test('the panel headline and stage labels report saved state', () => {
  assert.equal(integrationStageLabel('queued'), 'Queued for integration');
  assert.equal(integrationHeadline(experiment()), 'Branch active');
  assert.equal(
    integrationHeadline(experiment({ contract: { execution_contract: 3, dont_merge: true } })),
    'Branch active · Don’t merge',
  );
  assert.equal(
    integrationHeadline(experiment({ ...merged, pending_commands: pending('integrate') })),
    'Integration request queued',
  );
  assert.equal(
    integrationHeadline(experiment({ ...reverted, integration: integration('applied') })),
    'Integration reverted',
  );
  assert.equal(
    integrationHeadline(experiment({ ...reverted, integration: integration('claimed') })),
    'Preparing the merge',
  );
});
