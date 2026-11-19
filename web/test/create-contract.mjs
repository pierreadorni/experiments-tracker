import assert from 'node:assert/strict';
import test from 'node:test';
import { contractFourCreateIssue, contractFourForProject } from '../src/createContract.mjs';

const status = {
  execution_contract_create: 4,
  research_loop_projects: ['loop-project'],
  mode: 'gateway',
  heartbeat_received_at: 1_000,
};

test('contract 4 requires an explicit project opt-in and advertised support', () => {
  assert.equal(contractFourForProject(status, 'loop-project', 1_050), true);
  assert.equal(contractFourForProject(status, 'other-project', 1_050), false);
  assert.equal(
    contractFourForProject({ ...status, execution_contract_create: 3 }, 'loop-project', 1_050),
    false,
  );
  assert.equal(
    contractFourForProject({ ...status, research_loop_projects: undefined }, 'loop-project', 1_050),
    false,
  );
});

test('gateway contract-4 support requires a non-future heartbeat no older than 90 seconds', () => {
  assert.equal(contractFourForProject(status, 'loop-project', 1_090), true);
  assert.equal(contractFourForProject(status, 'loop-project', 1_090.01), false);
  assert.equal(
    contractFourForProject({ ...status, heartbeat_received_at: 1_101 }, 'loop-project', 1_100),
    false,
  );
  assert.equal(
    contractFourForProject({ ...status, heartbeat_received_at: null }, 'loop-project', 1_050),
    false,
  );
});

test('contract-4 submit validation rechecks capability and refuses unsupported inputs', () => {
  assert.equal(contractFourCreateIssue(status, 'loop-project', 'auto', '', 1_050), null);
  assert.match(
    contractFourCreateIssue(status, 'loop-project', 'auto', 'reference', 1_050),
    /does not support reference/,
  );
  assert.match(
    contractFourCreateIssue(status, 'loop-project', 'shared', '', 1_050),
    /requires its own named branch workspace/,
  );
  assert.match(
    contractFourCreateIssue(status, 'loop-project', 'auto', '', 1_091),
    /not currently advertised/,
  );
});

test('local advertised support does not depend on Worker heartbeat fields', () => {
  assert.equal(
    contractFourForProject(
      { ...status, mode: 'live', heartbeat_received_at: null },
      'loop-project',
      10_000,
    ),
    true,
  );
});
