import { test } from 'node:test';
import assert from 'node:assert/strict';
import { leadId, leadRunId, uuid5 } from '../src/lead.ts';

// Expected values come from Python uuid.uuid5(NAMESPACE_URL, ...), the backend derivation.
test('lead id matches the backend uuid5', () => {
  assert.equal(leadId('run-1'), '347d59c1-0d70-5093-8c4a-a4ede816c859');
  assert.equal(uuid5('autopilot-lead:' + 'x'.repeat(70)), '85c9ae25-8531-56e7-a097-b267f46df4f0');
});
test('only a marked contract leads a run', () => {
  assert.equal(leadRunId({ autopilot_lead: { run_id: 'r' } }), 'r');
  assert.equal(leadRunId({}), null);
  assert.equal(leadRunId({ autopilot_lead: 'r' }), null);
});
