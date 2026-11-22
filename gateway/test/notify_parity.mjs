// Golden check for the Python port in src/experimenttracker/publish/notifications.py.
// Run `node test/notify_parity.mjs --write` from gateway/ to regenerate the expectations.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, writeFileSync } from 'node:fs';
import { experimentNotification } from '../src/push.ts';

const file = new URL('./fixtures/notification_inputs.json', import.meta.url);
const golden = new URL('./fixtures/notification_expected.json', import.meta.url);
const { base, cases } = JSON.parse(readFileSync(file, 'utf8'));
const compute = () =>
  Object.fromEntries(
    Object.entries(cases).map(([name, patch]) => {
      const { data, fingerprint } = experimentNotification({ ...base, ...patch });
      return [name, { fingerprint, title: data.title, body: data.body, url: data.url, alert: data.alert }];
    }),
  );
if (process.argv.includes('--write')) writeFileSync(golden, JSON.stringify(compute(), null, 1) + '\n');
test('TypeScript notifications equal the shared golden file', () => {
  assert.deepEqual(compute(), JSON.parse(readFileSync(golden, 'utf8')));
});
