// Synthetic UI check for the relay's maintenance-update flow, against the research-owner fixture
// (web/scripts/research-owner-fixture.py). The maintenance projection and the command receipts are
// mocked in the browser; no executor restart happens and no live state is touched.
import assert from 'node:assert/strict';
import { chromium } from '@playwright/test';
import { mkdir, readFile } from 'node:fs/promises';
import path from 'node:path';

const target = new URL(process.env.ET_UI_URL || 'http://127.0.0.1:8879');
const root = process.env.ET_C4_FIXTURE_ROOT;
const output = path.resolve(process.env.ET_SCREENSHOTS || '/tmp/et-maintenance-screenshots');
if (target.protocol !== 'http:' || !['127.0.0.1', 'localhost'].includes(target.hostname) || !root)
  throw new Error('A temporary loopback fixture (ET_C4_FIXTURE_ROOT) is required');
const token = (await readFile(path.join(root, 'api-token'), 'utf8')).trim();
await mkdir(output, { recursive: true });
const browser = await chromium.launch({
  headless: true,
  args: ['--no-sandbox'],
  ...(process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH
    ? { executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH }
    : {}),
});
const page = await browser.newPage({
  viewport: { width: 1365, height: 1000 },
  reducedMotion: 'reduce',
});
const errors = [];
page.on('pageerror', (error) => errors.push(error.message));

const release = (commit, summary) => ({
  commit_id: commit,
  summary,
  state: 'ready',
  error: null,
});
let state = {
  current_commit: 'a'.repeat(40),
  available: true,
  release: release('b'.repeat(40), 'Synthetic release one'),
  operation: null,
  running_jobs: 2,
  blockers: [],
};
// What the next POST /api/commands and receipt poll answer with.
let outcome = { state: 'applied', error: null };
let sentCommand;
const fresh = () => ({ ...state, observed_at: Date.now() / 1000 });
await page.route(/\/api\/projections\/maintenance$/, (route) =>
  route.fulfill({
    json: { key: 'maintenance', revision: 1, updated_at: Date.now() / 1000, document: fresh() },
  }),
);
await page.route(/\/api\/commands(\?.*)?$/, async (route) => {
  const request = route.request();
  if (request.method() !== 'POST') return route.fulfill({ json: { commands: [] } });
  sentCommand = request.postDataJSON();
  await route.fulfill({
    json: {
      request_id: sentCommand.request_id,
      kind: sentCommand.kind,
      target: sentCommand.target,
      state: 'pending',
      result: null,
      error: null,
      received_at: Date.now() / 1000,
    },
  });
});
await page.route(/\/api\/commands\/[0-9a-f-]{36}$/, (route) =>
  route.fulfill({
    json: {
      request_id: sentCommand.request_id,
      kind: 'maintenance',
      target: 'maintenance',
      state: outcome.state,
      result: null,
      error: outcome.error,
      received_at: Date.now() / 1000,
    },
  }),
);

try {
  await page.goto(target.href);
  await page.getByLabel('Access token').fill(token);
  await page.getByRole('button', { name: 'Open workspace' }).click();
  await page.getByRole('button', { name: 'Executor & quota', exact: true }).click();
  const panel = page.locator('.executor-update');
  const install = panel.getByRole('button', { name: 'Install update…' });

  // Applied: the ready release is shown, the exact release is sent, then the executor reports progress.
  await panel.getByText('Update available', { exact: true }).waitFor();
  await panel.getByText('Synthetic release one').waitFor();
  await panel.getByText('All release checks passed', { exact: false }).waitFor();
  await install.click();
  await page.getByRole('dialog', { name: 'Install tested update?' }).waitFor();
  await page.screenshot({ path: path.join(output, 'confirm.png') });
  await page.getByRole('button', { name: 'Authorize installation', exact: true }).click();
  await panel.getByText('Install request queued', { exact: true }).waitFor();
  assert.ok(await install.isDisabled(), 'Install stays disabled while the request is pending');
  assert.equal(sentCommand.kind, 'maintenance');
  assert.equal(sentCommand.target, 'maintenance');
  assert.equal(sentCommand.payload.release_id, 'b'.repeat(40));
  state = {
    ...state,
    operation: {
      request_id: sentCommand.request_id,
      release_id: 'b'.repeat(40),
      state: 'waiting',
      error: null,
      updated_at: Date.now() / 1000,
    },
  };
  await panel.getByText('Waiting for a safe restart', { exact: true }).waitFor({ timeout: 15000 });
  assert.ok(await install.isDisabled(), 'Install stays disabled while installing');
  await page.screenshot({ path: path.join(output, 'applied.png') });

  // Rejected: a later release is refused by the executor and the reason is shown.
  state = {
    ...state,
    operation: null,
    release: release('c'.repeat(40), 'Synthetic release two'),
  };
  outcome = { state: 'rejected', error: 'Synthetic refusal: executor is not accepting installs.' };
  await page.reload();
  await page.getByRole('button', { name: 'Executor & quota', exact: true }).click();
  await panel.getByText('Synthetic release two').waitFor();
  await install.click();
  await page.getByRole('button', { name: 'Authorize installation', exact: true }).click();
  await panel.getByText('Install request queued', { exact: true }).waitFor();
  assert.equal(sentCommand.payload.release_id, 'c'.repeat(40));
  await panel.getByText('Install request rejected', { exact: true }).waitFor({ timeout: 15000 });
  await panel.getByText(outcome.error).waitFor();
  await page.screenshot({ path: path.join(output, 'rejected.png') });

  assert.deepEqual(errors, []);
  console.log(
    'Maintenance UI passed ready release, exact-release command, pending, applied and rejected receipt checks. Install was mocked.',
  );
} finally {
  await browser.close();
}
