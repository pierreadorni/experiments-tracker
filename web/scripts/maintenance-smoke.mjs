// Synthetic UI acceptance. The install boundary is intercepted; no live restart.
import { chromium } from '@playwright/test';
import { readFile, mkdir } from 'node:fs/promises';
import path from 'node:path';

const target = new URL(process.env.ET_UI_URL || 'http://127.0.0.1:18769');
if (
  target.protocol !== 'http:' ||
  !['127.0.0.1', 'localhost', '[::1]'].includes(target.hostname) ||
  target.username ||
  target.password
)
  throw new Error('Maintenance smoke requires a loopback demo');
if (!process.env.ET_TOKEN_FILE)
  throw new Error('Set ET_TOKEN_FILE to the protected demo credential');
const output = path.resolve(process.env.ET_SCREENSHOTS || '/tmp/et-maintenance-screenshots');
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ headless: true, args: ['--no-sandbox'] });
const page = await browser.newPage({
  viewport: { width: 1440, height: 1050 },
  reducedMotion: 'reduce',
});
const errors = [];
page.on('pageerror', (error) => errors.push(error.message));
try {
  await page.goto(target.href);
  await page
    .getByLabel('Access token')
    .fill((await readFile(process.env.ET_TOKEN_FILE, 'utf8')).trim());
  await page.getByRole('button', { name: 'Open workspace' }).click();
  await page.getByRole('heading', { name: 'Experiments', exact: true }).waitFor();
  const status = await (await page.request.get(new URL('/api/status', target).href)).json();
  if (status.mode !== 'demo') throw new Error('Refusing synthetic experiment outside demo mode');
  const created = await page.request.post(new URL('/api/experiments', target).href, {
    headers: { Origin: target.origin },
    data: {
      schema_version: 1,
      request_id: crypto.randomUUID(),
      brief: 'Synthetic maintenance browser acceptance.',
      recipe_id: 'toy_compare_v1',
    },
  });
  if (!created.ok()) throw new Error('Could not create synthetic experiment');
  const initial = await (await page.request.get(new URL('/api/maintenance', target).href)).json();
  if (initial.release?.state !== 'ready')
    throw new Error('Demo fixture needs a ready synthetic release');
  let state = { ...initial, current_commit: 'b'.repeat(40) };
  let install;
  await page.route('**/api/maintenance', (route) =>
    route.fulfill({ json: { ...state, observed_at: state.observed_at ?? Date.now() / 1000 } }),
  );
  await page.route('**/api/maintenance/install', async (route) => {
    install = route.request().postDataJSON();
    if (Object.keys(install).sort().join(',') !== 'release_id,request_id,schema_version')
      throw new Error('Unexpected install payload');
    state = {
      ...state,
      operation: {
        request_id: install.request_id,
        release_id: install.release_id,
        state: 'waiting',
        error: null,
        updated_at: Date.now() / 1000,
      },
    };
    await route.fulfill({
      status: 202,
      json: { schema_version: 1, request_id: install.request_id, state: 'waiting', error: null },
    });
  });
  await page.getByRole('button', { name: 'Executor & quota', exact: true }).click();
  const panel = page.locator('.executor-update');
  await panel.getByText('Update available', { exact: true }).waitFor();
  await panel.getByRole('button', { name: 'Install update…' }).click();
  await page.getByRole('dialog', { name: 'Install tested update?' }).waitFor();
  await page.screenshot({ path: path.join(output, 'desktop-confirm.png') });
  await page.keyboard.press('Escape');
  if (await page.getByRole('dialog').count())
    throw new Error('Escape did not dismiss confirmation');
  await panel.getByRole('button', { name: 'Install update…' }).click();
  await page.getByRole('button', { name: 'Authorize installation', exact: true }).click();
  await panel.getByText('Waiting for a safe restart', { exact: true }).waitFor();
  if (!install || install.release_id !== initial.release.commit_id)
    throw new Error('Wrong release authorization');
  if (!(await panel.getByRole('button', { name: 'Install update…' }).isDisabled()))
    throw new Error('Duplicate install remains enabled');
  state.operation = { ...state.operation, state: 'backing_up' };
  await panel.getByText('Backing up database', { exact: true }).waitFor();
  await page.setViewportSize({ width: 390, height: 844 });
  await page.screenshot({
    path: path.join(output, 'mobile-backup.png'),
    animations: 'disabled',
    fullPage: true,
  });
  if (await page.evaluate(() => document.documentElement.scrollWidth > innerWidth))
    throw new Error('Mobile horizontal overflow');
  state = {
    ...state,
    observed_at: Date.now() / 1000 - 120,
    operation: {
      ...state.operation,
      state: 'blocked',
      error: 'Synthetic startup failure. Dispatch remains held for inspection.',
    },
  };
  await panel.getByText('Update needs attention', { exact: true }).waitFor();
  await panel
    .getByText('Progress has not refreshed recently. This is the last reported state.', {
      exact: true,
    })
    .waitFor();
  if (!(await panel.getByRole('button', { name: 'Install update…' }).isDisabled()))
    throw new Error('Blocked handover can be retried blindly');
  if (errors.length) throw new Error(errors.join('\n'));
  console.log(
    'Maintenance UI passed desktop/mobile, confirmation, durable command, progress, stale status and blocked recovery checks. Install was mocked; demo experiment was synthetic.',
  );
} finally {
  await browser.close();
}
