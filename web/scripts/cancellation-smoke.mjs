// Synthetic local acceptance only; never run against a live experiment.
import { chromium } from '@playwright/test';
import { readFile, mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
const target = new URL(process.env.ET_UI_URL || 'http://127.0.0.1:8879');
if (target.protocol !== 'http:' || !['127.0.0.1', 'localhost'].includes(target.hostname))
  throw new Error('Cancellation smoke requires a loopback demo');
const output = path.resolve(process.env.ET_SCREENSHOTS || '/tmp/et-cancellation-screenshots');
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
  const status = await (await page.request.get(new URL('/api/status', target).href)).json();
  if (status.mode !== 'demo') throw new Error('Refusing non-demo state');
  const post = async (route, data) => {
    const response = await page.request.post(new URL(route, target).href, {
      headers: { Origin: target.origin },
      data: { schema_version: 1, request_id: crypto.randomUUID(), ...data },
    });
    if (!response.ok()) throw new Error(await response.text());
    return response.json();
  };
  const brief = 'Synthetic cancellation browser acceptance ' + crypto.randomUUID();
  const created = await post('/api/experiments', { brief, recipe_id: 'agent_experiment_v1' });
  const eid = created.experiment_id;
  await post(`/api/experiments/${eid}/actions`, { action: 'pause_agent' });
  await post(`/api/experiments/${eid}/actions`, { action: 'pause_new_compute' });
  await page.reload();
  await page.getByLabel('Search experiments').fill(brief);
  await page.locator('.experiment-row').first().click();
  for (const [label, width, height] of [
    ['desktop', 1440, 1050],
    ['mobile', 390, 844],
  ]) {
    await page.setViewportSize({ width, height });
    await page.getByRole('button', { name: 'Experiment actions', exact: true }).click();
    await page.getByRole('button', { name: 'Cancel experiment', exact: true }).click();
    const dialog = page.getByRole('dialog', { name: 'Cancel experiment?' });
    await dialog.waitFor();
    if (
      !(await dialog
        .getByRole('button', { name: 'Keep experiment' })
        .evaluate((el) => el === document.activeElement))
    )
      throw new Error('Safe initial focus missing');
    if (await page.evaluate(() => document.documentElement.scrollWidth > innerWidth))
      throw new Error('Horizontal overflow');
    await page.screenshot({ path: path.join(output, `${label}-confirm.png`) });
    await page.keyboard.press('Escape');
    if (await page.getByRole('dialog').count()) throw new Error('Escape failed');
    // Escape restores focus; dismiss the action menu before the next viewport.
    await page.keyboard.press('Escape');
  }
  await page.getByRole('button', { name: 'Experiment actions', exact: true }).click();
  await page.getByRole('button', { name: 'Cancel experiment', exact: true }).click();
  await page
    .getByRole('dialog')
    .getByRole('button', { name: 'Cancel experiment', exact: true })
    .click();
  await page
    .locator('.detail-heading .badge')
    .filter({ hasText: /cancelled/i })
    .waitFor();
  const result = await (
    await page.request.get(new URL(`/api/experiments/${eid}`, target).href)
  ).json();
  if (
    result.phase !== 'cancelled' ||
    !result.pause_agent ||
    !result.pause_compute ||
    result.jobs.length
  )
    throw new Error('Cancellation did not stop early work');
  await page.goto(new URL(`/?experiment=${eid}`, target).href);
  await page
    .locator('.detail-heading .badge')
    .filter({ hasText: /cancelled/i })
    .waitFor();
  await page.screenshot({ path: path.join(output, 'mobile-cancelled.png') });
  if (errors.length) throw new Error(errors.join('\n'));
  await writeFile(
    path.join(output, 'evidence.json'),
    JSON.stringify(
      { synthetic: true, experiment_id: eid, phase: result.phase, screenshots: output },
      null,
      2,
    ),
  );
  console.log(JSON.stringify({ synthetic: true, phase: result.phase, screenshots: output }));
} catch (error) {
  await page.screenshot({ path: path.join(output, 'failure.png') });
  console.error((await page.locator('body').innerText()).slice(-5000));
  throw error;
} finally {
  await browser.close();
}
