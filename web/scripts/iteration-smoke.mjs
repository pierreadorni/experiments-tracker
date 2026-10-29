// Real synthetic workflow and browser; requires the dedicated ephemeral fixture server.
import { chromium } from '@playwright/test';
import { readFile, mkdir, writeFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
const target = new URL(process.env.ET_UI_URL || 'http://127.0.0.1:8879');
if (target.protocol !== 'http:' || !['127.0.0.1', 'localhost'].includes(target.hostname))
  throw new Error('Loopback demo fixture required');
if (!process.env.ET_FIXTURE_TOKEN_FILE)
  throw new Error('Dedicated synthetic fixture token file required');
const token = (await readFile(process.env.ET_FIXTURE_TOKEN_FILE, 'utf8')).trim();
async function api(route, body) {
  const response = await fetch(new URL(route, target), {
    method: body ? 'POST' : 'GET',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  if (!response.ok) throw new Error(`Fixture HTTP ${response.status}: ${route}`);
  return response.json();
}
if ((await api('/api/status')).mode !== 'demo') throw new Error('Refusing non-demo API');
const { experiment_id: eid } = await api('/api/experiments', {
  schema_version: 1,
  request_id: randomUUID(),
  recipe_id: 'agent_experiment_v1',
  brief: 'Exercise synthetic iteration mechanisms.',
});
async function waitFor(predicate) {
  const deadline = Date.now() + 45000;
  while (Date.now() < deadline) {
    const exp = await api(`/api/experiments/${eid}`);
    if (predicate(exp)) return exp;
    await new Promise((r) => setTimeout(r, 150));
  }
  throw new Error('Synthetic workflow did not reach expected state');
}
const review = await waitFor((e) => e.plan_review?.state === 'awaiting_review');
await api(`/api/experiments/${eid}/plan-review`, {
  schema_version: 1,
  request_id: randomUUID(),
  decision: 'approve',
  plan_revision: review.plan_review.revision,
});
const failed = await waitFor((e) => e.phase === 'failed');
if (failed.partial_results.tasks.evaluate.result.metrics[0].value !== 42)
  throw new Error('Metrics lost at plot failure');
const producers = failed.jobs
  .filter((j) => j.stage_id !== 'plot')
  .map((j) => j.intent_id)
  .sort();
const browser = await chromium.launch({ headless: true, args: ['--no-sandbox'] });
const page = await browser.newPage();
const errors = [];
page.on('pageerror', (e) => errors.push(e.message));
const output = process.env.ET_SMOKE_OUTPUT || '/tmp/iteration-browser-evidence';
await mkdir(output, { recursive: true });
try {
  await page.goto(target.href);
  await page.getByLabel('Access token').fill(token);
  await page.getByRole('button', { name: 'Open workspace' }).click();
  await page.goto(new URL(`/?experiment=${eid}`, target).href);
  await page.getByRole('tab', { name: 'Report', exact: true }).click();
  await page.getByRole('heading', { name: 'Results so far' }).waitFor();
  await page.getByRole('heading', { name: 'Requested work remaining' }).waitFor();
  const failedTask = page.locator('.incremental-results li').filter({ hasText: 'Synthetic plot' });
  await failedTask.waitFor();
  if (!(await failedTask.textContent()).includes('failed'))
    throw new Error('Plot failure is not visible');
  for (const [label, width, height] of [
    ['desktop', 1440, 1050],
    ['mobile', 390, 844],
  ]) {
    await page.setViewportSize({ width, height });
    await page.waitForTimeout(350);
    const closeNavigation = page.locator('.sidebar.nav-open .close-nav');
    if (await closeNavigation.count()) await closeNavigation.click();
    await page.waitForTimeout(350);
    if (await page.evaluate(() => document.documentElement.scrollWidth > innerWidth))
      throw new Error('Horizontal overflow');
    await page.screenshot({ path: `${output}/failed-${label}.png`, fullPage: true });
  }
  await api('/fixture/repair', {});
  const complete = await waitFor((e) => e.phase === 'complete');
  if (
    JSON.stringify(
      complete.jobs
        .filter((j) => j.stage_id !== 'plot')
        .map((j) => j.intent_id)
        .sort(),
    ) !== JSON.stringify(producers)
  )
    throw new Error('Figure repair repeated training/evaluation');
  if (complete.jobs.length !== 4) throw new Error('Expected only one new plot attempt');
  await page.reload();
  await page.getByRole('tab', { name: 'Report', exact: true }).click();
  await page.getByRole('heading', { name: 'Results so far' }).waitFor();
  if (await page.getByRole('heading', { name: 'Requested work remaining' }).count())
    throw new Error('Requested completion status is stale');
  await page.locator('.incremental-results img').waitFor();
  for (const [label, width, height] of [
    ['desktop', 1440, 1050],
    ['mobile', 390, 844],
  ]) {
    await page.setViewportSize({ width, height });
    await page.waitForTimeout(350);
    const closeNavigation = page.locator('.sidebar.nav-open .close-nav');
    if (await closeNavigation.count()) await closeNavigation.click();
    await page.waitForTimeout(350);
    if (await page.evaluate(() => document.documentElement.scrollWidth > innerWidth))
      throw new Error('Recovery overflow');
    await page.screenshot({ path: `${output}/recovered-${label}.png`, fullPage: true });
  }
  if (errors.length) throw new Error(errors.join('\n'));
  await writeFile(
    `${output}/evidence.json`,
    JSON.stringify(
      {
        synthetic: true,
        experiment_id: eid,
        producer_ids: producers,
        job_attempts: complete.jobs.length,
        mode: 'demo',
        scope:
          'Local synthetic subprocesses, mocked model and scheduler; no live Slurm, managed runtime or phone acceptance.',
      },
      null,
      2,
    ),
  );
  console.log(
    JSON.stringify({
      synthetic: true,
      experiment_id: eid,
      job_attempts: complete.jobs.length,
      screenshots: output,
    }),
  );
} finally {
  await browser.close();
}
