import { chromium, expect } from '@playwright/test';
import { readFile, mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';

const root = path.resolve(import.meta.dirname, '../..');
const target = new URL(process.env.ET_UI_URL || 'http://127.0.0.1:8794');
if (target.protocol !== 'http:' || !['127.0.0.1', 'localhost'].includes(target.hostname))
  throw new Error('Progress acceptance requires a loopback demo API');
const directory = path.join(root, '.local/stage-progress');
await mkdir(directory, { recursive: true });
const browser = await chromium.launch({ headless: true, args: ['--no-sandbox'] });
const page = await browser.newPage({ viewport: { width: 1440, height: 1050 } });
const errors = [];
page.on('pageerror', (error) => errors.push(error.message));
try {
  await page.goto(target.href);
  await page
    .getByLabel('Access token')
    .fill((await readFile(path.join(root, '.local/api-token'), 'utf8')).trim());
  await page.getByRole('button', { name: 'Open workspace', exact: true }).click();
  await page.getByRole('heading', { name: 'Experiments', exact: true }).waitFor();
  const brief =
    'Compare the fixed synthetic linear and quadratic models.\n' +
    'This synthetic acceptance description deliberately contains enough detail to test a long follow-up brief. Preserve the common split, compare held-out error, and explain the limitations.\n'.repeat(
      12,
    );
  const id = await page.evaluate(async (brief) => {
    const status = await (await fetch('/api/status')).json();
    if (status.mode !== 'demo') throw new Error('Refusing non-demo experiment creation');
    const response = await fetch('/api/experiments', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        schema_version: 1,
        request_id: crypto.randomUUID(),
        recipe_id: 'toy_compare_v1',
        brief,
      }),
    });
    if (!response.ok) throw new Error('Synthetic fixture creation failed');
    return (await response.json()).experiment_id;
  }, brief);
  await expect
    .poll(
      async () => {
        const response = await page.request.get(new URL(`/api/experiments/${id}`, target).href);
        return (await response.json()).phase;
      },
      { timeout: 60000 },
    )
    .toBe('complete');
  await page.goto(`${target.href}?project=test-project&experiment=${id}`);
  await page.getByRole('button', { name: 'All experiments', exact: true }).waitFor();
  await page.getByRole('tab', { name: 'Progress', exact: true }).click();
  const description = page.locator('.detail-heading .description');
  const more = description.getByRole('button', { name: 'See more', exact: true });
  await expect(more).toHaveAttribute('aria-expanded', 'false');
  const collapsedHeight = await description.locator('p').evaluate((p) => p.clientHeight);
  await more.click();
  await expect(description.getByRole('button', { name: 'See less', exact: true })).toHaveAttribute(
    'aria-expanded',
    'true',
  );
  if ((await description.locator('p').evaluate((p) => p.clientHeight)) <= collapsedHeight)
    throw new Error('Description did not expand');
  await description.getByRole('button', { name: 'See less', exact: true }).focus();
  await page.keyboard.press('Enter');
  await expect(more).toHaveAttribute('aria-expanded', 'false');
  await expect(
    page.getByRole('progressbar', { name: 'Stages completed', exact: true }),
  ).toHaveAttribute('value', '7');

  for (const width of [390, 360, 320]) {
    await page.setViewportSize({ width, height: 844 });
    await expect(more).toBeVisible();
    const aligned = await page.locator('.stage-toggle').evaluateAll((buttons) =>
      buttons.every((button) => {
        const children = [...button.children].map((child) => child.getBoundingClientRect());
        const centers = children.map((rect) => rect.y + rect.height / 2);
        return (
          Math.max(...centers) - Math.min(...centers) < 2 &&
          children.at(-1).right <= button.getBoundingClientRect().right + 1
        );
      }),
    );
    if (!aligned) throw new Error(`Stage row wrapped at ${width}px`);
    if (await page.evaluate(() => document.documentElement.scrollWidth > innerWidth))
      throw new Error(`Horizontal overflow at ${width}px`);
    await page.screenshot({ path: path.join(directory, `mobile-${width}.png`), fullPage: true });
  }
  await page.reload();
  await expect(more).toHaveAttribute('aria-expanded', 'false');

  // Visual-only telemetry fixtures: the underlying synthetic completed run stays immutable.
  const previewImage = await page.evaluate(() => {
    const canvas = document.createElement('canvas');
    canvas.width = 640;
    canvas.height = 360;
    const ctx = canvas.getContext('2d');
    ctx.fillStyle = '#ddd';
    ctx.fillRect(0, 0, 640, 360);
    ctx.fillStyle = '#475e98';
    ctx.fillRect(0, 180, 400, 180);
    ctx.fillStyle = '#87ae91';
    ctx.fillRect(400, 90, 240, 270);
    ctx.fillStyle = '#222';
    ctx.font = '24px sans-serif';
    ctx.fillText('Synthetic image preview', 20, 40);
    return canvas.toDataURL('image/jpeg');
  });
  let state = 'fresh';
  await page.route('**/api/experiments', async (route) => {
    if (route.request().method() !== 'GET') return route.continue();
    const response = await route.fetch();
    const data = await response.json();
    const exp = data.experiments.find((e) => e.id === id);
    if (exp) {
      exp.phase = 'training';
      exp.analysis = null;
      const job = exp.jobs.find((j) => j.stage_id === 'train_linear');
      job.state = 'submitted';
      job.scheduler_state = 'RUNNING';
      job.started_at = Date.now() / 1000 - 90;
      job.ended_at = null;
      job.tensorboard = {
        scalar: {
          tag: 'loss/train',
          points: Array.from({ length: 100 }, (_, step) => [
            step,
            2 * Math.exp(-step / 30) + 0.02 * Math.sin(step),
          ]),
        },
        image: {
          tag: 'prediction/example',
          step: 99,
          wall_time: Date.now() / 1000,
          src: previewImage,
        },
        updated_at: Date.now() / 1000,
        limited: false,
      };
      job.progress =
        state === 'missing'
          ? null
          : {
              completed: state === 'full' ? 100 : 25,
              total: 100,
              reported_at: Date.now() / 1000 - (state === 'stale' ? 180 : 0),
              eta_seconds: 300,
            };
      exp.stages.find((s) => s.id === 'train_linear').state = 'running';
      exp.snapshot.plan = {
        stages: [
          {
            id: 'train_linear',
            title:
              'Train a deliberately very long whole-stage title with multiple seeds and variants',
          },
        ],
      };
    }
    await route.fulfill({ json: data });
  });
  await page.reload();
  const work = page.getByRole('progressbar', { name: /Train a deliberately.*reported work/ });
  await expect(work).toHaveAttribute('value', '25');
  await expect(page.locator('.stage-progress-label')).toContainText('ETA ≈');
  await expect(page.locator('.stage-row.running .stage-timing')).toContainText('1m');
  const running = page.locator('.stage-row.running');
  const execution = running.locator('.stage-execution-details');
  await expect(execution).not.toHaveAttribute('open');
  await execution.locator('summary').click();
  await expect(running.locator('.stage-job-meta')).toBeVisible();
  await execution.locator('summary').click();
  for (const width of [320, 390, 1440]) {
    await page.setViewportSize({ width, height: 844 });
    const previews = running.locator('.tensorboard-previews');
    const graph = await previews.locator('figure').first().boundingBox();
    const image = await previews.locator('.preview-image-button').boundingBox();
    if (!graph || !image || graph.x + graph.width > image.x + 1)
      throw new Error(`Graph and image are not side by side at ${width}px`);
    await running.screenshot({
      path: path.join(directory, `expanded-${width}.png`),
      animations: 'disabled',
    });
    if (width <= 390 && (image.height > 96 || (await running.boundingBox()).height > 500))
      throw new Error(
        `Expanded stage is too tall at ${width}px: image ${image.height}px, stage ${(await running.boundingBox()).height}px`,
      );
  }
  await page.setViewportSize({ width: 390, height: 844 });
  const enlarge = running.getByRole('button', { name: 'Enlarge prediction/example' });
  await enlarge.click();
  await expect(page.getByRole('dialog', { name: 'prediction/example at step 99' })).toBeVisible();
  await page.keyboard.press('Escape');
  await expect(enlarge).toBeFocused();
  await enlarge.click();
  await page.getByRole('button', { name: 'Close image preview' }).click();
  await expect(page.getByRole('dialog')).toHaveCount(0);
  await page.locator('.stage-row.running .stage-toggle').click();
  await expect(work).toBeVisible();
  for (const width of [320, 390, 1440]) {
    await page.setViewportSize({ width, height: 1000 });
    const previews = page.locator('.tensorboard-previews');
    await expect(previews.getByRole('img', { name: /loss\/train/ })).toBeVisible();
    await expect(previews.getByRole('img', { name: /prediction\/example/ })).toBeVisible();
    const image = previews.locator('.preview-image-button img');
    await expect
      .poll(() => image.evaluate((img) => img.complete && img.naturalWidth > 0))
      .toBe(true);
    if (await page.evaluate(() => document.documentElement.scrollWidth > innerWidth))
      throw new Error(`TensorBoard horizontal overflow at ${width}px`);
    await page.screenshot({
      path: path.join(directory, `tensorboard-${width}.png`),
      fullPage: true,
    });
  }
  await page.screenshot({ path: path.join(directory, 'mobile-progress.png'), fullPage: true });
  await page.setViewportSize({ width: 1440, height: 1050 });
  await page.screenshot({ path: path.join(directory, 'desktop-progress.png'), fullPage: true });
  state = 'stale';
  await page.reload();
  await expect(work).toHaveAttribute('value', '25');
  await expect(page.locator('.stage-progress-label')).toContainText(
    'ETA ≈ up to 5m 0s remaining · last report',
  );
  state = 'full';
  await page.reload();
  await expect(work).toHaveAttribute('value', '100');
  await expect(page.locator('.stage-progress-label')).toContainText('checking outputs');
  await expect(
    page.getByRole('progressbar', { name: 'Stages completed', exact: true }),
  ).toHaveAttribute('value', '6');
  state = 'missing';
  await page.reload();
  await expect(page.locator('.stage-progress')).toContainText('Progress and ETA unavailable');
  await expect(work).toHaveCount(0);
  if (errors.length) throw new Error(errors.join('\n'));
  await writeFile(
    path.join(directory, 'acceptance.json'),
    JSON.stringify(
      {
        mode: 'synthetic demo',
        experiment_id: id,
        progress_views:
          'mocked numeric telemetry; parser/persistence independently integration tested',
        physical_phone: 'not tested',
        checks: [
          'long multiline brief collapsed, mouse/keyboard toggle and reload',
          '320px, 360px, 390px rows aligned on one line',
          'equal-stage completion bar',
          'reported percentage/ETA visible with stage collapsed',
          'elapsed time in stage header',
          'stale and missing telemetry',
          '100% work does not complete stage',
          'TensorBoard curve and decoded image visible under collapsed stage at 320px, 390px and desktop',
          'compact expanded stage, side-by-side graph/image and execution detail disclosure',
          'image enlargement, Escape/close and focus restoration',
        ],
        errors,
      },
      null,
      2,
    ) + '\n',
  );
  console.log('Progress browser acceptance passed (synthetic demo and mocked telemetry views).');
} finally {
  await browser.close();
}
