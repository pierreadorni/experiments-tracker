// Read an existing completed result. This never creates an experiment or sends a research command.
import { chromium } from '@playwright/test';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { spawnSync } from 'node:child_process';
import { isDeepStrictEqual } from 'node:util';
import { unzipSync } from 'fflate';

const root = path.resolve(import.meta.dirname, '../..');
const target = new URL(process.env.ET_UI_URL || 'http://127.0.0.1:8767');
const eid = process.env.ET_EXPERIMENT_ID;
if (
  target.protocol !== 'http:' ||
  !['127.0.0.1', 'localhost', '[::1]'].includes(target.hostname) ||
  target.username ||
  target.password
)
  throw new Error('Verification requires the authenticated loopback API');
if (!/^[0-9a-f-]{36}$/.test(eid || ''))
  throw new Error('Set ET_EXPERIMENT_ID to the completed experiment UUID');
const output = path.join(root, '.local/verified-result', eid);
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ headless: true, args: ['--no-sandbox'] });
const page = await browser.newPage({ viewport: { width: 1440, height: 1050 } });
const errors = [];
page.on('pageerror', (error) => errors.push(error.message));
try {
  await page.goto(target.href);
  await page
    .getByLabel('Access token')
    .fill((await readFile(path.join(root, '.local/api-token'), 'utf8')).trim());
  await page.getByRole('button', { name: 'Open workspace' }).click();
  await page.getByRole('heading', { name: 'Experiments', exact: true }).waitFor();
  const response = await page.request.get(new URL(`/api/experiments/${eid}`, target).href);
  if (!response.ok()) throw new Error('Cannot read the saved result');
  const exp = await response.json();
  if (exp.phase !== 'complete') throw new Error(`Result is ${exp.phase}`);
  await page.locator('.experiment-row').filter({ hasText: exp.analysis.title }).click();
  await page.getByRole('tab', { name: 'Report', exact: true }).click();
  const frame = page.frameLocator('iframe');
  await frame.getByRole('heading', { level: 1 }).waitFor();
  await frame
    .locator('img')
    .first()
    .evaluate(
      (img) =>
        new Promise((resolve, reject) => {
          if (img.complete && img.naturalWidth > 0) return resolve(true);
          img.onload = () => resolve(true);
          img.onerror = () => reject(new Error('Figure did not load'));
        }),
    );
  await page.screenshot({ path: path.join(output, 'report-desktop.png'), fullPage: true });
  await page.getByRole('tab', { name: 'Evidence', exact: true }).click();
  const pending = page.waitForEvent('download');
  await page.getByRole('button', { name: 'Download evidence bundle' }).click();
  await (await pending).saveAs(path.join(output, 'evidence.zip'));
  const files = unzipSync(await readFile(path.join(output, 'evidence.zip')));
  for (const [name, bytes] of Object.entries(files)) {
    if (!/^(?:evidence\/)?[a-zA-Z0-9][a-zA-Z0-9._-]*$/.test(name))
      throw new Error('Unsafe export path');
    const file = path.join(output, name);
    await mkdir(path.dirname(file), { recursive: true });
    await writeFile(file, bytes);
  }
  const offline = await browser.newPage();
  await offline.route(/^https?:/, (route) => route.abort());
  await offline.goto(pathToFileURL(path.join(output, 'report.html')).href);
  await offline.getByRole('heading', { level: 1 }).waitFor();
  if (
    !(await offline
      .locator('img')
      .first()
      .evaluate((img) => img.complete && img.naturalWidth > 0))
  )
    throw new Error('Offline figure failed');
  const manifest = JSON.parse(new TextDecoder().decode(files['evidence/manifest.json']));
  if (!isDeepStrictEqual(manifest, exp.snapshot)) throw new Error('Exported manifest differs');
  const figureName =
    manifest.plan?.figures?.find((name) => name.endsWith('.svg')) || 'predictions.svg';
  if (files['evidence/' + figureName]) {
    await offline.goto(pathToFileURL(path.join(output, 'evidence', figureName)).href);
    await offline.locator('svg').screenshot({ path: path.join(output, 'frontier.png') });
  }
  let reproduced = false;
  // Opt-in only: downloading evidence must not automatically execute scientific code.
  if (process.env.ET_REPRODUCE === 'true') {
    if (new TextDecoder().decode(files['evidence/requirements.txt'] || new Uint8Array()).trim())
      throw new Error('Install/review pinned dependencies separately before reproduction');
    const stages = manifest.plan?.stages.map((s) => s.id) || [
      'train_linear',
      'train_quadratic',
      'evaluate_and_plot',
    ];
    for (const stage of stages) {
      const result = spawnSync(
        '/usr/bin/python3',
        [
          path.join(output, 'evidence/recipe.py'),
          stage,
          '--output',
          path.join(output, 'reproduction'),
        ],
        { encoding: 'utf8', timeout: 20000 },
      );
      if (result.status !== 0) throw new Error(`Reproduction failed: ${result.stderr}`);
    }
    const original = JSON.parse(new TextDecoder().decode(files['evidence/results.json']));
    const repeated = JSON.parse(
      await readFile(path.join(output, 'reproduction/results.json'), 'utf8'),
    );
    if (!isDeepStrictEqual(original, repeated)) throw new Error('Reproduced results differ');
    reproduced = true;
  }
  await offline.close();
  await page.getByRole('tab', { name: 'Progress', exact: true }).click();
  await page.setViewportSize({ width: 390, height: 844 });
  if (await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1))
    throw new Error('Mobile overflow');
  await page.screenshot({ path: path.join(output, 'progress-mobile.png'), fullPage: true });
  if (errors.length) throw new Error(errors.join('; '));
  const result = {
    passed: true,
    experiment_id: eid,
    recipe_id: exp.recipe_id,
    artifacts: exp.artifacts.length,
    checks: [
      'authenticated existing result',
      'report and arbitrary figure',
      'evidence ZIP',
      'offline report with network blocked',
      'frozen manifest',
      '390px mobile layout',
    ],
    reproduced,
    errors,
  };
  await writeFile(path.join(output, 'acceptance.json'), JSON.stringify(result, null, 2));
  console.log(JSON.stringify(result));
} finally {
  await browser.close();
}
