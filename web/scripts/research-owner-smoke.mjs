// C4 loopback owner approval acceptance against a temporary real FastAPI/SQLite fixture.
import assert from 'node:assert/strict';
import { chromium } from '@playwright/test';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

const target = new URL(process.env.ET_UI_URL || 'http://127.0.0.1:8879');
const root = process.env.ET_C4_FIXTURE_ROOT;
const output = process.env.ET_C4_OUTPUT || '/tmp/research-owner-acceptance';
if (target.protocol !== 'http:' || !['127.0.0.1', 'localhost'].includes(target.hostname) || !root)
  throw new Error('A temporary loopback C4 fixture is required');
const token = (await readFile(path.join(root, 'api-token'), 'utf8')).trim();
const experimentId = (await readFile(path.join(root, 'experiment-id'), 'utf8')).trim();
const browser = await chromium.launch({
  headless: true,
  args: ['--no-sandbox'],
  ...(process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH
    ? { executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH }
    : {}),
});
const page = await browser.newPage({ viewport: { width: 1365, height: 1000 } });
const errors = [];
page.on('pageerror', (error) => errors.push(error.message));
let approvalRequest;
let approvalResponse;
page.on('request', (request) => {
  if (
    request.method() === 'POST' &&
    request.url().endsWith(`/api/experiments/${experimentId}/answers`)
  )
    approvalRequest = request.postDataJSON();
});
try {
  const detailResponse = await page.request.get(
    new URL(`/api/experiments/${experimentId}`, target).href,
    {
      headers: { Authorization: `Bearer ${token}` },
    },
  );
  assert.equal(detailResponse.status(), 200);
  const detail = await detailResponse.json();
  assert.equal(detail.contract.execution_contract, 4);
  assert.equal(JSON.stringify(detail).includes('PRIVATE CHECKPOINT'), false);
  assert.equal(JSON.stringify(detail).includes('never show this content'), false);

  await page.goto(target.href);
  await page.getByLabel('Access token').fill(token);
  await page.getByRole('button', { name: 'Open workspace' }).click();
  await page.goto(new URL(`/?experiment=${experimentId}`, target).href);
  await page.getByRole('heading', { name: 'Synthetic C4 approval UI acceptance.' }).waitFor();
  await page.getByRole('heading', { name: 'Research activity' }).waitFor();
  await page.getByText('Proposed objective:', { exact: false }).waitFor();
  assert.equal(await page.getByText('May this exact objective proceed?').count(), 1);
  assert.equal(await page.getByText('Run the bounded synthetic CPU comparison only.').count(), 1);
  assert.equal(await page.getByText(/PRIVATE CHECKPOINT/).count(), 0);
  assert.equal(await page.locator('.plan-review, .steering-panel, .stage-timeline').count(), 0);
  assert.equal(await page.getByRole('heading', { name: 'Stages', exact: true }).count(), 0);
  assert.equal(await page.getByRole('tab', { name: 'Report', exact: true }).count(), 1);
  await mkdir(output, { recursive: true });
  await page.screenshot({
    path: path.join(output, 'c4-owner-approval-desktop.png'),
    fullPage: true,
  });
  await page.setViewportSize({ width: 390, height: 844 });
  const closeNavigation = page.locator('.sidebar.nav-open .close-nav');
  if (await closeNavigation.count()) {
    await closeNavigation.click();
    await page.waitForFunction(
      () => !document.querySelector('.sidebar')?.classList.contains('nav-open'),
    );
  }
  await page.evaluate(() =>
    Promise.all(
      [...document.querySelectorAll('.sidebar')].flatMap((element) =>
        element.getAnimations().map((animation) => animation.finished),
      ),
    ),
  );
  assert.match(
    await page.locator('.sidebar').evaluate((element) => getComputedStyle(element).transform),
    /^matrix\(1, 0, 0, 1, -/,
  );
  await page.screenshot({
    path: path.join(output, 'c4-owner-approval-mobile.png'),
    fullPage: true,
  });
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
  assert.equal(await page.locator('.plan-review, .steering-panel, .stage-timeline').count(), 0);
  await page.setViewportSize({ width: 1365, height: 1000 });

  const responseWait = page.waitForResponse(
    (response) =>
      response.url().endsWith(`/api/experiments/${experimentId}/answers`) &&
      response.request().method() === 'POST',
  );
  await page.getByRole('button', { name: 'Accept', exact: true }).click();
  const response = await responseWait;
  assert.equal(response.status(), 200);
  approvalResponse = await response.json();
} catch (error) {
  throw error;
} finally {
  await browser.close();
}

// The same request identity/body must replay the saved receipt instead of deciding again.
assert.ok(approvalRequest, 'approval POST was not observed');
assert.equal(approvalRequest.answer, 'accept');
assert.ok(approvalResponse, 'approval receipt was not observed');
const replay = await fetch(new URL(`/api/experiments/${experimentId}/answers`, target), {
  method: 'POST',
  headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
  body: JSON.stringify(approvalRequest),
});
assert.equal(replay.status, 200);
assert.deepEqual(await replay.json(), approvalResponse);
const after = await fetch(new URL(`/api/experiments/${experimentId}`, target), {
  headers: { Authorization: `Bearer ${token}` },
});
assert.equal(after.status, 200);
const final = await after.json();
assert.equal(final.pending_request.answer, 'accept');
assert.equal(JSON.stringify(final).includes('PRIVATE CHECKPOINT'), false);
assert.equal(errors.length, 0, errors.join('\n'));
await writeFile(
  path.join(output, 'acceptance.json'),
  JSON.stringify(
    {
      synthetic: true,
      experiment_id: experimentId,
      exact_approval: 'accept',
      replay_receipt_equal: true,
      c3_stage_controls_visible: false,
      private_checkpoint_visible: false,
    },
    null,
    2,
  ),
);
console.log(
  `C4 loopback owner acceptance passed; screenshot: ${path.join(output, 'c4-owner-approval-desktop.png')}`,
);
