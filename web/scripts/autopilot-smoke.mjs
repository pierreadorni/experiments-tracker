// Synthetic browser acceptance: real demo authentication and experiment creation,
// mocked research boundary. No live model, Slurm job, restart or scientific claim.
import { chromium } from '@playwright/test';
import { readFile, mkdir, writeFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
const target = new URL(process.env.ET_UI_URL || 'http://127.0.0.1:8879');
if (target.protocol !== 'http:' || !['127.0.0.1', 'localhost'].includes(target.hostname))
  throw new Error('Loopback demo required');
if (!process.env.ET_TOKEN_FILE) throw new Error('Dedicated demo token file required');
const token = (await readFile(process.env.ET_TOKEN_FILE, 'utf8')).trim();
const output = process.env.ET_SCREENSHOTS || '/tmp/et-autopilot-screenshots';
await mkdir(output, { recursive: true });
const browser = await chromium.launch({
  headless: true,
  args: ['--no-sandbox'],
  executablePath: process.env.ET_CHROMIUM_EXECUTABLE || undefined,
});
const page = await browser.newPage({
  viewport: { width: 1440, height: 1050 },
  reducedMotion: 'reduce',
  acceptDownloads: true,
});
const errors = [];
page.on('pageerror', (e) => errors.push(e.message));
try {
  await page.goto(target.href);
  await page.getByLabel('Access token').fill(token);
  await page.getByRole('button', { name: 'Open workspace' }).click();
  await page.getByRole('heading', { name: 'Experiments', exact: true }).waitFor();
  const realStatus = await (await page.request.get(new URL('/api/status', target).href)).json();
  if (realStatus.mode !== 'demo') throw new Error('Refusing non-demo fixture');
  const created = await page.request.post(new URL('/api/experiments', target).href, {
    headers: { Origin: target.origin },
    data: {
      schema_version: 1,
      request_id: randomUUID(),
      recipe_id: 'toy_compare_v1',
      brief: 'Synthetic Autopilot browser acceptance; no research.',
    },
  });
  if (!created.ok()) throw new Error('Synthetic experiment creation failed');
  const eid = (await created.json()).experiment_id,
    runId = randomUUID();
  let state = { schema_version: 1, available: true, run: null, pending_commands: [] };
  const commands = [];
  let handoffServed = false;
  await page.route('**/api/status', (route) =>
    route.fulfill({ json: { ...realStatus, autopilot: { available: true, schema_version: 1 } } }),
  );
  await page
    .context()
    .route(/\/api\/projects\/test-project\/autopilot(?:\/.*)?$/, async (route) => {
      const path = new URL(route.request().url()).pathname;
      if (path.endsWith('/commands')) {
        const payload = route.request().postDataJSON();
        commands.push(payload);
        if (payload.kind === 'configure') {
          const p = payload.policy,
            total = p.handoff_at - p.start_at;
          state = {
            ...state,
            observed_at: Date.now() / 1000,
            policy: p,
            run: {
              id: runId,
              revision: 1,
              status: 'draft',
              phase: 'foundation',
              foundation: 'pending',
              hold_reason: null,
              central_argument: null,
              uncertainty: null,
              handoff: null,
            },
            calendar: {
              phase: 'foundation',
              week: 0,
              weeks_remaining: total / 604800,
              boundaries: {
                foundation: p.start_at + total * 0.01,
                main_argument: p.start_at + total * 0.6,
                ablations: p.start_at + total * 0.9,
                writing: p.handoff_at,
              },
            },
            budget: {
              cap_seconds: 360000,
              consumed_seconds: 72000,
              reserved_seconds: 144000,
              available_seconds: 144000,
              unknown_seconds: 0,
              reset_at: p.anchor_at + 604800,
              anomalies: [],
            },
            documents: [],
            experiments: [],
            stage_holds: [],
            notices: [],
            backlog: [],
            messages: [],
            decisions: [],
          };
        } else {
          if (payload.expected_revision !== state.run.revision)
            throw new Error('UI sent stale expected revision');
          state.run = { ...state.run, revision: state.run.revision + 1 };
          if (payload.kind === 'activate') state.run.status = 'active';
          if (payload.kind === 'pause') state.run.status = 'paused';
          if (payload.kind === 'resume') state.run.status = 'active';
          if (payload.kind === 'message')
            state.messages.push({
              seq: commands.length,
              kind: 'message',
              document: JSON.stringify({ text: payload.text }),
            });
          if (payload.kind === 'answer') {
            const notice = state.notices.find((n) => n.id === payload.input_id);
            if (!notice) throw new Error('Answer did not reference the saved question');
            notice.resolved_at = now;
            state.run.hold_reason = null;
          }
        }
        await route.fulfill({
          status: 202,
          json: { schema_version: 1, state: 'applied', request_id: payload.request_id },
        });
      } else if (path.includes('/documents/'))
        await route.fulfill({
          body: 'Synthetic paper draft. TODO: real results.',
          contentType: 'text/plain',
        });
      else if (path.endsWith('/handoff')) {
        handoffServed = true;
        await route.continue();
      } else await route.fulfill({ json: state });
    });
  await page.reload();
  await page.getByRole('button', { name: 'Project knowledge', exact: true }).click();
  const panel = page.locator('.autopilot');
  await panel.getByRole('button', { name: 'Set up Autopilot' }).click();
  await panel
    .getByLabel('Research subject', { exact: true })
    .fill('Synthetic research-controller acceptance');
  await panel
    .getByLabel('Scope and central question')
    .fill('Test project authority and UI behavior. No scientific research.');
  await panel.getByLabel('Publication goal').fill('Software acceptance only');
  await panel.getByLabel('Venue and year').fill('Synthetic venue, 2026');
  const stamp = (seconds) => {
    const d = new Date(seconds * 1000);
    return new Date(d.getTime() - d.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
  };
  const now = Math.floor(Date.now() / 1000);
  await panel.getByLabel('Researcher handoff', { exact: true }).fill(stamp(now + 10 * 604800));
  await panel.getByLabel('Submission deadline').fill(stamp(now + 11 * 604800));
  for (const [name, width, height] of [
    ['desktop', 1440, 1050],
    ['mobile', 390, 844],
  ]) {
    await page.setViewportSize({ width, height });
    await page.screenshot({ path: `${output}/setup-${name}.png`, fullPage: true });
    if (await page.evaluate(() => document.documentElement.scrollWidth > innerWidth))
      throw new Error('Setup horizontal overflow');
  }
  await panel.getByRole('button', { name: 'Review policy', exact: true }).click();
  await panel.getByRole('button', { name: 'Save draft', exact: true }).click();
  await panel.getByRole('button', { name: 'Review activation', exact: true }).click();
  await panel.getByRole('button', { name: 'Activate Autopilot', exact: true }).click();
  await panel.getByRole('button', { name: 'Pause Autopilot', exact: true }).click();
  await panel.getByRole('button', { name: 'Resume Autopilot', exact: true }).click();
  state = {
    ...state,
    run: { ...state.run, hold_reason: 'autopilot_researcher_input' },
    notices: [
      {
        id: randomUUID(),
        kind: 'researcher_input',
        text: 'Synthetic recovered question: select a fixture source to continue.',
        resolved_at: null,
      },
    ],
  };
  await panel.getByLabel('Message to Autopilot').fill('Synthetic refresh for recovered guidance.');
  await panel.getByRole('button', { name: 'Send to Autopilot', exact: true }).click();
  await panel.getByText('Waiting for your reply.', { exact: false }).waitFor();
  for (const [name, width, height] of [
    ['desktop', 1440, 1050],
    ['mobile', 390, 844],
  ]) {
    await page.setViewportSize({ width, height });
    await page.screenshot({ path: `${output}/guidance-${name}.png`, fullPage: true });
    if (await page.evaluate(() => document.documentElement.scrollWidth > innerWidth))
      throw new Error('Guidance view horizontal overflow');
  }
  await panel.getByRole('button', { name: 'Answer researcher question', exact: true }).click();
  await panel.getByLabel('Your answer').fill('Use only the synthetic fixture source.');
  await panel.getByRole('button', { name: 'Send to Autopilot', exact: true }).click();
  await panel.getByText('Waiting for your reply.', { exact: false }).waitFor({ state: 'hidden' });
  state = {
    ...state,
    observed_at: now - 300,
    run: {
      ...state.run,
      foundation: 'ready',
      central_argument:
        'Synthetic test: one experiment retains its identity across allowance windows.',
      uncertainty: 'Real cluster accounting and managed retrieval are separate live prerequisites.',
    },
    documents: [
      { name: 'paper.tex', version: 1 },
      { name: 'weekly_report', version: 1 },
    ],
    experiments: [{ experiment_id: eid, phase: 'training', deferred: 0, priority: 1 }],
    stage_holds: [
      {
        experiment_id: eid,
        stage_id: 'next_allocation',
        reason: 'autopilot_weekly_gpu_limit',
        detail: '{}',
      },
    ],
    backlog: [
      {
        id: 'q1',
        document: {
          question: 'Does the synthetic multi-week fixture preserve exact intents?',
          stop_condition: 'Stop after infrastructure acceptance',
          review_at: now + 604800,
          priority: 1,
          status: 'active',
        },
      },
    ],
  };
  await panel
    .getByLabel('Message to Autopilot')
    .fill(
      'Synthetic guidance: retain negative fixture results and pause low-priority exploration.',
    );
  await panel.getByRole('button', { name: 'Send to Autopilot', exact: true }).click();
  await panel.getByText('Waiting for autopilot limit reset', { exact: false }).waitFor();
  await panel.getByText('Executor observation is old', { exact: false }).waitFor();
  await panel.getByRole('button', { name: 'Defer', exact: true }).click();
  await panel
    .getByLabel('Reason', { exact: true })
    .fill('Synthetic deferral preserves history and user pauses.');
  await panel.getByRole('button', { name: 'Confirm defer', exact: true }).click();
  for (const [name, width, height] of [
    ['desktop', 1440, 1050],
    ['mobile', 390, 844],
  ]) {
    await page.setViewportSize({ width, height });
    await page.screenshot({ path: `${output}/active-${name}.png`, fullPage: true });
    if (await page.evaluate(() => document.documentElement.scrollWidth > innerWidth))
      throw new Error('Active view horizontal overflow');
  }
  const realHandoff = await page.request.post(new URL('/fixture/autopilot-handoff', target).href, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!realHandoff.ok()) throw new Error('Could not prepare draft-only synthetic handoff fixture');
  const downloadPromise = page.waitForEvent('download');
  await panel.getByRole('link', { name: 'Download current handoff' }).click();
  const download = await downloadPromise;
  const failure = await download.failure();
  if (failure)
    throw new Error(
      'Download failure: ' +
        failure +
        ' URL ' +
        download.url() +
        ' filename ' +
        download.suggestedFilename() +
        ' intercepted=' +
        handoffServed,
    );
  const downloaded = await readFile(await download.path());
  if (downloaded[0] !== 80 || downloaded[1] !== 75)
    throw new Error('Handoff package is not a ZIP archive: ' + download.suggestedFilename());
  if (errors.length) throw new Error(errors.join('\n'));
  if (
    !commands.some((c) => c.kind === 'activate') ||
    !commands.some((c) => c.kind === 'answer') ||
    !commands.some((c) => c.kind === 'control' && c.control === 'defer')
  )
    throw new Error('Missing control paths');
  await writeFile(
    `${output}/evidence.json`,
    JSON.stringify(
      {
        synthetic: true,
        experiment_id: eid,
        commands: commands.map((c) => c.kind),
        scope:
          'Real local demo authentication and synthetic experiment; mocked project research boundary; no live Slurm, managed runtime or phone acceptance.',
      },
      null,
      2,
    ),
  );
  console.log(JSON.stringify({ synthetic: true, experiment_id: eid, screenshots: output }));
} finally {
  await browser.close();
}
