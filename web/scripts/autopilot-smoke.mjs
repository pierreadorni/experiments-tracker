// Synthetic browser acceptance of the Autopilot lead model against the C4 loopback fixture
// (research-owner-fixture.py): real demo authentication and a real synthetic experiment, with the
// `autopilot:<project>` projection and its commands mocked at the browser. No live model, Slurm job
// or scientific claim.
import assert from 'node:assert/strict';
import { chromium, expect } from '@playwright/test';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { leadId } from '../src/lead.ts';

const target = new URL(process.env.ET_UI_URL || 'http://127.0.0.1:8879');
const root = process.env.ET_C4_FIXTURE_ROOT;
if (target.protocol !== 'http:' || !['127.0.0.1', 'localhost'].includes(target.hostname) || !root)
  throw new Error('A temporary loopback C4 fixture is required');
const token = (await readFile(path.join(root, 'api-token'), 'utf8')).trim();
const eid = (await readFile(path.join(root, 'experiment-id'), 'utf8')).trim();
const output = process.env.ET_SCREENSHOTS || '/tmp/et-autopilot-screenshots';
await mkdir(output, { recursive: true });
const browser = await chromium.launch({
  headless: true,
  args: ['--no-sandbox'],
  executablePath:
    process.env.ET_CHROMIUM_EXECUTABLE ||
    process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH ||
    undefined,
});
const page = await browser.newPage({
  viewport: { width: 1440, height: 1050 },
  reducedMotion: 'reduce',
});
const errors = [];
page.on('pageerror', (e) => errors.push(e.message));
const now = Math.floor(Date.now() / 1000);
const runId = randomUUID();
const lead = leadId(runId);
const commands = [];
let state = { schema_version: 1, available: true, run: null, pending_commands: [] };
const shot = async (name) => {
  for (const [kind, width, height] of [
    ['desktop', 1440, 1050],
    ['mobile', 390, 844],
  ]) {
    await page.setViewportSize({ width, height });
    await page.screenshot({ path: `${output}/${name}-${kind}.png`, fullPage: true });
    if (await page.evaluate(() => document.documentElement.scrollWidth > innerWidth))
      throw new Error(`${name} horizontal overflow at ${kind} width`);
  }
  await page.setViewportSize({ width: 1440, height: 1050 });
};
const stamp = (seconds) => {
  const d = new Date(seconds * 1000);
  return new Date(d.getTime() - d.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
};
const projection = (key, document) => ({ key, revision: 1, updated_at: Date.now(), document });
try {
  await page.goto(target.href);
  await page.getByLabel('Access token').fill(token);
  await page.getByRole('button', { name: 'Open workspace' }).click();
  await page.getByRole('heading', { name: 'Experiments', exact: true }).waitFor();

  // The status advertises Autopilot; the fixture experiment stands in as a lead with the saved marker.
  await page.route('**/api/projections/status', async (route) => {
    const body = await (await route.fetch()).json();
    await route.fulfill({
      json: {
        ...body,
        document: { ...body.document, autopilot: { available: true, schema_version: 1 } },
      },
    });
  });
  const marked = (item) => ({
    ...item,
    contract: { ...item.contract, autopilot_lead: { run_id: runId } },
  });
  await page.route('**/api/projections?*prefix=experiment*', async (route) => {
    const body = await (await route.fetch()).json();
    const items = (body.items || body.projections || []).map((p) =>
      p.key === `experiment:${eid}` ? { ...p, document: marked(p.document) } : p,
    );
    await route.fulfill({
      json: { ...body, ...(body.items ? { items } : { projections: items }) },
    });
  });
  await page.route(`**/api/projections/experiment%3A${eid}`, async (route) => {
    const body = await (await route.fetch()).json();
    await route.fulfill({ json: { ...body, document: marked(body.document) } });
  });
  await page.route('**/api/projections/autopilot%3Atest-project', (route) =>
    route.fulfill({ json: projection('autopilot:test-project', state) }),
  );
  await page.route('**/api/blobs/*', (route) =>
    route.fulfill({ body: 'Synthetic saved note.', contentType: 'text/plain' }),
  );
  await page.route('**/api/commands', async (route) => {
    if (route.request().method() !== 'POST') return route.fallback();
    const { request_id, kind, target: to, payload } = route.request().postDataJSON();
    assert.equal(kind, 'autopilot');
    assert.equal(to, 'autopilot:test-project');
    commands.push(payload);
    if (payload.kind === 'configure') {
      const p = payload.policy;
      assert.equal(p.borrowing_percent, 5, 'borrowing keeps its saved default');
      const total = p.handoff_at - p.start_at;
      state = {
        ...state,
        observed_at: now,
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
          boundaries: { foundation: p.start_at + total * 0.01, writing: p.handoff_at },
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
        files: [],
        downloads: [],
        experiments: [],
        stage_holds: [],
        notices: [],
        backlog: [],
        messages: [],
        decisions: [],
        responses: [],
      };
    } else {
      assert.equal(payload.expected_revision, state.run.revision, 'stale expected revision');
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
    }
    await route.fulfill({
      json: {
        request_id,
        kind,
        target: to,
        state: 'applied',
        result: { state: 'applied' },
        error: null,
        received_at: now,
      },
    });
  });

  // The lead is marked in the list and on its own page.
  await page.reload();
  const row = page.locator('.experiment-row', { hasText: 'Autopilot lead' });
  await row.waitFor();
  await shot('list');
  await page.goto(new URL(`/?experiment=${eid}`, target).href);
  await page.locator('.detail-meta', { hasText: 'Autopilot lead' }).waitFor();
  await page.goto(target.href);

  await page.getByRole('button', { name: 'Project knowledge', exact: true }).click();
  const panel = page.locator('.autopilot');
  await panel.getByRole('button', { name: 'Set up Autopilot' }).click();
  assert.equal(await panel.getByLabel(/Borrowing/).count(), 0, 'borrowing input is gone');
  await panel.getByLabel('Research subject', { exact: true }).fill('Synthetic lead acceptance');
  await panel
    .getByLabel('Scope and central question')
    .fill('Test the lead presentation. No scientific research.');
  await panel.getByLabel('Publication goal').fill('Software acceptance only');
  await panel.getByLabel('Venue and year').fill('Synthetic venue, 2026');
  await panel.getByLabel('Researcher handoff', { exact: true }).fill(stamp(now + 10 * 604800));
  await panel.getByLabel('Submission deadline').fill(stamp(now + 11 * 604800));
  await shot('setup');
  await panel.getByRole('button', { name: 'Review policy', exact: true }).click();
  await panel.getByRole('button', { name: 'Save draft', exact: true }).click();
  await expect(panel.getByText('Saved policy')).toBeVisible();
  assert.equal(await panel.getByRole('button', { name: /lead experiment/ }).count(), 0);
  await panel.getByRole('button', { name: 'Review activation', exact: true }).click();
  await panel.getByRole('button', { name: 'Activate Autopilot', exact: true }).click();
  await panel.getByRole('button', { name: 'Pause Autopilot', exact: true }).click();
  await panel.getByRole('button', { name: 'Resume Autopilot', exact: true }).click();
  await panel.getByRole('button', { name: 'Pause Autopilot', exact: true }).waitFor();

  // A new run: no backlog, decisions, documents or files; members have empty backlog and rationale.
  state = {
    ...state,
    experiments: [
      { experiment_id: eid, phase: 'training', deferred: 0, backlog_id: '', rationale: '' },
    ],
  };
  await panel.getByLabel('Message to Autopilot').fill('Synthetic guidance for the lead.');
  await panel.getByRole('button', { name: 'Send to Autopilot', exact: true }).click();
  await panel.getByRole('heading', { name: 'Linked experiments' }).waitFor();
  assert.equal(
    await panel.getByText(/Initial literature and protocol/).count(),
    0,
    'no foundation gate text',
  );
  for (const heading of ['Background record', 'Saved documents and files'])
    assert.equal(await panel.getByRole('heading', { name: heading }).count(), 0, heading);
  await shot('new-run');

  // The lead link opens the derived experiment id.
  await panel.getByRole('button', { name: 'Open the Autopilot lead experiment' }).click();
  await page.waitForURL(new RegExp(lead.slice(0, 8)), { timeout: 3000 }).catch(() => {});
  await page.goto(target.href);
  await page.getByRole('button', { name: 'Project knowledge', exact: true }).click();

  // A retired run keeps its saved record, shown read-only, including sparse items.
  state = {
    ...state,
    observed_at: now - 300,
    run: {
      ...state.run,
      hold_reason: 'autopilot_researcher_input',
      central_argument: 'Synthetic.',
    },
    notices: [
      {
        id: randomUUID(),
        kind: 'researcher_input',
        text: 'Synthetic recovered question: select a fixture source.',
        resolved_at: null,
      },
    ],
    documents: [{ name: 'paper.tex', version: 1 }],
    downloads: [
      {
        blob_id: randomUUID(),
        kind: 'document',
        name: 'paper.tex',
        version: 1,
        size: 21,
        sha256: null,
        media_type: 'text/plain',
        available: true,
      },
    ],
    experiments: [
      {
        experiment_id: eid,
        phase: 'training',
        deferred: 0,
        backlog_id: 'q1',
        rationale: 'Saved reason',
      },
    ],
    backlog: [
      {
        id: 'q1',
        document: {
          question: 'Does the synthetic fixture preserve exact intents?',
          stop_condition: 'Stop after acceptance',
          review_at: now + 604800,
          priority: 1,
          status: 'active',
        },
      },
      { id: 'q2', document: {} },
    ],
    decisions: [{ id: 'd1', author: 'agent:lead', created_at: now, document: {} }],
    responses: [
      {
        id: 'r1',
        cursor: 1,
        result: JSON.stringify({ summary: 'Historical project turn.' }),
        created_at: now,
      },
    ],
  };
  await panel.getByLabel('Message to Autopilot').fill('Synthetic refresh.');
  await panel.getByRole('button', { name: 'Send to Autopilot', exact: true }).click();
  await expect(panel.locator('.autopilot-state')).toContainText('Waiting for your reply');
  await panel.getByText('Executor observation is old', { exact: false }).waitFor();
  await panel.getByText('Saved reason', { exact: false }).waitFor();
  await panel.getByRole('heading', { name: 'Saved documents and files' }).waitFor();
  await panel.getByText('Investigation backlog (2)').click();
  await panel.getByText('Research decisions (1)').click();
  await panel.getByText(/Earlier guidance/).click();
  await panel.getByText('Historical project turn.').waitFor();
  await shot('saved-record');
  await panel.getByLabel('Your answer').fill('Use only the synthetic fixture source.');
  await panel.getByRole('button', { name: 'Send answer', exact: true }).click();

  if (errors.length) throw new Error(errors.join('\n'));
  for (const kind of ['configure', 'activate', 'pause', 'resume', 'message', 'answer'])
    assert.ok(
      commands.some((c) => c.kind === kind),
      `missing ${kind} command`,
    );
  await writeFile(
    path.join(output, 'evidence.json'),
    JSON.stringify(
      {
        synthetic: true,
        experiment_id: eid,
        lead_id: lead,
        commands: commands.map((c) => c.kind),
        scope:
          'Real local demo authentication and synthetic experiment; mocked Autopilot projection and commands; no live Slurm, managed runtime or phone acceptance.',
      },
      null,
      2,
    ),
  );
  console.log(JSON.stringify({ synthetic: true, experiment_id: eid, screenshots: output }));
} finally {
  await browser.close();
}
