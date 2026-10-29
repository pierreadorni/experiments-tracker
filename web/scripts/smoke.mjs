import { chromium } from '@playwright/test';
import { readFile, mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { unzipSync } from 'fflate';
import { spawnSync } from 'node:child_process';

const root = path.resolve(import.meta.dirname, '../..');
const target = new URL(process.env.ET_UI_URL || 'http://127.0.0.1:8765');
if (
  target.protocol !== 'http:' ||
  !['127.0.0.1', 'localhost', '[::1]'].includes(target.hostname) ||
  target.username ||
  target.password
)
  throw new Error('Browser acceptance requires a loopback HTTP demo API');
const browser = await chromium.launch({ headless: true, args: ['--no-sandbox'] });
const page = await browser.newPage({ viewport: { width: 1440, height: 1050 } });
const errors = [];
page.on('pageerror', (error) => errors.push(error.message));
try {
  // Mock the proposed registry only. This does not claim native ChatGPT WebMCP support.
  await page.addInitScript(() => {
    window.__etTools = {};
    Object.defineProperty(document, 'modelContext', {
      value: {
        registerTool(tool, { signal }) {
          window.__etTools[tool.name] = tool;
          signal.addEventListener('abort', () => delete window.__etTools[tool.name]);
        },
      },
    });
  });
  await page.goto(target.href);
  await page
    .getByLabel('Access token')
    .fill((await readFile(path.join(root, '.local/api-token'), 'utf8')).trim());
  await page.getByRole('button', { name: 'Open workspace' }).click();
  await page.getByRole('heading', { name: 'Experiments', exact: true }).waitFor();
  await page.getByLabel('Search experiments', { exact: true }).fill('no-match-acceptance-uuid');
  if (await page.locator('.experiment-row').count())
    throw new Error('Unexpected search result for synthetic missing query');
  await page.getByLabel('Search experiments', { exact: true }).fill('');
  const status = await page.request.get(new URL('/api/status', target).href);
  if (!status.ok() || (await status.json()).mode !== 'demo')
    throw new Error('Browser acceptance refuses to create experiments outside demo mode');
  if (process.env.ET_TEST_PROJECT_ID) {
    const catalog = (await status.json()).projects;
    const project = catalog.find((p) => p.id === process.env.ET_TEST_PROJECT_ID);
    if (!project || project.id === 'test-project')
      throw new Error('Missing synthetic second project');
    await page.getByRole('button', { name: 'Project', exact: true }).click();
    await page.getByRole('option', { name: project.name }).click();
    await page.getByRole('heading', { name: 'No experiments yet.' }).waitFor();
    await page
      .getByRole('main')
      .getByRole('button', { name: 'New experiment', exact: true })
      .click();
    if ((await page.getByLabel('Research brief').inputValue()) !== '')
      throw new Error('Project inherited the test brief');
    await page
      .getByLabel('Research brief')
      .fill('An unsent brief for the synthetic second project.');
    await page.keyboard.press('Escape');
    await page.reload();
    await page.getByRole('heading', { name: 'No experiments yet.' }).waitFor();
    if (new URL(page.url()).searchParams.get('project') !== project.id)
      throw new Error('Reload lost project selection');
    await page.setViewportSize({ width: 390, height: 844 });
    await page.getByRole('button', { name: 'Open navigation' }).click();
    await page.getByRole('button', { name: 'Project', exact: true }).click();
    await page.getByRole('option', { name: 'Test project', exact: true }).click();
    await page.getByRole('button', { name: 'Open navigation' }).waitFor();
    await page.setViewportSize({ width: 1440, height: 1050 });
    await page
      .getByRole('main')
      .getByRole('button', { name: 'New experiment', exact: true })
      .click();
    if (
      !(await page.getByLabel('Research brief').inputValue()).startsWith('Compare a linear model')
    )
      throw new Error('Demo brief was not restored');
    await page.keyboard.press('Escape');
    if (await page.evaluate(() => document.documentElement.scrollWidth > innerWidth))
      throw new Error('Project selector overflow');
  }
  await page.waitForFunction(() => Object.keys(window.__etTools).length === 3);
  const toolsBefore = await page.evaluate(async () => ({
    names: Object.keys(window.__etTools),
    list: await window.__etTools.list_experiments.execute({}),
  }));
  if (!toolsBefore.names.includes('start_experiment_creation'))
    throw new Error('Brief staging tool missing');
  await page.evaluate(() =>
    window.__etTools.start_experiment_creation.execute({
      brief: 'Compare the fixed models; this is staged by a mocked registry.',
    }),
  );
  await page.getByRole('dialog').waitFor();
  await page.keyboard.press('Escape');
  const toolsAfter = await page.evaluate(async () => {
    try {
      await window.__etTools.start_experiment_creation.execute({ brief: 'x' });
      throw new Error('Accepted invalid brief');
    } catch (error) {
      if (error.message === 'Accepted invalid brief') throw error;
    }
    return window.__etTools.list_experiments.execute({});
  });
  if (toolsBefore.list.length !== toolsAfter.length)
    throw new Error('Staging launched an experiment');
  if (toolsAfter.length) {
    await page.evaluate(
      (id) => window.__etTools.open_experiment.execute({ experiment_id: id }),
      toolsAfter[0].id,
    );
    await page.getByRole('button', { name: 'All experiments' }).waitFor();
    await page.getByRole('button', { name: 'All experiments' }).click();
  }
  await page.getByRole('main').getByRole('button', { name: 'New experiment', exact: true }).click();
  await page.getByText('Execution options', { exact: true }).click();
  if (toolsAfter.length) {
    await page
      .getByRole('button', { name: 'Reference experiment (optional)', exact: true })
      .click();
    const reference = await (
      await page.request.get(new URL(`/api/experiments/${toolsAfter[0].id}`, target).href)
    ).json();
    await page
      .getByRole('option', { name: reference.analysis?.title || reference.brief, exact: true })
      .first()
      .click();
  }
  await page.getByRole('button', { name: 'Codex model', exact: true }).click();
  await page.getByRole('option').filter({ hasText: 'Deterministic' }).click();
  await page.getByRole('button', { name: 'Reasoning effort', exact: true }).click();
  await page.getByRole('option', { name: 'Medium', exact: true }).click();
  await page.getByRole('button', { name: 'Prepare and run experiment' }).click();
  await page.getByRole('button', { name: 'All experiments' }).waitFor();
  await page.locator('.detail-meta .badge.success').waitFor({ timeout: 30000 });
  const tabs = await page.getByRole('tab').allTextContents();
  if (
    JSON.stringify(tabs) !== JSON.stringify(['Progress', 'Report', 'Notes', 'Protocol', 'Evidence'])
  )
    throw new Error('Unexpected detail tabs');
  const firstStage = page.locator('.stage-toggle').first();
  if ((await firstStage.getAttribute('aria-expanded')) !== 'false')
    throw new Error('Completed stage not collapsed');
  await firstStage.click();
  if ((await firstStage.getAttribute('aria-expanded')) !== 'true')
    throw new Error('Stage cannot expand');
  await firstStage.click();
  const selected = await page.evaluate(
    async () => (await (await fetch('/api/experiments')).json()).experiments[0],
  );
  if (
    selected.contract.agent.model !== 'deterministic-demo' ||
    selected.contract.agent.reasoning_effort !== 'medium'
  )
    throw new Error('Model selection did not persist');

  if (process.env.ET_TEST_PROJECT_ID) {
    if (selected.project_id !== 'test-project') throw new Error('Creation lost project identity');
    const project = (await status.json()).projects.find(
      (p) => p.id === process.env.ET_TEST_PROJECT_ID,
    );
    await page.getByRole('button', { name: 'Project', exact: true }).click();
    await page.getByRole('option', { name: project.name }).click();
    await page.getByRole('heading', { name: 'No experiments yet.' }).waitFor();
    const scoped = await page.evaluate(() => window.__etTools.list_experiments.execute({}));
    if (scoped.length) throw new Error('Workspace tool leaked another project');
    await page.getByRole('button', { name: /^Inbox/ }).click();
    await page.getByRole('heading', { name: 'Nothing needs your attention.' }).waitFor();
    await page.getByRole('button', { name: 'New experiment', exact: true }).click();
    await page.getByText('Execution options', { exact: true }).click();
    await page
      .getByRole('button', { name: 'Reference experiment (optional)', exact: true })
      .click();
    if ((await page.getByRole('option').count()) !== 1)
      throw new Error('Cross-project reference offered');
    await page.keyboard.press('Escape');
    await page.keyboard.press('Escape');
    await page.getByRole('button', { name: 'Project', exact: true }).click();
    await page.getByRole('option', { name: 'Test project', exact: true }).click();
    await page.evaluate(
      (id) => window.__etTools.open_experiment.execute({ experiment_id: id }),
      selected.id,
    );
    await page.getByRole('button', { name: 'All experiments' }).waitFor();
  }
  await page.getByLabel('Message Codex').fill('What does this result establish?');
  await page.getByRole('button', { name: 'Send message', exact: true }).click();
  await page.waitForFunction(
    async (id) =>
      (await (await fetch(`/api/experiments/${id}`)).json()).conversation?.some(
        (m) => m.state === 'completed' && m.reply,
      ),
    selected.id,
  );
  await page.locator('.discussion-reply').waitFor();
  await page.getByRole('button', { name: 'Close discussion', exact: true }).click();
  await page.getByRole('button', { name: 'Discuss results', exact: true }).click();
  await page.locator('.discussion-reply').waitFor();

  await page.getByRole('tab', { name: 'Notes', exact: true }).click();
  const text = 'Browser acceptance note: <script>alert("unsafe")</script> remains plain text.';
  await page.getByLabel('Add a note').fill(text);
  await page.getByRole('button', { name: 'Save note' }).click();
  await page.getByText(text, { exact: true }).waitFor();
  const figure = selected.artifacts.find((a) => a.name === 'predictions.svg');
  const revision = await page.evaluate(
    async ({ id, figureId }) => {
      const response = await fetch(`/api/experiments/${id}/reports`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          schema_version: 1,
          request_id: crypto.randomUUID(),
          document: {
            title: 'A readable synthetic comparison',
            standfirst: 'Same measurements, presented concisely.',
            blocks: [
              { kind: 'heading', text: 'Measured results' },
              {
                kind: 'table',
                columns: ['Model', 'MSE'],
                rows: [
                  [{ text: 'Linear' }, { metric: { name: 'mse', variant: 'linear' } }],
                  [{ text: 'Quadratic' }, { metric: { name: 'mse', variant: 'quadratic' } }],
                ],
                caption: 'Held-out MSE on the shared synthetic test split.',
              },
              {
                kind: 'figure',
                artifact_id: figureId,
                alt: 'Predictions on the shared test split.',
                caption: 'The registered prediction figure.',
              },
              {
                kind: 'paragraph',
                text: 'This is a presentation revision of saved measurements, not a new experiment.',
              },
            ],
          },
        }),
      });
      if (!response.ok) throw new Error('Report revision command failed');
      return response.json();
    },
    { id: selected.id, figureId: figure.id },
  );
  await page.waitForFunction(
    async (id) =>
      (await (await fetch(`/api/experiments/${id}`)).json()).report_versions?.some(
        (r) => r.version === 2 && r.state === 'ready',
      ),
    selected.id,
  );
  await page.getByRole('tab', { name: 'Report', exact: true }).click();
  const frame = page.frameLocator('iframe');
  await page.getByLabel('Report version').waitFor();
  await frame
    .getByRole('heading', { name: 'A readable synthetic comparison', exact: true })
    .waitFor();
  await page.getByRole('button', { name: 'Report version', exact: true }).click();
  await page.getByRole('option', { name: 'Original · version 1', exact: true }).click();
  await frame.getByRole('heading', { name: selected.analysis.title, exact: true }).waitFor();
  if ((await frame.locator('h1').textContent()) === 'A readable synthetic comparison')
    throw new Error('Original report selection did not apply');
  await page.getByRole('button', { name: 'Report version', exact: true }).click();
  await page.getByRole('option', { name: 'Version 2', exact: true }).click();

  await frame.getByRole('heading', { name: 'Measured results' }).waitFor();
  await frame.locator('img').evaluate(
    (img) =>
      new Promise((resolve, reject) => {
        if (img.complete && img.naturalWidth > 0) return resolve(true);
        img.onload = () => resolve(true);
        img.onerror = () => reject(new Error('report figure failed'));
      }),
  );
  await page.getByRole('tab', { name: 'Evidence', exact: true }).click();
  const downloaded = page.waitForEvent('download');
  await page.getByRole('button', { name: 'Download evidence bundle' }).click();
  const archive = await downloaded;
  const bundlePath = path.join(root, '.local/browser-evidence.zip');
  await archive.saveAs(bundlePath);
  const files = unzipSync(await readFile(bundlePath));
  const offlineRoot = path.join(root, '.local/offline-report');
  for (const [name, bytes] of Object.entries(files)) {
    if (name.includes('..') || name.startsWith('/')) throw new Error('Unsafe bundle filename');
    const target = path.join(offlineRoot, name);
    await mkdir(path.dirname(target), { recursive: true });
    await writeFile(target, bytes);
  }
  const offlinePage = await browser.newPage();
  await offlinePage.route(/^https?:/, (route) => route.abort());
  await offlinePage.goto(pathToFileURL(path.join(offlineRoot, 'report.html')).href);
  await offlinePage.getByRole('heading', { name: 'Measured results' }).waitFor();
  if (!(await offlinePage.locator('img').evaluate((img) => img.complete && img.naturalWidth > 0)))
    throw new Error('Offline report figure failed');
  await offlinePage.close();
  if (!files['evidence/recipe.py'] || !files['evidence/manifest.json'])
    throw new Error('Frozen source missing from new export');
  for (const stage of ['train_linear', 'train_quadratic', 'evaluate_and_plot']) {
    const run = spawnSync(
      '/usr/bin/python3',
      [
        path.join(offlineRoot, 'evidence/recipe.py'),
        stage,
        '--output',
        path.join(offlineRoot, 'reproduction'),
      ],
      { encoding: 'utf8', timeout: 20000 },
    );
    if (run.status !== 0) throw new Error(`Exported recipe reproduction failed: ${run.stderr}`);
  }
  const originalMetrics = JSON.parse(new TextDecoder().decode(files['evidence/results.json']));
  const reproducedMetrics = JSON.parse(
    await readFile(path.join(offlineRoot, 'reproduction/results.json'), 'utf8'),
  );
  if (JSON.stringify(originalMetrics) !== JSON.stringify(reproducedMetrics))
    throw new Error('Exported CPU recipe metrics differ');
  await page.getByRole('button', { name: 'All experiments' }).click();
  await page.getByRole('button', { name: 'Project knowledge' }).click();
  await page.getByRole('heading', { name: 'Project knowledge', exact: true }).waitFor();
  await page.getByRole('heading', { name: /^Saved next ideas/ }).waitFor();
  const idea = page.locator('.next-idea').first();
  if (await idea.count()) {
    await idea.getByRole('button', { name: 'Use as brief', exact: true }).click();
    if (!(await page.getByLabel('Research brief').inputValue()).trim())
      throw new Error('Saved idea did not prefill the brief');
    await page.keyboard.press('Escape');
  }
  await page.setViewportSize({ width: 390, height: 844 });
  if (await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth))
    throw new Error('Project knowledge overflows mobile');
  await page.getByRole('button', { name: 'Open navigation', exact: true }).click();
  await page.getByRole('button', { name: /^Experiments/ }).click();
  await page.setViewportSize({ width: 1440, height: 1050 });
  await mkdir(path.join(root, '.local/screenshots'), { recursive: true });
  await page.screenshot({
    path: path.join(root, '.local/screenshots/desktop.png'),
    fullPage: true,
  });
  await page.setViewportSize({ width: 390, height: 844 });
  await page.screenshot({ path: path.join(root, '.local/screenshots/mobile.png'), fullPage: true });
  if (await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth))
    throw new Error('horizontal overflow on mobile');
  if (errors.length) throw new Error(errors.join('\n'));
  const data = await page.evaluate(async () => ({
    experiments: await (await fetch('/api/experiments')).json(),
    inbox: await (await fetch('/api/inbox')).json(),
  }));
  const latest = data.experiments.experiments[0];
  // Only delete our completed synthetic acceptance fixture, never a historical/live result.
  await page.evaluate(
    (id) => window.__etTools.open_experiment.execute({ experiment_id: id }),
    latest.id,
  );
  if (
    (await page.getByRole('tab', { name: 'Report', exact: true }).getAttribute('aria-selected')) !==
    'true'
  )
    throw new Error('Completed experiments must open on Report');
  if (await page.getByRole('dialog', { name: 'Discuss with Codex' }).isVisible())
    await page.getByRole('button', { name: 'Close discussion', exact: true }).last().click();
  await page.getByRole('button', { name: 'Experiment actions', exact: true }).click();
  await page.getByRole('button', { name: 'Delete experiment', exact: true }).click();
  const deletion = page.getByRole('dialog', { name: 'Delete experiment?', exact: true });
  await deletion.waitFor();
  if (
    !(await deletion
      .getByRole('button', { name: 'Cancel', exact: true })
      .evaluate((button) => button === document.activeElement))
  )
    throw new Error('Deletion confirmation must focus Cancel');
  await deletion.getByRole('button', { name: 'Cancel', exact: true }).click();
  await page.getByRole('button', { name: 'Experiment actions', exact: true }).click();
  await page.getByRole('button', { name: 'Delete experiment', exact: true }).click();
  await deletion.getByRole('button', { name: 'Delete experiment', exact: true }).click();
  await page.getByRole('heading', { name: 'Experiments', exact: true }).waitFor();
  const afterDelete = await page.evaluate(async () => ({
    experiments: (await (await fetch('/api/experiments')).json()).experiments,
    inbox: (await (await fetch('/api/inbox')).json()).entries,
  }));
  if (
    afterDelete.experiments.some((e) => e.id === latest.id) ||
    afterDelete.inbox.some((e) => e.experiment_id === latest.id)
  )
    throw new Error('Deleted fixture still appears in the workspace');

  const summary = {
    passed: true,
    mode: 'demo',
    experiment_id: latest.id,
    reference_experiment_id: latest.reference_experiment_id || null,
    jobs: latest.jobs.length,
    agent_actions: latest.agent_actions.length,
    stages: latest.stages.map((s) => [s.id, s.state]),
    result_inbox_entries: data.inbox.entries.filter(
      (n) => n.experiment_id === latest.id && n.kind === 'result_ready',
    ).length,
    checks: [
      ...(process.env.ET_TEST_PROJECT_ID
        ? [
            'project switching, blank brief, refresh and phone navigation',
            'project-scoped experiment/inbox/tool lists and reference choices',
            'project identity persisted on creation',
          ]
        : []),
      'token login',
      'saved conversation, deterministic reply and collapsible panel',
      'versioned report command, original selector and current offline export',
      'model and medium reasoning persisted',
      'five ordered tabs and stage expansion/collapse',
      'synthetic fixture deletion removes list and inbox entries',
      'brief submission',
      'automatic completion',
      'escaped note',
      'report and figure fetch',
      'desktop render',
      '390px mobile without overflow',
      'verified evidence ZIP download',
      'extracted offline report and figure with HTTP disabled',
      'project knowledge and saved idea brief prefill',
      'explicit reference lineage through completion',
      'exported frozen recipe reproduces exact metrics with local Python only',
      'mocked WebMCP registration, reads, navigation, safe staging and invalid input',
    ],
    errors,
  };
  await writeFile(path.join(root, '.local/ui-acceptance.json'), JSON.stringify(summary, null, 2));
  console.log(JSON.stringify(summary));
} finally {
  await browser.close();
}
