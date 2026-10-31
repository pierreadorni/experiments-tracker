import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  stageSummary,
  stageJob,
  jobTiming,
  observationTime,
  stageTitle,
  stageIndicator,
  timelineStages,
  jobProgress,
  experimentStatus,
} from '../src/progress.ts';
const stage = { id: 'fit', state: 'running' };
const job = {
  stage_id: 'fit',
  state: 'submitted',
  scheduler_state: 'PENDING',
  created_at: 100,
  resources: { walltime_minutes_per_job: 5 },
  intent_id: 'intent',
  job_id: '42',
  observed_at: 110,
};
const exp = {
  stages: [stage],
  jobs: [job],
  agent_actions: [],
  artifacts: [],
  snapshot: { plan: { stages: [{ id: 'fit', title: 'Fit requested method' }] } },
};
test('successful scheduler accounting shows output checks until evidence is committed', () => {
  const checking = { ...job, scheduler_state: 'COMPLETED', exit_code: '0:0' };
  const base = { ...exp, phase: 'training', hold_reason: null, jobs: [checking] };
  assert.equal(experimentStatus(base), 'checking job outputs');
  assert.equal(stageSummary(base, stage), 'Checking job outputs');
  assert.equal(stageIndicator(base, stage), 'waiting');
  assert.equal(
    experimentStatus({ ...base, jobs: [{ ...checking, exit_code: null }] }),
    'waiting for scheduler accounting',
  );
  assert.equal(
    stageSummary({ ...base, jobs: [{ ...checking, exit_code: null }] }, stage),
    'Waiting for scheduler accounting',
  );
});
test('experiment badges distinguish queued compute from running and respect terminal/hold state', () => {
  const base = { ...exp, phase: 'training', hold_reason: null };
  assert.equal(experimentStatus(base), 'queued');
  for (const phase of ['training', 'evaluating']) {
    assert.equal(experimentStatus({ ...base, phase }), 'queued');
    assert.equal(
      experimentStatus({ ...base, phase, jobs: [{ ...job, scheduler_state: 'RUNNING' }] }),
      phase,
    );
  }
  for (const [scheduler_state, expected] of [
    ['CONFIGURING', 'queued'],
    [null, 'waiting for scheduler'],
    ['ACCOUNTING_PENDING', 'waiting for scheduler accounting'],
    ['COMPLETING', 'finishing'],
    ['SUSPENDED', 'scheduler: suspended'],
  ])
    assert.equal(experimentStatus({ ...base, jobs: [{ ...job, scheduler_state }] }), expected);
  assert.equal(
    experimentStatus({ ...base, jobs: [{ ...job, state: 'submitting' }] }),
    'submitting',
  );
  assert.equal(experimentStatus({ ...base, hold_reason: 'quota_hold' }), 'quota hold');
  assert.equal(
    experimentStatus({ ...base, phase: 'failed', hold_reason: 'compute_failed' }),
    'failed',
  );
  assert.equal(experimentStatus({ ...base, phase: 'complete' }), 'complete');
  assert.equal(experimentStatus({ ...base, phase: 'preparing', jobs: [] }), 'preparing');
  assert.equal(experimentStatus({ ...base, jobs: [{ ...job, state: 'complete' }, job] }), 'queued');
  assert.equal(
    experimentStatus({
      ...base,
      jobs: [job, { ...job, stage_id: 'other', scheduler_state: 'RUNNING' }],
    }),
    'training',
  );
});
test('work progress and ETA retain freshness and never infer success', () => {
  const running = {
    ...job,
    scheduler_state: 'RUNNING',
    progress: { completed: 25, total: 100, reported_at: 160, eta_seconds: 300 },
  };
  assert.equal(jobProgress(job, 160), null);
  assert.deepEqual(jobProgress(running, 180), {
    percent: 25,
    stale: false,
    eta: 'ETA ≈ 4m 40s remaining',
  });
  assert.equal(jobProgress(running, 281).eta, 'ETA ≈ up to 5m 0s remaining · last report');
  assert.equal(jobProgress(running, 281).percent, 25);
  assert.match(jobProgress({ ...running, scheduler_state: 'PENDING' }, 180).eta, /not running/);
  assert.equal(
    jobProgress({ ...running, progress: { ...running.progress, eta_seconds: null } }, 180).eta,
    'ETA unavailable',
  );
  assert.match(
    jobProgress({ ...running, progress: { ...running.progress, eta_seconds: 10 } }, 180).eta,
    /updating/,
  );
  assert.match(
    jobProgress({ ...running, progress: { ...running.progress, completed: 100 } }, 180).eta,
    /checking outputs/,
  );
  assert.equal(jobProgress({ ...running, state: 'complete' }, 999).eta, 'Outputs checked');
  for (const state of ['complete', 'failed', 'cancelled'])
    assert.equal(jobProgress({ ...running, state }, 999).stale, false);
  assert.equal(jobProgress(running, 999).stale, true);
  assert.equal(jobProgress({ ...running, state: 'failed' }, 180).eta, 'Stage stopped');
  assert.equal(jobProgress({ ...running, progress: { ...running.progress, total: 0 } }, 180), null);
});
test('missing ETA falls back to measured progress and scheduler runtime at the report', () => {
  const running = {
    ...job,
    started_at: 100,
    scheduler_state: 'RUNNING',
    progress: { completed: 25, total: 100, reported_at: 160, eta_seconds: null },
  };
  // 60s for 25% gives 180s remaining at the report, minus its 20s age.
  assert.equal(jobProgress(running, 180).eta, 'ETA ≈ 2m 40s remaining · based on progress');
  assert.equal(jobProgress({ ...running, created_at: 1 }, 180).eta, jobProgress(running, 180).eta);
  assert.equal(
    jobProgress({ ...running, progress: { ...running.progress, eta_seconds: 300 } }, 180).eta,
    'ETA ≈ 4m 40s remaining',
  );
  assert.equal(
    jobProgress({ ...running, progress: { ...running.progress, eta_seconds: 0 } }, 180).eta,
    'ETA updating…',
  );
  assert.equal(
    jobProgress(running, 281).eta,
    'ETA ≈ up to 3m 0s remaining · last report · based on progress',
  );
  assert.match(jobProgress({ ...running, scheduler_state: 'PENDING' }, 180).eta, /not running/);
  assert.equal(jobProgress({ ...running, state: 'complete' }, 180).eta, 'Outputs checked');
  assert.equal(jobProgress({ ...running, state: 'failed' }, 180).eta, 'Stage stopped');
  for (const started_at of [undefined, null, 0, -1, 160, 170, NaN, Infinity])
    assert.equal(jobProgress({ ...running, started_at }, 180).eta, 'ETA unavailable');
  assert.equal(
    jobProgress({ ...running, progress: { ...running.progress, completed: 0 } }, 180).eta,
    'ETA unavailable',
  );
  assert.equal(
    jobProgress({ ...running, progress: { ...running.progress, completed: 100 } }, 180).eta,
    'Finishing and checking outputs',
  );
  assert.equal(
    jobProgress({ ...running, progress: { ...running.progress, completed: 99 } }, 180).eta,
    'ETA updating…',
  );
  assert.equal(
    jobProgress({ ...running, progress: { ...running.progress, completed: Number.MIN_VALUE } }, 180)
      .eta,
    'ETA unavailable',
  );
  assert.equal(jobProgress(running, 300, 600).eta, 'ETA ≈ 40s remaining · based on progress');
});
test('stale estimates retain the report upper estimate without implying new work or success', () => {
  const running = {
    ...job,
    started_at: 100,
    scheduler_state: 'RUNNING',
    progress: { completed: 25, total: 100, reported_at: 160, eta_seconds: 300 },
  };
  for (const now of [281, 1000, 86400]) {
    assert.deepEqual(jobProgress(running, now), {
      percent: 25,
      stale: true,
      eta: 'ETA ≈ up to 5m 0s remaining · last report',
    });
    assert.equal(
      jobProgress({ ...running, progress: { ...running.progress, eta_seconds: null } }, now).eta,
      'ETA ≈ up to 3m 0s remaining · last report · based on progress',
    );
  }
  assert.equal(jobProgress(running, 1360, 600).stale, false);
  assert.match(jobProgress(running, 1361, 600).eta, /up to 5m 0s/);
  for (const scheduler_state of ['PENDING', 'COMPLETING', 'COMPLETED', 'FAILED', 'CANCELLED'])
    assert.match(jobProgress({ ...running, scheduler_state }, 1000).eta, /not running/);
  assert.equal(
    jobProgress({ ...running, progress: { ...running.progress, completed: 100 } }, 1000).eta,
    'Finishing and checking outputs',
  );
  const noEstimate = { ...running, started_at: null };
  for (const eta_seconds of [null, 0, -1, NaN, Infinity])
    assert.equal(
      jobProgress({ ...noEstimate, progress: { ...running.progress, eta_seconds } }, 1000).eta,
      'ETA unavailable · last report has no usable estimate',
    );
});
test('reviewed stages appear before implementation and follow plan revisions', () => {
  const base = ['plan', 'prepare', 'validate', 'interpret', 'publish'].map((id) => ({
    id,
    state: id === 'plan' ? 'complete' : 'pending',
    error: null,
    updated_at: 100,
  }));
  const review = {
    ...exp,
    snapshot: null,
    stages: base,
    jobs: [],
    updated_at: 100,
    plan_review: {
      state: 'awaiting_review',
      revision: 1,
      stages: [
        { id: 'fit', title: 'Train requested baseline', summary: 'Fit on the training split' },
      ],
    },
  };
  const stages = timelineStages(review);
  assert.deepEqual(
    stages.map((s) => s.id),
    ['plan', 'prepare', 'fit', 'validate', 'interpret', 'publish'],
  );
  assert.equal(stages[2].state, 'pending');
  assert.equal(stageTitle(review, stages[2]), 'Train requested baseline');
  assert.equal(stageSummary(review, stages[2]), 'Fit on the training split');
  assert.equal(stageIndicator(review, stages[2]), 'pending');
  assert.deepEqual(
    timelineStages({ ...review, plan_review: { ...review.plan_review, state: 'approved' } }),
    stages,
  );
  const revised = {
    ...review,
    plan_review: {
      ...review.plan_review,
      revision: 2,
      stages: [{ id: 'evaluate', title: 'Evaluate revised method' }],
    },
  };
  assert.deepEqual(
    timelineStages(revised).map((s) => s.id),
    ['plan', 'prepare', 'evaluate', 'validate', 'interpret', 'publish'],
  );
  const frozen = { ...review, snapshot: exp.snapshot, stages: [stage] };
  assert.equal(timelineStages(frozen), frozen.stages);
  assert.equal(stageTitle(frozen, stage), 'Fit requested method');
  assert.equal(timelineStages({ ...review, plan_review: null }), base);
});
test('graph stages show the dependencies that are still waiting', () => {
  const graph = {
    ...exp,
    snapshot: {
      plan: {
        schema_version: 2,
        stages: [
          { id: 'features', title: 'Features', needs: [] },
          { id: 'train_a', title: 'Train A', needs: ['features'] },
        ],
      },
    },
    stages: [
      { id: 'features', state: 'running' },
      { id: 'train_a', state: 'pending' },
    ],
    jobs: [],
  };
  assert.match(stageSummary(graph, graph.stages[1]), /Waiting for checked outputs from features/);
  assert.match(
    stageSummary(
      { ...graph, stages: [{ id: 'features', state: 'complete' }, graph.stages[1]] },
      graph.stages[1],
    ),
    /Ready to queue/,
  );
});
test('progress distinguishes queued, running, accounting and checked results', () => {
  assert.equal(stageTitle(exp, stage), 'Fit requested method');
  assert.match(stageSummary(exp, stage), /Waiting.*start/);
  assert.match(
    stageSummary({ ...exp, jobs: [{ ...job, scheduler_state: 'RUNNING' }] }, stage),
    /running/,
  );
  assert.match(
    stageSummary({ ...exp, jobs: [{ ...job, scheduler_state: 'ACCOUNTING_PENDING' }] }, stage),
    /accounting/,
  );
  assert.equal(
    stageSummary(
      { ...exp, jobs: [{ ...job, state: 'complete' }] },
      { ...stage, state: 'complete' },
    ),
    'Completed and checked',
  );
});
test('timing uses scheduler estimates and actual timestamps without inventing ETA', () => {
  assert.match(jobTiming(job, 160), /Queued for 1m 0s.*unavailable/);
  assert.match(jobTiming({ ...job, estimated_start_at: 200 }, 160), /Estimated start/);
  assert.match(jobTiming({ ...job, estimated_start_at: 150 }, 160), /unavailable/);
  assert.match(jobTiming({ ...job, started_at: 120 }, 160), /Running for 40s.*limit 5 min/);
  assert.equal(jobTiming({ ...job, started_at: 120, ended_at: 180 }, 300), 'Ran for 1m 0s');
  assert.equal(
    observationTime(job, { job_observations: { intent: { job_id: 'other', observed_at: 999 } } }),
    110,
  );
  assert.equal(
    observationTime(job, { job_observations: { intent: { job_id: '42', observed_at: 999 } } }),
    999,
  );
});

test('animated progress follows actual execution through holds and accounting', () => {
  assert.equal(stageIndicator(exp, stage), 'waiting');
  assert.equal(
    stageIndicator(
      { ...exp, jobs: [{ ...job, scheduler_state: 'RUNNING' }], hold_reason: 'quota_hold' },
      stage,
    ),
    'running',
  );
  assert.equal(
    stageIndicator({ ...exp, jobs: [{ ...job, scheduler_state: 'ACCOUNTING_PENDING' }] }, stage),
    'waiting',
  );
  assert.equal(stageIndicator({ ...exp, jobs: [], hold_reason: 'agent_paused' }, stage), 'waiting');
  assert.equal(
    stageIndicator(
      { ...exp, jobs: [], agent_actions: [{ phase: 'fit', state: 'uncertain' }] },
      stage,
    ),
    'waiting',
  );
  assert.equal(stageIndicator({ ...exp, jobs: [] }, { ...stage, state: 'pending' }), 'pending');
  assert.equal(stageIndicator(exp, { ...stage, state: 'complete' }), 'complete');
  assert.equal(stageIndicator(exp, { ...stage, state: 'cancelled' }), 'cancelled');
  assert.equal(stageSummary(exp, { ...stage, state: 'cancelled' }), 'Stage cancelled');
});

test('Jean Zay progress stays usable between ten-minute downloads and still expires', () => {
  const sample = {
    ...job,
    scheduler_state: 'RUNNING',
    progress: { completed: 5, total: 10, reported_at: 1000, eta_seconds: 3600 },
  };
  assert.equal(jobProgress(sample, 1500).stale, true);
  assert.equal(jobProgress(sample, 1500, 600).stale, false);
  assert.match(jobProgress(sample, 1500, 600).eta, /ETA ≈/);
  assert.equal(jobProgress(sample, 2300, 600).stale, true);
});

test('repair progress retains prior completion without claiming reuse before validation', () => {
  const prepare = { id: 'prepare', state: 'pending' };
  const fit = { id: 'fit', state: 'pending' };
  const base = {
    ...exp,
    phase: 'preparing',
    hold_reason: null,
    snapshot: null,
    contract: { repair_count: 2, auto_repair: { completed_stages: ['plan', 'prepare', 'fit'] } },
    stages: [prepare, fit],
    jobs: [],
    agent_actions: [{ phase: 'prepare', state: 'running' }],
    plan_review: { state: 'approved', stages: [{ id: 'fit', title: 'Fit', summary: 'Train' }] },
  };
  assert.equal(stageTitle(base, prepare), 'Repair experiment');
  assert.equal(experimentStatus(base), 'repairing');
  assert.equal(stageIndicator(base, prepare), 'running');
  assert.equal(stageIndicator(base, fit), 'waiting');
  assert.match(stageSummary(base, fit), /Completed in the previous run/);
  assert.equal(timelineStages(base).find((s) => s.id === 'fit').state, 'pending');
  assert.equal(stageIndicator({ ...base, snapshot: { reused_stages: {} } }, fit), 'pending');
  const reused = { ...base, snapshot: { reused_stages: { fit: { intent_id: 'old' } } } };
  assert.match(stageSummary(reused, { ...fit, state: 'complete' }), /reused here/);
  assert.equal(experimentStatus({ ...base, hold_reason: 'quota_hold' }), 'quota hold');
  assert.equal(stageTitle({ ...base, contract: {} }, prepare), 'Implement experiment');
});

test('resource adjustments and repair failures retain their actual status', () => {
  const prepare = { id: 'prepare', state: 'failed' };
  const base = {
    ...exp,
    phase: 'failed',
    hold_reason: 'agent_failed',
    snapshot: null,
    contract: { auto_repair: { kind: 'resources', completed_stages: [] } },
  };
  assert.equal(stageTitle(base, prepare), 'Adjust compute resources');
  assert.equal(stageIndicator(base, prepare), 'failed');
  assert.equal(experimentStatus(base), 'failed');
});

test('iteration dependencies describe input availability without a validation barrier', () => {
  const stage = { id: 'plot', state: 'pending' };
  const sample = {
    ...exp,
    contract: { execution_contract: 3 },
    snapshot: {
      plan: {
        schema_version: 3,
        stages: [
          { id: 'evaluate', needs: [] },
          { id: 'plot', needs: ['evaluate'] },
        ],
      },
    },
    stages: [{ id: 'evaluate', state: 'running' }, stage],
    jobs: [],
  };
  assert.match(stageSummary(sample, stage), /Waiting for outputs from evaluate/);
});

test('agent stages show turn activity and reconciliation without a scheduler job', () => {
  const check = { id: 'model_check', state: 'running' };
  const base = {
    ...exp,
    stages: [check],
    jobs: [],
    agent_actions: [{ phase: 'agent:model_check', state: 'running' }],
    stage_activity: [{ stage_id: 'model_check', summary: 'Fixing the tiny forward check' }],
  };
  assert.equal(stageSummary(base, check), 'Fixing the tiny forward check');
  assert.equal(
    stageSummary(
      { ...base, agent_actions: [{ phase: 'agent:model_check', state: 'uncertain' }] },
      check,
    ),
    'Waiting for turn reconciliation',
  );
  assert.equal(
    stageSummary(
      { ...base, agent_actions: [{ phase: 'agent:model_check', state: 'completed' }] },
      check,
    ),
    'Checking agent output',
  );
});

test('repair timeline selects the current attempt regardless of job array order', () => {
  const currentStage = { ...stage, attempt: 4 };
  const failed = { ...job, attempt: 1, state: 'failed', scheduler_state: 'FAILED' };
  const running = { ...job, attempt: 4, created_at: 200, scheduler_state: 'RUNNING', job_id: '99' };
  for (const jobs of [
    [failed, running],
    [running, failed],
  ]) {
    const repaired = { ...exp, jobs };
    assert.equal(stageJob(repaired, currentStage), running);
    assert.equal(stageSummary(repaired, currentStage), 'Job “fit” running');
    assert.equal(stageIndicator(repaired, currentStage), 'running');
  }
  const pending = { ...currentStage, state: 'pending' };
  const awaitingRetry = { ...exp, jobs: [failed] };
  assert.equal(stageJob(awaitingRetry, pending), undefined);
  assert.equal(stageIndicator(awaitingRetry, pending), 'pending');
  assert.notEqual(stageSummary(awaitingRetry, pending), 'Job “fit” failed');
  const reused = {
    ...exp,
    contract: { reused_job_intents: [job.intent_id] },
    jobs: [{ ...job, attempt: 1, state: 'complete' }],
  };
  assert.equal(stageJob(reused, { ...currentStage, state: 'complete' }), reused.jobs[0]);
});
