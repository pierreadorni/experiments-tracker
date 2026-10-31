import type { Experiment, Job, Stage, Status } from './types';

// Contract 3 retains failed attempts; array order is not the current job identity.
export function stageJob(exp: Experiment, stage: Stage): Job | undefined {
  const reused = (exp.contract?.reused_job_intents || []) as string[];
  return exp.jobs
    .filter(
      (job) =>
        job.stage_id === stage.id &&
        (stage.attempt === undefined ||
          job.attempt === undefined ||
          job.attempt === stage.attempt ||
          reused.includes(job.intent_id)),
    )
    .reduce<Job | undefined>((latest, job) => {
      if (!latest) return job;
      const attemptOrder = (job.attempt ?? 0) - (latest.attempt ?? 0);
      return attemptOrder > 0 || (attemptOrder === 0 && job.created_at > latest.created_at)
        ? job
        : latest;
    }, undefined);
}

export function repairProgress(exp: Experiment) {
  const repair = exp.contract?.auto_repair as
    | { kind?: string; error?: string; completed_stages?: string[] }
    | undefined;
  if (!repair) return null;
  const completed = (repair.completed_stages || []).filter(
    (id) => !['plan', 'prepare', 'validate', 'interpret', 'publish'].includes(id),
  );
  return {
    title: repair.kind === 'resources' ? 'Adjust compute resources' : 'Repair experiment',
    attempt: Number(exp.contract.repair_count) || 1,
    preparing: !exp.snapshot && !['complete', 'failed', 'cancelled'].includes(exp.phase),
    completed,
    error: repair.error || null,
  };
}

export function stageRecovery(exp: Experiment, stage: Stage): 'retained' | 'reused' | null {
  if (repairProgress(exp)?.preparing && repairProgress(exp)?.completed.includes(stage.id))
    return 'retained';
  const reused = exp.snapshot?.reused_stages as Record<string, unknown> | undefined;
  return stage.state === 'complete' && reused?.[stage.id] ? 'reused' : null;
}

export function experimentStatus(exp: Experiment): string {
  const pretty = (value: string) => value.replaceAll('_', ' ');
  if (['complete', 'failed', 'cancelled'].includes(exp.phase)) return pretty(exp.phase);
  if (exp.hold_reason) return pretty(exp.hold_reason);
  const repair = repairProgress(exp);
  if (repair?.preparing)
    return repair.title === 'Repair experiment' ? 'repairing' : 'adjusting resources';
  const jobs = exp.jobs.filter((job) => ['submitting', 'submitted'].includes(job.state));
  if (!jobs.length) return pretty(exp.phase);
  if (jobs.some((job) => job.scheduler_state === 'RUNNING')) return pretty(exp.phase);
  if (jobs.some((job) => job.scheduler_state === 'COMPLETING')) return 'finishing';
  if (jobs.some((job) => job.state === 'submitting')) return 'submitting';
  if (jobs.some((job) => ['PENDING', 'CONFIGURING'].includes(job.scheduler_state || '')))
    return 'queued';
  if (jobs.some((job) => !job.scheduler_state)) return 'waiting for scheduler';
  if (jobs.some((job) => job.scheduler_state === 'COMPLETED' && job.exit_code === '0:0'))
    return 'checking job outputs';
  if (jobs.some((job) => ['ACCOUNTING_PENDING', 'COMPLETED'].includes(job.scheduler_state || '')))
    return 'waiting for scheduler accounting';
  return `scheduler: ${pretty(jobs[0].scheduler_state!.toLowerCase())}`;
}

export function jobProgress(job: Job, now = Date.now() / 1000, pollSeconds = 0) {
  const sample = job.progress;
  if (
    !sample ||
    !Number.isFinite(sample.completed) ||
    !Number.isFinite(sample.total) ||
    sample.total <= 0 ||
    sample.completed < 0 ||
    sample.completed > sample.total ||
    !Number.isFinite(sample.reported_at)
  )
    return null;
  const age = Math.max(0, now - sample.reported_at);
  const stale =
    ['submitting', 'submitted'].includes(job.state) && age > Math.max(120, 2 * pollSeconds);
  const running = job.state === 'submitted' && job.scheduler_state === 'RUNNING';
  const reportedEta =
    sample.eta_seconds !== null && Number.isFinite(sample.eta_seconds) && sample.eta_seconds >= 0
      ? sample.eta_seconds
      : null;
  // Measure throughput at the sample time: time since that report cannot count
  // as additional measured work, and queue time is not execution time.
  const elapsed =
    job.started_at && Number.isFinite(job.started_at) && job.started_at > 0
      ? sample.reported_at - job.started_at
      : 0;
  const projectedEta =
    elapsed > 0 && sample.completed > 0
      ? elapsed * ((sample.total - sample.completed) / sample.completed)
      : null;
  const fallbackEta = projectedEta !== null && Number.isFinite(projectedEta) ? projectedEta : null;
  const eta = reportedEta ?? fallbackEta;
  // Old samples retain their full remaining-time estimate as an approximate
  // upper bound: unobserved time must not be treated as measured progress.
  const remaining = !stale && running && eta !== null ? eta - age : null;
  return {
    // Whole-stage work completion is separate from successful output validation.
    percent: (sample.completed / sample.total) * 100,
    stale,
    eta:
      job.state === 'complete'
        ? 'Outputs checked'
        : job.state === 'failed'
          ? 'Stage stopped'
          : !running
            ? 'ETA unavailable · job is not running'
            : sample.completed === sample.total
              ? 'Finishing and checking outputs'
              : stale && eta !== null && eta > 0
                ? `ETA ≈ up to ${duration(eta)} remaining · last report${reportedEta === null ? ' · based on progress' : ''}`
                : stale
                  ? 'ETA unavailable · last report has no usable estimate'
                  : remaining !== null && remaining > 0
                    ? `ETA ≈ ${duration(remaining)} remaining${reportedEta === null ? ' · based on progress' : ''}`
                    : remaining !== null
                      ? 'ETA updating…'
                      : 'ETA unavailable',
  };
}

// Review stages describe future work; execution rows remain authoritative once frozen.
export function timelineStages(exp: Experiment): Stage[] {
  const review = exp.plan_review;
  if (exp.snapshot || !review?.stages.length || review.state === 'cancelled') return exp.stages;
  const stages = exp.stages.filter((s) => ['plan', 'prepare'].includes(s.id));
  return [
    ...stages,
    ...review.stages.map((s) => ({
      id: s.id,
      state: 'pending',
      error: null,
      updated_at: exp.updated_at,
    })),
    ...exp.stages.filter((s) => ['validate', 'interpret', 'publish'].includes(s.id)),
  ];
}

export function resourceSummary(resources: string | Record<string, unknown>) {
  if (typeof resources === 'string') return resources;
  const gpuCount = Number(resources.gpus_per_job || 0);
  return [
    gpuCount
      ? `${gpuCount} GPU${gpuCount === 1 ? '' : 's'}${resources.gpu_type ? ` · ${resources.gpu_type}` : ''}`
      : `${resources.cpus_per_job || 1} CPU/task`,
    resources.partition,
    resources.walltime_minutes_per_job && `${resources.walltime_minutes_per_job} min limit`,
  ]
    .filter(Boolean)
    .join(' · ');
}

export function duration(seconds: number): string {
  const value = Math.max(0, Math.floor(seconds));
  if (value < 60) return `${value}s`;
  if (value < 3600) return `${Math.floor(value / 60)}m ${value % 60}s`;
  return `${Math.floor(value / 3600)}h ${Math.floor((value % 3600) / 60)}m`;
}
export function stageTitle(exp: Experiment, stage: Stage): string {
  if (stage.id === 'prepare' && repairProgress(exp)) return repairProgress(exp)!.title;
  const plan = exp.snapshot?.plan as { stages?: { id: string; title: string }[] } | undefined;
  return (
    plan?.stages?.find((s) => s.id === stage.id)?.title ||
    exp.plan_review?.stages.find((s) => s.id === stage.id)?.title ||
    (
      {
        plan: 'Plan experiment',
        prepare: 'Implement experiment',
        train_linear: 'Fit linear model',
        train_quadratic: 'Fit quadratic model',
        evaluate_and_plot: 'Evaluate and plot',
        validate: 'Validate evidence',
        interpret: 'Interpret results',
        publish: 'Publish report',
      } as Record<string, string>
    )[stage.id] ||
    stage.id.replaceAll('_', ' ')
  );
}
export function stageSummary(exp: Experiment, stage: Stage): string {
  if (stageRecovery(exp, stage) === 'retained')
    return 'Completed in the previous run. Checking whether its saved outputs can be reused.';
  if (stageRecovery(exp, stage) === 'reused')
    return 'Completed and checked in a previous run; reused here.';
  if (stage.id === 'plan' && exp.plan_review?.state === 'awaiting_review')
    return 'Waiting for your plan approval';
  if (stage.state === 'failed') return 'Stage failed';
  if (stage.state === 'cancelled') return 'Stage cancelled';
  const reviewed = exp.plan_review?.stages.find((s) => s.id === stage.id);
  if (!exp.snapshot && reviewed)
    return (
      reviewed.summary ||
      (exp.plan_review?.state === 'approved'
        ? 'Planned; implementation is being prepared'
        : 'Planned; waiting for plan approval')
    );
  const job = stageJob(exp, stage);
  const label = stage.id.replaceAll('_', ' ');
  if (job) {
    if (job.state === 'submitting') return `Submitting job “${label}”`;
    if (job.state === 'failed') return `Job “${label}” failed`;
    if (job.state === 'complete')
      return stage.state === 'complete' ? 'Completed and checked' : 'Checking job outputs';
    if (job.scheduler_state === 'COMPLETED' && job.exit_code === '0:0')
      return 'Checking job outputs';
    if (
      job.scheduler_state === 'ACCOUNTING_PENDING' ||
      (job.scheduler_state === 'COMPLETED' && !job.exit_code)
    )
      return 'Waiting for scheduler accounting';
    if (job.scheduler_state === 'RUNNING') return `Job “${label}” running`;
    if (job.scheduler_state === 'COMPLETING') return `Job “${label}” finishing`;
    if (!job.scheduler_state || ['PENDING', 'CONFIGURING'].includes(job.scheduler_state))
      return `Waiting for job “${label}” to start`;
    return `Scheduler: ${job.scheduler_state.toLowerCase().replaceAll('_', ' ')}`;
  }
  if (stage.state === 'complete') {
    if (stage.id === 'prepare') {
      const files = exp.snapshot?.files;
      return files
        ? `${Object.keys(files).filter((name) => name.endsWith('.py')).length} source files prepared`
        : 'Preparation complete';
    }
    return stage.id === 'publish' ? 'Report and evidence published' : 'Complete';
  }
  if (
    ['plan', 'prepare', 'interpret'].includes(stage.id) ||
    exp.agent_actions.some((a) => a.phase === `agent:${stage.id}`)
  ) {
    const action = exp.agent_actions
      .filter((a) => a.phase === stage.id || a.phase === `agent:${stage.id}`)
      .at(-1);
    if (action?.state === 'uncertain') return 'Waiting for turn reconciliation';
    if (action?.state === 'running' || action?.state === 'dispatching') {
      const activity = (exp.stage_activity || []).filter((a) => a.stage_id === stage.id).at(-1);
      return activity?.summary || 'Thinking…';
    }
    if (action?.state === 'failed') return 'Agent turn failed';
    if (action?.state === 'completed') return 'Checking agent output';
    if (exp.phase === 'waiting_for_executor' || exp.hold_reason === 'waiting_for_executor')
      return 'Waiting for executor';
    if (exp.hold_reason === 'agent_busy') return 'Waiting for Codex';
    if (exp.hold_reason === 'project_busy') return 'Waiting for project';
    if (exp.hold_reason === 'workspace_unavailable') return 'Waiting for working folder';
    if (exp.pause_agent) return 'Agent paused';
    if (exp.hold_reason === 'quota_hold') return 'Waiting for quota';
  }
  if (stage.id === 'publish' && exp.phase === 'publishing') {
    const uploaded = exp.artifacts.filter((a) => a.remote_published).length;
    return `Publishing evidence · ${uploaded}/${exp.artifacts.length} files uploaded`;
  }
  if (stage.state === 'running') return 'In progress';
  const frozenPlan = exp.snapshot?.plan as
    | { schema_version?: number; stages?: { id: string; needs?: string[] }[] }
    | undefined;
  const dag =
    (frozenPlan?.schema_version || 1) >= 2
      ? frozenPlan
      : (exp.plan_review?.schema_version || 1) >= 2
        ? exp.plan_review
        : null;
  const needs = dag?.stages?.find((item) => item.id === stage.id)?.needs;
  if (needs) {
    const waiting = needs.filter(
      (id) => exp.stages.find((item) => item.id === id)?.state !== 'complete',
    );
    if (waiting.length)
      return `Waiting for ${dag?.schema_version === 3 ? 'outputs' : 'checked outputs'} from ${waiting.join(', ')}`;
    return exp.pause_compute ? 'New compute paused' : `Ready to queue job “${label}”`;
  }
  const position = exp.stages.findIndex((s) => s.id === stage.id);
  const ready = position > 0 && exp.stages.slice(0, position).every((s) => s.state === 'complete');
  if (ready && !['plan', 'prepare', 'validate', 'interpret', 'publish'].includes(stage.id))
    return exp.pause_compute
      ? 'New compute paused'
      : exp.hold_reason === 'compute_busy'
        ? 'Waiting for compute slot'
        : `Queue job “${label}”`;
  return 'Not started';
}
export function jobTiming(job: Job, now = Date.now() / 1000): string {
  const time = (stamp: number) =>
    new Date(stamp * 1000).toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' });
  if (job.started_at) {
    const elapsed = duration((job.ended_at || now) - job.started_at);
    return job.ended_at
      ? `Ran for ${elapsed}`
      : `Running for ${elapsed} · limit ${job.resources.walltime_minutes_per_job} min`;
  }
  if (job.state === 'complete' || job.state === 'failed') return 'Timing was not recorded';
  const queued = job.created_at ? `Queued for ${duration(now - job.created_at)} · ` : '';
  return (
    queued +
    (job.estimated_start_at && job.estimated_start_at > now
      ? `Estimated start ${time(job.estimated_start_at)}`
      : 'Start estimate unavailable')
  );
}
export function observationTime(job: Job, status: Status | null): number | null {
  const fresh = status?.job_observations?.[job.intent_id];
  return fresh?.job_id === job.job_id ? fresh.observed_at : job.observed_at;
}

export function stageIndicator(exp: Experiment, stage: Stage): string {
  if (stageRecovery(exp, stage) === 'retained') return 'waiting';
  if (stage.id === 'plan' && exp.plan_review?.state === 'awaiting_review') return 'waiting';
  if (['complete', 'failed', 'cancelled'].includes(stage.state)) return stage.state;
  const job = stageJob(exp, stage);
  if (job) {
    if (job.state === 'failed') return 'failed';
    if (job.state === 'submitting') return 'waiting';
    if (job.state === 'submitted')
      return ['RUNNING', 'COMPLETING'].includes(job.scheduler_state || '') ? 'running' : 'waiting';
  }
  const action = exp.agent_actions.filter((a) => a.phase === stage.id).at(-1);
  if (action?.state === 'uncertain') return 'waiting';
  if (action?.state === 'failed') return 'failed';
  if (action && ['running', 'dispatching'].includes(action.state)) return 'running';
  if (stage.state === 'running') return exp.hold_reason ? 'waiting' : 'running';
  return 'pending';
}
