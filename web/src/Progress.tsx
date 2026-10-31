import { useState } from 'react';
import { Check, ChevronDown, ChevronRight, Circle, Clock3, LoaderCircle, X } from 'lucide-react';
import type { Experiment, Status } from './types';
import { Description } from './Description';
import { TensorBoard } from './TensorBoard';
import {
  duration,
  jobTiming,
  observationTime,
  stageIndicator,
  stageSummary,
  stageTitle,
  timelineStages,
  resourceSummary,
  jobProgress,
  repairProgress,
  stageRecovery,
  stageJob,
} from './progress';

export function StageTimeline({ exp, status }: { exp: Experiment; status: Status | null }) {
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const stages = timelineStages(exp);
  const repair = repairProgress(exp);
  const completed = stages.filter((stage) => stage.state === 'complete').length;
  const defining =
    !['complete', 'failed', 'cancelled'].includes(exp.phase) &&
    (stages.length === 0 ||
      (exp.recipe_id === 'agent_experiment_v1' &&
        !exp.snapshot &&
        !exp.plan_review?.stages.length));
  return (
    <>
      {repair?.preparing && (
        <div className="notice" role="status">
          <strong>
            {repair.title} · attempt {repair.attempt}
          </strong>
          <p>Codex is checking saved outputs and preparing the remaining work.</p>
          {repair.completed.length > 0 && (
            <p>
              {repair.completed.length} compute{' '}
              {repair.completed.length === 1 ? 'stage completed' : 'stages completed'} previously.
              Reuse is being checked.
            </p>
          )}
          {repair.error && <Description text={repair.error} lines={3} />}
        </div>
      )}
      {!defining && !repair?.preparing && stages.length > 0 && (
        <div className="experiment-progress">
          <progress aria-label="Stages completed" value={completed} max={stages.length} />
          <span>
            {completed} of {stages.length} stages complete
          </span>
          <p className="muted small">Stage count, not elapsed time.</p>
        </div>
      )}
      {defining && (
        <div
          className="stage-placeholder"
          role="status"
          aria-label="Experiment stages are being prepared"
        >
          <p>
            {exp.plan_review?.state === 'approved'
              ? 'Preparing the approved stages.'
              : 'Codex is defining the experiment stages.'}
          </p>
          {[0, 1, 2].map((item) => (
            <div key={item} className="stage-skeleton" aria-hidden="true">
              <span className="skeleton-circle" />
              <span className="skeleton-line" />
              <span className="skeleton-tail" />
            </div>
          ))}
        </div>
      )}
      <ol className="stage-list">
        {stages.map((stage) => {
          const indicator = stageIndicator(exp, stage);
          const recovery = stageRecovery(exp, stage);
          const open = expanded[stage.id] ?? ['running', 'failed'].includes(stage.state);
          const job = stageJob(exp, stage);
          const execution = exp.contract.execution as
            | { context?: string; poll_seconds?: number }
            | undefined;
          const pollSeconds = execution?.context === 'jean-zay' ? execution.poll_seconds || 600 : 0;
          const progress = job ? jobProgress(job, undefined, pollSeconds) : null;
          const planned = !exp.snapshot
            ? exp.plan_review?.stages.find((s) => s.id === stage.id)
            : undefined;
          const activity = (exp.stage_activity || []).filter((a) => a.stage_id === stage.id);
          const outputs = exp.artifacts.filter((artifact) => artifact.stage_id === stage.id);
          const action = exp.agent_actions
            .filter((a) => a.phase === stage.id || a.phase === `agent:${stage.id}`)
            .at(-1);
          const observed = job ? observationTime(job, status) : null;
          const stale = Boolean(
            job &&
              job.state === 'submitted' &&
              (!observed || Date.now() / 1000 - observed > Math.max(90, 2 * pollSeconds)),
          );
          const panelId = `stage-${exp.id}-${stage.id}`;
          return (
            <li key={stage.id} className={`stage-row ${indicator} ${open ? 'expanded' : ''}`}>
              <button
                type="button"
                className="stage-toggle"
                aria-expanded={open}
                aria-controls={panelId}
                onClick={() => setExpanded((old) => ({ ...old, [stage.id]: !open }))}
              >
                <span className="stage-state" aria-hidden="true">
                  {indicator === 'complete' ? (
                    <Check size={17} />
                  ) : indicator === 'failed' || indicator === 'cancelled' ? (
                    <X size={17} />
                  ) : indicator === 'running' ? (
                    <LoaderCircle className="spinner" size={18} />
                  ) : indicator === 'waiting' ? (
                    <Clock3 size={17} />
                  ) : (
                    <Circle size={17} />
                  )}
                </span>
                <span className="stage-description">
                  <strong title={stageTitle(exp, stage)}>{stageTitle(exp, stage)}</strong>
                </span>
                <span className="stage-timing">
                  {recovery === 'retained'
                    ? 'Reuse pending'
                    : recovery === 'reused'
                      ? 'Reused'
                      : indicator === 'complete'
                        ? job?.started_at && job.ended_at
                          ? duration(job.ended_at - job.started_at)
                          : 'Complete'
                        : indicator === 'pending'
                          ? planned
                            ? 'Planned'
                            : 'Not started'
                          : indicator === 'waiting'
                            ? 'Waiting'
                            : indicator === 'failed'
                              ? 'Failed'
                              : indicator === 'cancelled'
                                ? 'Cancelled'
                                : job?.started_at
                                  ? duration((job.ended_at || Date.now() / 1000) - job.started_at)
                                  : 'Running'}
                </span>
                {open ? <ChevronDown size={18} /> : <ChevronRight size={18} />}
              </button>
              {recovery === 'retained' && (
                <p className="stage-progress muted small">Previously completed · checking reuse</p>
              )}
              {progress && (
                <div className={`stage-progress ${progress.stale ? 'stale' : ''}`}>
                  <div className="stage-progress-label">
                    <span>{Math.floor(progress.percent)}% of reported work</span>
                    <span>{progress.eta}</span>
                  </div>
                  <progress
                    aria-label={`${stageTitle(exp, stage)} reported work`}
                    value={progress.percent}
                    max={100}
                  />
                  {progress.stale && job?.progress && (
                    <p className="muted small">
                      Last reported {duration(Date.now() / 1000 - job.progress.reported_at)} ago.
                      ETA uses that report; slower progress may take longer.
                    </p>
                  )}
                </div>
              )}
              {!progress && job?.scheduler_state === 'RUNNING' && (
                <p className="stage-progress muted small">
                  Progress and ETA unavailable · this stage has not reported progress.
                </p>
              )}
              {job && <TensorBoard job={job} />}
              {open && (
                <div id={panelId} className="stage-body">
                  <Description className="muted" text={stageSummary(exp, stage)} lines={2} />
                  {action?.phase.startsWith('agent:') && (
                    <p className="muted small">Agent · interactive checks and fixes</p>
                  )}
                  {planned?.resources && (
                    <p className="muted small">{resourceSummary(planned.resources)}</p>
                  )}
                  {stage.error && <p className="error">{stage.error}</p>}
                  {stage.id === 'publish' &&
                    exp.phase === 'publishing' &&
                    exp.artifacts.length > 0 && (
                      <progress
                        aria-label="Evidence files published"
                        value={exp.artifacts.filter((a) => a.remote_published).length}
                        max={exp.artifacts.length}
                      />
                    )}
                  {stale && (
                    <p className="notice warning">
                      Scheduler status last observed{' '}
                      {observed ? duration(Date.now() / 1000 - observed) + ' ago' : 'never'}. The
                      job may have changed since then.
                    </p>
                  )}
                  {(job || activity.length > 0 || outputs.length > 0) && (
                    <details className="stage-execution-details">
                      <summary>Execution details</summary>
                      <p className="stage-full-title">{stageTitle(exp, stage)}</p>
                      {job && (
                        <>
                          <div className="stage-job-meta">
                            <span>Job {job.job_id || 'submission awaiting reconciliation'}</span>
                            <span>{job.scheduler_state || job.state}</span>
                            <span>
                              {job.resources.partition && `${job.resources.partition} · `}
                              {job.resources.nodes &&
                                job.resources.nodes > 1 &&
                                `${job.resources.nodes} nodes · `}
                              {job.resources.cpus_per_job} CPU/task ·{' '}
                              {job.resources.memory_mb_per_job} MiB/node ·
                              {job.resources.gpus_per_job > 0 &&
                                `${job.resources.gpus_per_job} ${job.resources.gpu_type || ''} GPU · `}
                              limit {job.resources.walltime_minutes_per_job} min
                            </span>
                            {job.exit_code && <span>Exit {job.exit_code}</span>}
                          </div>
                          <p className="stage-job-time">
                            {jobTiming(job)}
                            {job.pending_reason &&
                              ` · ${job.pending_reason.replace(/^\(|\)$/g, '')}`}
                          </p>
                          <ol className="job-handoff">
                            <li className={job.job_id ? 'done' : ''}>
                              Submit job {job.job_id && <Check size={12} />}
                            </li>
                            <li
                              className={job.started_at || job.state === 'complete' ? 'done' : ''}
                            >
                              {job.started_at || job.state === 'complete'
                                ? 'Job started'
                                : 'Wait for job to start'}
                            </li>
                            <li className={job.state === 'complete' ? 'done' : ''}>
                              Run and check outputs
                            </li>
                          </ol>
                        </>
                      )}
                      {activity.length > 0 && (
                        <ul className="stage-activity">
                          {activity.map((item, index) => (
                            <li key={`${item.created_at}:${index}`}>
                              <span
                                className={`activity-state ${item.state}`}
                                aria-label={item.state}
                              >
                                {item.state === 'complete' ? (
                                  <Check size={12} />
                                ) : item.state === 'failed' ? (
                                  <X size={12} />
                                ) : (
                                  <LoaderCircle
                                    className={item.state === 'running' ? 'spinner' : ''}
                                    size={16}
                                  />
                                )}
                              </span>
                              <span>{item.summary}</span>
                              <time>
                                {new Date(item.updated_at * 1000).toLocaleTimeString(undefined, {
                                  hour: '2-digit',
                                  minute: '2-digit',
                                  second: '2-digit',
                                })}
                              </time>
                            </li>
                          ))}
                        </ul>
                      )}
                      {outputs.length > 0 && (
                        <div className="stage-output-links">
                          {outputs.map((a) => (
                            <a
                              key={a.id}
                              href={`/api/artifacts/${a.id}`}
                              target="_blank"
                              rel="noreferrer"
                            >
                              {a.name}
                            </a>
                          ))}
                        </div>
                      )}
                    </details>
                  )}
                  {action?.error && !stage.error && <p className="error">{action.error}</p>}
                </div>
              )}
            </li>
          );
        })}
      </ol>
    </>
  );
}
