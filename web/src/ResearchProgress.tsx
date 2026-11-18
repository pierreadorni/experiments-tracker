import { Clock3 } from 'lucide-react';
import type { Experiment } from './types';

const date = (seconds: number) =>
  new Date(seconds * 1000).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });

export function ResearchProgress({ exp }: { exp: Experiment }) {
  const turns = exp.research_turns || [];
  const checkpoint = exp.research_checkpoint;
  return (
    <div className="progress-view research-progress">
      <section className="panel" aria-label="Research activity">
        <div className="section-heading">
          <h2>Research activity</h2>
          <span className="muted">
            {turns.length} {turns.length === 1 ? 'turn' : 'turns'}
          </span>
        </div>
        {!turns.length ? (
          <p className="muted">The first research turn is waiting to start.</p>
        ) : (
          <ol className="research-turns">
            {turns.map((turn, index) => (
              <li key={turn.id}>
                <div className="section-heading">
                  <strong>Turn {index + 1}</strong>
                  <span className="badge active">
                    {turn.status || turn.state.replaceAll('_', ' ')}
                  </span>
                </div>
                <p>
                  {turn.summary ||
                    (turn.state === 'running'
                      ? 'Research turn in progress.'
                      : 'No turn summary available.')}
                </p>
                {turn.next && (
                  <p className="muted">
                    <strong>Next:</strong> {turn.next}
                  </p>
                )}
                <small className="muted">
                  <Clock3 size={13} aria-hidden="true" /> {date(turn.updated_at)}
                </small>
              </li>
            ))}
          </ol>
        )}
      </section>
      <section className="panel" aria-label="Checkpoint status">
        <h2>Research checkpoint</h2>
        {checkpoint ? (
          <p>
            Version {checkpoint.version} · {checkpoint.bytes.toLocaleString()} bytes · saved{' '}
            {date(checkpoint.created_at)}. Notebook content is kept private.
          </p>
        ) : (
          <p className="muted">No checkpoint saved yet.</p>
        )}
      </section>
      <section className="panel" aria-label="Research jobs">
        <div className="section-heading">
          <h2>Jobs</h2>
          <span className="muted">{exp.jobs.length}</span>
        </div>
        {!exp.jobs.length ? (
          <p className="muted">No jobs submitted.</p>
        ) : (
          exp.jobs.map((job) => (
            <article className="research-job" key={job.intent_id}>
              <strong>{job.stage_id}</strong>
              <span
                className={`badge ${job.state === 'completed' ? 'success' : job.state === 'failed' ? 'danger' : 'active'}`}
              >
                {job.scheduler_state || job.state.replaceAll('_', ' ')}
              </span>
              {job.job_id && <span>Job {job.job_id}</span>}
              {job.error && <p>{job.error}</p>}
            </article>
          ))
        )}
      </section>
      <section className="panel">
        <h2>Report</h2>
        <p>Working report versions and published report are available in the Report tab.</p>
      </section>
    </div>
  );
}
