import { useState } from 'react';
import { Check, ChevronRight, Clock3, Info, LoaderCircle } from 'lucide-react';
import { Select } from './Select';

export type ProgressMode = 'complete' | 'training' | 'preparing' | 'queued';
const stages = [
  {
    title: 'Prepare the experiment',
    detail: 'Brief translated into source, parameters and an ordered stage plan.',
    time: '1m 12s',
    activity: [
      'Read the reference protocol',
      'Prepared the comparison',
      'Froze source and dependencies',
    ],
  },
  {
    title: 'Train & evaluate',
    detail: 'Three architectures · five initializations · one fixed split.',
    time: '3m 46s',
    activity: ['Completed all 15 fits', 'Saved held-out predictions', 'Checked finite metrics'],
  },
  {
    title: 'Validate the evidence',
    detail: 'Outputs checked against the frozen protocol.',
    time: '8s',
    activity: [
      'Confirmed artifact identities',
      'Validated domain checks',
      'Recorded output checksums',
    ],
  },
  {
    title: 'Interpret & publish',
    detail: 'Report and evidence available together.',
    time: '54s',
    activity: [
      'Interpreted saved measurements',
      'Composed the report',
      'Published supporting evidence',
    ],
  },
];

export function ProgressStudy({
  mode,
  onChange,
}: {
  mode: ProgressMode;
  onChange: (mode: ProgressMode) => void;
}) {
  const active = mode === 'complete' ? stages.length : mode === 'training' ? 1 : 0;
  const [estimateDetails, setEstimateDetails] = useState(false);
  return (
    <section className="plain-section progress-study">
      <div className="section-title">
        <span className="progress-summary">
          {mode === 'complete'
            ? '4 stages · 6m total'
            : mode === 'training'
              ? '1 / 4 stages complete'
              : mode === 'preparing'
                ? 'Preparing source'
                : 'Waiting for a CPU slot'}
        </span>
        <Select
          className="progress-example"
          label="Progress example"
          value={mode}
          displayValue={
            mode === 'training'
              ? 'Training'
              : mode === 'preparing'
                ? 'Preparing'
                : mode === 'queued'
                  ? 'Queued'
                  : 'Completed'
          }
          onChange={(value) => onChange(value as ProgressMode)}
          options={[
            { value: 'complete', label: 'Preview: complete' },
            {
              value: 'training',
              label: 'Preview: training',
            },
            {
              value: 'preparing',
              label: 'Preview: preparing',
            },
            {
              value: 'queued',
              label: 'Preview: queued',
            },
          ]}
        />
      </div>
      <ol className="timeline" key={mode}>
        {stages.map((stage, index) => {
          // Queued compute has a completed preparation stage and a waiting training stage.
          const cursor = mode === 'queued' ? 1 : active;
          const done = index < cursor;
          const current = index === cursor;
          const state = done
            ? 'done'
            : current
              ? mode === 'queued'
                ? 'waiting'
                : 'running'
              : 'future';
          return (
            <li key={stage.title} className={`timeline-stage ${state}`}>
              <details open={current}>
                <summary>
                  <span
                    className={`stage-icon ${state}`}
                    aria-label={
                      done
                        ? 'Complete'
                        : state === 'running'
                          ? 'Running'
                          : state === 'waiting'
                            ? 'Queued'
                            : 'Not started'
                    }
                  >
                    {done ? (
                      <Check size={16} />
                    ) : state === 'running' ? (
                      <LoaderCircle className="spinner" size={18} />
                    ) : state === 'waiting' ? (
                      <Clock3 size={16} />
                    ) : (
                      index + 1
                    )}
                  </span>
                  <span>
                    <strong>{stage.title}</strong>
                  </span>
                  <time>
                    {done
                      ? stage.time
                      : current
                        ? mode === 'training'
                          ? '4m 18s elapsed'
                          : mode === 'preparing'
                            ? '38s elapsed'
                            : '2m queued'
                        : '—'}
                  </time>
                  <ChevronRight size={16} />
                </summary>
                <div className="stage-content">
                  {current && mode === 'training' && (
                    <div className="job-progress">
                      <div className="job-progress-label">
                        <span>244 / 400 epochs</span>
                        <strong>61%</strong>
                      </div>
                      <div
                        className="progress-track"
                        role="progressbar"
                        aria-label="Training progress"
                        aria-valuenow={61}
                        aria-valuemin={0}
                        aria-valuemax={100}
                        aria-valuetext="244 of 400 epochs, 61 percent"
                      >
                        <span style={{ width: '61%' }} />
                      </div>
                      <div className="job-estimate">
                        <span>
                          <Clock3 size={12} />
                          Estimated remaining: 2m 40s
                        </span>
                        <button
                          className="icon-button"
                          type="button"
                          aria-label="Estimate details"
                          aria-expanded={estimateDetails}
                          onClick={() => setEstimateDetails(!estimateDetails)}
                        >
                          <Info size={18} />
                        </button>
                      </div>
                      {estimateDetails && (
                        <p className="estimate-explanation">
                          Based on reported epochs and recent throughput. Last progress update: 8s
                          ago. Resource allocation: 1 CPU.
                        </p>
                      )}
                    </div>
                  )}
                  {current && mode === 'preparing' && (
                    <div className="agent-activity">Preparing training and evaluation scripts</div>
                  )}
                  {current && mode === 'queued' && (
                    <p className="queue-explanation">
                      Waiting for an available CPU slot. Start estimate unavailable.
                    </p>
                  )}
                  {done && (
                    <>
                      <p className="stage-detail">{stage.detail}</p>
                      <ul>
                        {stage.activity.map((item) => (
                          <li key={item}>
                            <Check size={13} />
                            {item}
                          </li>
                        ))}
                      </ul>
                    </>
                  )}
                  {state === 'future' && (
                    <p className="queue-explanation">Pending completion of the preceding stage.</p>
                  )}
                </div>
              </details>
            </li>
          );
        })}
      </ol>
    </section>
  );
}
