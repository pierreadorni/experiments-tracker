import { useState } from 'react';
import type { Experiment } from './types';

export function IncrementalResults({ experiment: exp }: { experiment: Experiment }) {
  const [expanded, setExpanded] = useState(false);
  if (exp.contract.execution_contract !== 3) return null;
  const tasks = Object.entries(exp.partial_results?.tasks || {});
  const plan = exp.snapshot?.plan as
    | { stages: { id: string; title: string; requested?: boolean }[]; figures: string[] }
    | undefined;
  const unfinished = (plan?.stages || []).filter(
    (task) =>
      task.requested !== false && exp.stages.find((s) => s.id === task.id)?.state !== 'complete',
  );
  const metrics = tasks.flatMap(([stage, task]) =>
    task.result.metrics.map((metric) => ({ ...metric, stage, intent: task.intent_id })),
  );
  const figures = exp.artifacts.filter((a) => plan?.figures.includes(a.name));
  return (
    <div className="incremental-results">
      <h2>Results so far</h2>
      {metrics.length ? (
        <>
          <p>Available measurements stay here as other tasks finish.</p>
          <div
            className="results-table-scroll"
            tabIndex={0}
            role="region"
            aria-label="Available metrics"
          >
            <table>
              <caption>Registered evaluation measurements</caption>
              <thead>
                <tr>
                  <th scope="col">Metric</th>
                  <th scope="col">Variant</th>
                  <th scope="col">Value</th>
                  <th scope="col">Task</th>
                </tr>
              </thead>
              <tbody>
                {(expanded ? metrics : metrics.slice(0, 24)).map((m) => (
                  <tr key={`${m.stage}:${m.name}:${m.variant}`}>
                    <th scope="row">{m.name}</th>
                    <td>{m.variant}</td>
                    <td>
                      {new Intl.NumberFormat(undefined, { maximumSignificantDigits: 6 }).format(
                        m.value,
                      )}{' '}
                      {m.unit}
                    </td>
                    <td>
                      {plan?.stages.find((s) => s.id === m.stage)?.title || m.stage}
                      {exp.stages.find((s) => s.id === m.stage)?.state !== 'complete' &&
                        ' · earlier result retained'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {metrics.length > 24 && (
            <button className="text-button" onClick={() => setExpanded(!expanded)}>
              {expanded ? 'Show fewer metrics' : `Show all ${metrics.length} metrics`}
            </button>
          )}
        </>
      ) : (
        <p>No evaluation measurements are available yet.</p>
      )}
      {tasks.some(([, task]) => task.metrics_truncated) && (
        <p>Showing a bounded preview. Full measurements are available in Evidence.</p>
      )}
      {unfinished.length > 0 && (
        <>
          <h3>Requested work remaining</h3>
          <ul>
            {unfinished.map((task) => {
              const stage = exp.stages.find((s) => s.id === task.id);
              return (
                <li key={task.id}>
                  <strong>{task.title}</strong> · {stage?.state || 'pending'}
                  {stage?.error && <p>{stage.error}</p>}
                </li>
              );
            })}
          </ul>
        </>
      )}
      {(plan?.figures.length || 0) > 0 && (
        <>
          <h3>Figures</h3>
          {plan?.figures.map((name) => {
            const artifact = figures.find((a) => a.name === name);
            return artifact ? (
              <figure key={name}>
                <a href={`/api/artifacts/${artifact.id}`}>
                  <img loading="lazy" src={`/api/artifacts/${artifact.id}`} alt={name} />
                </a>
                <figcaption>{name}</figcaption>
              </figure>
            ) : (
              <p key={name}>{name} · pending</p>
            );
          })}
        </>
      )}
    </div>
  );
}
