import type { ExecutionSettings, Experiment } from './types';

export function Protocol({ exp }: { exp: Experiment }) {
  const plan = exp.snapshot?.plan as
    | {
        protocol?: string;
        stages?: {
          id: string;
          title: string;
          executor?: 'agent' | 'slurm';
          resources?: Record<string, unknown> | null;
          local_outputs?: string[];
        }[];
      }
    | undefined;
  const agent = exp.contract.agent as { model?: string; reasoning_effort?: string } | undefined;
  const execution = exp.contract.execution as ExecutionSettings | undefined;
  const parameters = exp.snapshot?.parameters || exp.contract.parameters;
  const researchLoop = exp.contract.execution_contract === 4;
  return (
    <section className="panel protocol-panel">
      <h2>Protocol & provenance</h2>
      {plan?.protocol && <p>{plan.protocol}</p>}
      {execution?.context === 'jean-zay' && (
        <p className="muted">
          Jean Zay assigns memory through its allocation policy; saved memory values are planning
          estimates. Progress and ETA use the configured download interval.
        </p>
      )}
      <dl className="protocol-list">
        <div>
          <dt>Scheduled compute</dt>
          <dd>
            {execution?.context === 'jean-zay' ? 'Jean Zay' : 'Acacia'}
            {execution?.context === 'jean-zay' &&
              ` · checked every ${execution.poll_seconds / 60} minutes`}
          </dd>
        </div>
        <div>
          <dt>Project</dt>
          <dd>{(exp.contract.project as { name: string } | undefined)?.name || 'Test project'}</dd>
        </div>
        <div>
          <dt>Model</dt>
          <dd>
            {agent?.model || exp.model || 'Not resolved'}
            {agent?.reasoning_effort && ` · ${agent.reasoning_effort}`}
          </dd>
        </div>
        <div>
          <dt>Experiment</dt>
          <dd>{exp.id}</dd>
        </div>
        <div>
          <dt>Evidence revision</dt>
          <dd>{exp.evidence_revision}</dd>
        </div>
      </dl>
      {plan?.stages?.some((s) => s.executor === 'agent') && (
        <p className="muted">Small agent stages run on Acacia with interactive checks and fixes.</p>
      )}
      {plan?.stages?.some((s) => s.resources) && (
        <details className="technical-details" open>
          <summary>Planned Slurm resources</summary>
          {plan.stages
            .filter((s) => s.executor !== 'agent')
            .map((s) => (
              <div key={s.id}>
                <h3>{s.title}</h3>
                <pre>{JSON.stringify(s.resources, null, 2)}</pre>
                {Boolean(s.local_outputs?.length) && (
                  <p>Kept on execution machine: {s.local_outputs?.join(', ')}</p>
                )}
              </div>
            ))}
        </details>
      )}
      {parameters !== undefined && parameters !== null && (
        <details className="technical-details">
          <summary>Resolved parameters</summary>
          <pre>{JSON.stringify(parameters, null, 2)}</pre>
        </details>
      )}
      <details className="technical-details">
        <summary>
          {researchLoop || exp.contract.execution_contract === 3
            ? 'Saved experiment contract'
            : 'Frozen experiment contract'}
        </summary>
        <pre>{JSON.stringify(exp.contract, null, 2)}</pre>
      </details>
      {!researchLoop && (
        <details className="technical-details">
          <summary>
            {exp.contract.execution_contract === 3 ? 'Implementation record' : 'Snapshot manifest'}
          </summary>
          <pre>{JSON.stringify(exp.snapshot, null, 2)}</pre>
        </details>
      )}
      {(researchLoop || Boolean(exp.report_versions?.length)) && (
        <>
          <h3>Report provenance</h3>
          <p>
            {researchLoop
              ? 'Working reports are versioned from the evidence available at each update. Checkpoint notebook content remains private.'
              : exp.contract.execution_contract === 3
                ? 'Report versions retain small displayed measurements and figures. Working source and large outputs remain editable.'
                : 'Report versions derive from saved measurements. The original scientific source and results remain immutable.'}
          </p>
          {researchLoop &&
            (exp.report_versions?.length ? (
              <ol className="research-turns" aria-label="Working report status">
                {exp.report_versions.map((report) => (
                  <li key={report.version}>
                    <strong>Version {report.version}</strong> · {report.state}
                    {report.state === 'failed' && report.error && <p>{report.error}</p>}
                  </li>
                ))}
              </ol>
            ) : (
              <p className="muted">No working report yet.</p>
            ))}
          <div className="stage-output-links">
            {exp.artifacts
              .filter((a) => /^report-v[0-9]+\.json$/.test(a.name))
              .map((a) => (
                <a key={a.id} href={`/api/artifacts/${a.id}`} target="_blank" rel="noreferrer">
                  {a.name}
                </a>
              ))}
          </div>
        </>
      )}
    </section>
  );
}
