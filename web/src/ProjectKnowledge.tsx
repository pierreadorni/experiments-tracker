import { useEffect, useState, type ReactNode } from 'react';
import { api, command } from './api';
import { Select } from './Select';
import { ResearchMemory, type SavedIdea } from './ResearchMemory';
import type { Experiment } from './types';

type Summary = {
  id: string;
  title: string;
  tldr: string;
  synthetic: boolean;
  limitations: string[];
};
type ReportBlock =
  | { kind: 'paragraph'; text: string }
  | {
      kind: 'table';
      caption: string;
      columns: string[];
      rows: {
        cells: { text?: string | null; display?: string | null }[];
        experiment_ids: string[];
      }[];
    }
  | {
      kind: 'figure';
      caption: string;
      alt: string;
      experiment_id: string;
      artifact_id: string;
    };
type Report = {
  version: number;
  created_at: number;
  sources: Record<string, string>;
  document: {
    title: string;
    overview: string;
    source_scope?: 'synthetic' | 'mixed' | 'research';
    sections: { title: string; body: string; experiment_ids: string[]; blocks?: ReportBlock[] }[];
    open_questions: string[];
  };
};
type Message = {
  id: string;
  kind: string;
  text: string;
  state: string;
  hold_reason: string | null;
  error: string | null;
  reply: string | null;
  experiment_id?: string;
};
type Memory = {
  revision: number;
  weekly: boolean;
  next_due: number | null;
  report: Report | null;
  summaries: Summary[];
  pending_experiment_ids: string[];
  report_versions: number[];
  messages: Message[];
  dismissed_ideas?: SavedIdea[];
};
type Model = { id: string; name: string; reasoning_efforts: string[]; default_effort: string };

export function ProjectKnowledge({
  projectId,
  models,
  open,
  experiments,
  useIdea,
}: {
  projectId: string;
  models: Model[];
  open: (id: string) => void;
  experiments: Experiment[];
  useIdea: (idea: SavedIdea) => void;
}) {
  const [memory, setMemory] = useState<Memory | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [text, setText] = useState('');
  const [busy, setBusy] = useState(false);
  const [model, setModel] = useState('');
  const [effort, setEffort] = useState('');
  const [version, setVersion] = useState('');
  const [older, setOlder] = useState<Report | null>(null);
  const path = `/api/projects/${encodeURIComponent(projectId)}/knowledge`;
  const refresh = async () => setMemory(await api<Memory>(path));
  useEffect(() => {
    let active = true;
    const poll = async () => {
      try {
        const value = await api<Memory>(path);
        if (active) {
          setMemory(value);
          setError(null);
        }
      } catch (e) {
        if (active) setError(e instanceof Error ? e.message : 'Unable to load project knowledge');
      }
    };
    void poll();
    const timer = setInterval(() => void poll(), 5000);
    return () => {
      active = false;
      clearInterval(timer);
    };
  }, [path]);
  useEffect(() => {
    let active = true;
    setOlder(null);
    if (version)
      void api<{ report: Report }>(`${path}/reports/${version}`)
        .then((value) => {
          if (active) setOlder(value.report);
        })
        .catch((e) => {
          if (active) setError(e.message);
        });
    return () => {
      active = false;
    };
  }, [path, version]);
  const send = async (kind: string, extra: Record<string, unknown> = {}) => {
    setBusy(true);
    setError(null);
    try {
      await command(path, {
        kind,
        ...(model ? { model } : {}),
        ...(effort ? { reasoning_effort: effort } : {}),
        ...extra,
      });
      if (kind === 'chat') setText('');
      if (kind === 'synthesize') setVersion('');
      await refresh();
      return true;
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Unable to save project command');
      return false;
    } finally {
      setBusy(false);
    }
  };
  const working = memory?.messages.some(
    (m) => m.kind === 'synthesize' && !['completed', 'failed'].includes(m.state),
  );
  const report = version ? older : memory?.report;
  const referenceIds = [
    ...new Set(report?.document.sections.flatMap((section) => section.experiment_ids) || []),
  ];
  const source = (id: string) => memory?.summaries.find((s) => s.id === id);
  const sourceButton = (id: string) => (
    <button key={id} className="text-button" onClick={() => open(id)}>
      {source(id)?.title || `Archived source ${id.slice(0, 8)}`}
    </button>
  );
  const referenceButton = (id: string) => {
    const number = referenceIds.indexOf(id) + 1;
    return number ? (
      <button
        key={id}
        className="memory-reference-button"
        onClick={() => open(id)}
        title={source(id)?.title || `Archived source ${id}`}
        aria-label={`Open experiment ${number}: ${source(id)?.title || id}`}
      >
        [{number}]
      </button>
    ) : null;
  };
  const renderProse = (value: string): ReactNode => {
    const uuid = '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}';
    const citations = new RegExp(`\\[(${uuid}(?:\\s*[;,]\\s*${uuid})*)\\]`, 'gi');
    const parts: ReactNode[] = [];
    let start = 0;
    for (const match of value.matchAll(citations)) {
      const ids = match[1].split(/\s*[;,]\s*/);
      if (!ids.every((id) => referenceIds.includes(id))) continue;
      parts.push(value.slice(start, match.index));
      parts.push(
        <span key={match.index} className="memory-inline-citations">
          {ids.map(referenceButton)}
        </span>,
      );
      start = match.index + match[0].length;
    }
    parts.push(value.slice(start));
    return parts;
  };
  const renderBlock = (
    block: ReportBlock,
    index: number,
    tableNumber: number,
    figureNumber: number,
  ) => {
    if (block.kind === 'paragraph') return <p key={index}>{renderProse(block.text)}</p>;
    if (block.kind === 'figure')
      return (
        <figure key={index} className="memory-figure">
          <img
            src={`/api/artifacts/${encodeURIComponent(block.artifact_id)}`}
            alt={block.alt}
            loading="lazy"
          />
          <figcaption>
            <strong>Figure {figureNumber}.</strong> {renderProse(block.caption)}
            <span className="memory-citation">Source: {referenceButton(block.experiment_id)}</span>
          </figcaption>
        </figure>
      );
    return (
      <div key={index} className="memory-table-wrap">
        <table>
          <caption>
            <strong>Table {tableNumber}.</strong> {renderProse(block.caption)}
          </caption>
          <thead>
            <tr>
              {block.columns.map((column, i) => (
                <th key={i} scope="col">
                  {column}
                </th>
              ))}
              <th scope="col">Evidence</th>
            </tr>
          </thead>
          <tbody>
            {block.rows.map((row, i) => (
              <tr key={i}>
                {row.cells.map((cell, j) => (
                  <td key={j}>{cell.display || cell.text}</td>
                ))}
                <td className="memory-table-sources">{row.experiment_ids.map(referenceButton)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  };
  let tableNumber = 0;
  let figureNumber = 0;
  return (
    <section className="project-knowledge" aria-label="Project knowledge">
      {error && (
        <p className="error notice" role="alert">
          {error}
        </p>
      )}
      <section className="panel knowledge-report">
        <div className="memory-toolbar">
          <div>
            <h2>Working report</h2>
            <p className="muted">Findings, open questions and evidence across experiments.</p>
          </div>
          <button
            className="primary"
            disabled={busy || working || !memory?.summaries.length}
            onClick={() => void send('synthesize')}
          >
            {working ? 'Update in progress…' : 'Update report'}
          </button>
        </div>
        <div className="memory-toolbar memory-report-controls">
          <label className="memory-schedule">
            <input
              type="checkbox"
              role="switch"
              checked={memory?.weekly ?? false}
              disabled={busy || !memory}
              onChange={(e) => void send('schedule', { weekly: e.target.checked })}
            />
            Update weekly with new results
          </label>
          {!!memory?.report_versions.length && (
            <Select
              label="Report version"
              value={version}
              options={[
                { value: '', label: 'Latest version' },
                ...memory.report_versions.map((v) => ({ value: String(v), label: `Version ${v}` })),
              ]}
              onChange={setVersion}
            />
          )}
        </div>
        {!memory && <p role="status">Loading project knowledge…</p>}
        {memory && !memory.report && (
          <p>
            No working report yet. Finish an experiment, then ask the supervisor to organize its
            findings here.
          </p>
        )}
        {version && !older && <p role="status">Loading saved version…</p>}
        {report && (
          <article className="working-report">
            <header className="memory-report-header">
              <h1>{report.document.title}</h1>
              <p className="memory-report-meta">
                Version {report.version} · {new Date(report.created_at * 1000).toLocaleDateString()}
              </p>
              {report.document.source_scope === 'synthetic' && (
                <p className="memory-scope">Synthetic demonstration · No research conclusion</p>
              )}
              {report.document.source_scope === 'mixed' && (
                <p className="memory-scope">
                  Mixed synthetic and research sources · Read source labels before comparing
                  findings
                </p>
              )}
            </header>
            <section className="memory-abstract" aria-label="Abstract">
              <h2>Abstract</h2>
              <p>{renderProse(report.document.overview)}</p>
            </section>
            {report.document.sections.map((section, index) => (
              <section key={index} className="memory-report-section">
                <h2>{section.title}</h2>
                <p>{renderProse(section.body)}</p>
                {section.blocks?.map((block, blockIndex) => {
                  if (block.kind === 'table') tableNumber++;
                  if (block.kind === 'figure') figureNumber++;
                  return renderBlock(block, blockIndex, tableNumber, figureNumber);
                })}
                <div className="memory-section-sources">
                  <span>Experiments</span>
                  {section.experiment_ids.map(referenceButton)}
                </div>
              </section>
            ))}
            {!!report.document.open_questions.length && (
              <section className="memory-report-section">
                <h2>Open questions</h2>
                <ul>
                  {report.document.open_questions.map((q, i) => (
                    <li key={i}>{renderProse(q)}</li>
                  ))}
                </ul>
              </section>
            )}
            <section className="memory-report-section">
              <h2>Sources</h2>
              <ol className="memory-references">
                {referenceIds.map((id) => (
                  <li key={id}>{sourceButton(id)}</li>
                ))}
              </ol>
            </section>
          </article>
        )}
        {!!memory?.pending_experiment_ids.length && (
          <aside className="notice">
            <strong>
              {memory.pending_experiment_ids.length} result
              {memory.pending_experiment_ids.length === 1 ? '' : 's'} waiting to be incorporated
            </strong>
            <div className="memory-sources">{memory.pending_experiment_ids.map(sourceButton)}</div>
          </aside>
        )}
        {!!memory?.summaries.length && (
          <details>
            <summary>Experiment TL;DRs ({memory.summaries.length})</summary>
            {memory.summaries.map((s) => (
              <article key={s.id} className="saved-idea">
                {sourceButton(s.id)}
                {s.synthetic && <p className="muted">Synthetic demonstration</p>}
                <p>{s.tldr || 'No scientific summary was saved.'}</p>
                {s.limitations?.length > 0 && <p className="muted">{s.limitations.join(' ')}</p>}
              </article>
            ))}
          </details>
        )}
      </section>
      {memory?.messages
        .filter((m) => m.kind === 'dismiss_idea' && m.state === 'failed')
        .map((m) => (
          <p key={m.id} className="error" role="alert">
            Could not remove the saved idea:{' '}
            {m.error || 'The executor rejected the request. Try again after refreshing.'}
          </p>
        ))}
      {memory?.messages.some(
        (m) => m.kind === 'dismiss_idea' && !['completed', 'failed'].includes(m.state),
      ) && (
        <p className="muted" role="status">
          Idea removal saved; waiting for the executor to sync.
        </p>
      )}
      <ResearchMemory
        experiments={experiments}
        dismissed={[
          ...(memory?.dismissed_ideas || []),
          ...(memory?.messages || [])
            .filter((m) => m.kind === 'dismiss_idea' && m.state !== 'failed' && m.experiment_id)
            .map((m) => ({ experiment_id: m.experiment_id!, text: m.text })),
        ]}
        busy={busy || !memory}
        open={open}
        useIdea={useIdea}
        remove={({ experiment_id, text }) => send('dismiss_idea', { experiment_id, text })}
      />
      <section className="panel knowledge-conversation" aria-label="Project conversation">
        <h2>Chat with Codex</h2>
        <p>
          Discuss next steps across this project. Codex reads the working report and can inspect
          individual experiment details.
        </p>
        <div className="project-chat-log" aria-live="polite">
          {memory?.messages
            .filter((m) => ['chat', 'synthesize'].includes(m.kind))
            .map((m) => (
              <article key={m.id} className="saved-idea">
                <strong>{m.kind === 'synthesize' ? 'Project supervisor' : 'You'}</strong>
                {m.text && <p>{m.text}</p>}
                {m.reply && <p className="memory-prose">{m.reply}</p>}
                {!['completed', 'failed'].includes(m.state) && (
                  <p role="status">
                    {m.state}
                    {m.hold_reason ? ` · ${m.hold_reason.replaceAll('_', ' ')}` : ''}
                  </p>
                )}
                {m.error && <p className="error">{m.error}</p>}
              </article>
            ))}
        </div>
        <form
          onSubmit={(e) => {
            e.preventDefault();
            void send('chat', { text });
          }}
        >
          <label className="memory-compose">
            Project message
            <textarea
              value={text}
              maxLength={8000}
              rows={3}
              onChange={(e) => setText(e.target.value)}
              placeholder="What have we learned, and what should we try next?"
            />
          </label>
          <div className="memory-toolbar">
            <div className="memory-models">
              <div className="memory-model-field">
                <span>Model</span>
                <Select
                  label="Model"
                  value={model}
                  options={[
                    { value: '', label: 'Default model' },
                    ...models.map((m) => ({ value: m.id, label: m.name })),
                  ]}
                  onChange={(value) => {
                    setModel(value);
                    setEffort('');
                  }}
                />
              </div>
              <div className="memory-model-field">
                <span>Reasoning</span>
                <Select
                  label="Reasoning"
                  value={effort}
                  options={[
                    { value: '', label: 'Default effort' },
                    ...(models.find((m) => m.id === model)?.reasoning_efforts || []).map(
                      (value) => ({
                        value,
                        label: value,
                      }),
                    ),
                  ]}
                  onChange={setEffort}
                />
              </div>
            </div>
            <button className="primary" disabled={busy || !text.trim()}>
              Send
            </button>
          </div>
        </form>
      </section>
    </section>
  );
}
