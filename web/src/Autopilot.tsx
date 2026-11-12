import { useEffect, useState } from 'react';
import { Download, Pause, Play, Square, FileText, ChevronDown } from 'lucide-react';
import { api, command } from './api';
import type { Status } from './types';

type Policy = {
  schema_version: 1;
  subject: string;
  scope: string;
  instructions: string;
  exclusions: string;
  prerequisites: string;
  publication_goal: string;
  venue: string;
  guideline_links: string[];
  paper_format: string;
  start_at: number;
  handoff_at: number;
  submission_at: number;
  timezone: string;
  phase_percent: number[];
  borrowing_percent: number;
  protected_writing_seconds: number;
  weekly_gpu_hours: number;
  anchor_at: number;
  max_open_experiments: number;
  five_hour_reserve: number;
  weekly_reserve: number;
  model: string | null;
  reasoning_effort: string | null;
  may_abandon: boolean;
  weekly_report_at: number | null;
  notifications: boolean;
};
type Run = {
  id: string;
  revision: number;
  status: string;
  phase: string;
  foundation: string;
  hold_reason: string | null;
  central_argument: string | null;
  uncertainty: string | null;
  handoff: string | null;
};
type View = {
  available: boolean;
  run: Run | null;
  policy?: Policy;
  observed_at?: number;
  calendar?: {
    phase: string;
    week: number;
    weeks_remaining: number;
    boundaries: Record<string, number>;
  };
  budget?: {
    cap_seconds: number;
    consumed_seconds: number;
    reserved_seconds: number;
    available_seconds: number;
    unknown_seconds: number;
    reset_at: number;
    anomalies: { intent_id: string; reason: string }[];
  };
  documents?: { name: string; version: number }[];
  files?: { id: string; name: string; created_at: number }[];
  experiments?: {
    experiment_id: string;
    phase: string;
    deferred: number;
    priority: number;
    defer_reason: string | null;
    hold_reason?: string | null;
  }[];
  notices?: { id: string; kind: string; text: string; resolved_at: number | null }[];
  decisions?: {
    id: string;
    author: string;
    created_at: number;
    document: { name?: string; arguments?: { rationale?: string; reason?: string } };
  }[];
  messages?: { seq: number; kind: string; document: string }[];
  responses?: { id: string; cursor: number; result: string; created_at: number }[];
  pending_commands?: { request_id: string; state: string; error: string | null; payload: string }[];
  stage_holds?: { experiment_id: string; stage_id: string; reason: string; detail: string }[];
  backlog?: {
    id: string;
    document: {
      question: string;
      stop_condition: string;
      review_at: number;
      priority: number;
      status: string;
    };
  }[];
};
const label = (value: string) =>
  value === 'autopilot_researcher_input' ? 'Waiting for your reply' : value.replaceAll('_', ' ');
const hours = (seconds: number) =>
  new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(seconds / 3600);
const date = (seconds: number, zone?: string) => {
  try {
    return new Date(seconds * 1000).toLocaleString(undefined, { timeZone: zone });
  } catch {
    return new Date(seconds * 1000).toLocaleString() + ' · Check display timezone';
  }
};
const localDate = (seconds: number) => {
  const d = new Date(seconds * 1000);
  return new Date(d.getTime() - d.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
};
const starter = (): Policy => {
  const now = Math.floor(Date.now() / 1000);
  return {
    schema_version: 1,
    subject: '',
    scope: '',
    instructions: '',
    exclusions: '',
    prerequisites: '',
    publication_goal: '',
    venue: '',
    guideline_links: [],
    paper_format: 'LaTeX',
    start_at: now,
    handoff_at: 0,
    submission_at: 0,
    timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
    phase_percent: [1, 59, 30, 10],
    borrowing_percent: 5,
    protected_writing_seconds: 0,
    weekly_gpu_hours: 100,
    anchor_at: now,
    max_open_experiments: 2,
    five_hour_reserve: 50,
    weekly_reserve: 25,
    model: null,
    reasoning_effort: null,
    may_abandon: true,
    weekly_report_at: null,
    notifications: true,
  };
};

export function Autopilot({
  projectId,
  status,
  open,
  onActive,
}: {
  projectId: string;
  status: Status | null;
  open: (id: string) => void;
  onActive?: (active: boolean) => void;
}) {
  const path = `/api/projects/${encodeURIComponent(projectId)}/autopilot`;
  const [view, setView] = useState<View | null>(null),
    [error, setError] = useState<string | null>(null);
  const [editing, setEditing] = useState(false),
    [draft, setDraft] = useState<Policy>(starter);
  const [review, setReview] = useState(false),
    [busy, setBusy] = useState(false),
    [notice, setNotice] = useState('');
  const [text, setText] = useState(''),
    [answer, setAnswer] = useState<string | null>(null);
  const [control, setControl] = useState<{ eid: string; operation: string } | null>(null);
  const [doc, setDoc] = useState<{ name: string; version: number; content: string } | null>(null);
  const refresh = async () => setView(await api<View>(path));
  useEffect(() => {
    let active = true;
    const poll = async () => {
      try {
        const result = await api<View>(path);
        if (active) {
          setView(result);
          setError(null);
        }
      } catch (e) {
        if (active && status?.autopilot?.available)
          setError(e instanceof Error ? e.message : 'Unable to load Autopilot');
      }
    };
    void poll();
    const timer = setInterval(() => void poll(), 5000);
    return () => {
      active = false;
      clearInterval(timer);
    };
  }, [path, status?.autopilot?.available]);
  useEffect(() => {
    onActive?.(!!view?.run && !['draft', 'stopped', 'completed'].includes(view.run.status));
  }, [view?.run?.status, onActive]);
  const run = view?.run,
    policy = view?.policy,
    budget = view?.budget;
  const capable = !!status?.autopilot?.available && view?.available !== false;
  const pending =
    view?.pending_commands?.filter((c) => c.state === 'pending' || c.state === 'received') || [];
  const send = async (kind: string, fields: Record<string, unknown> = {}) => {
    setBusy(true);
    setError(null);
    try {
      const result = await command<{ state: string }>(path + '/commands', {
        kind,
        expected_revision: run?.revision || 0,
        ...fields,
      });
      setNotice(
        result.state === 'applied'
          ? 'Applied by executor.'
          : 'Received; waiting for executor to apply.',
      );
      await refresh();
      if (['message', 'answer', 'control'].includes(kind))
        setText((current) => (current === fields.text ? '' : current));
      setAnswer(null);
      setControl(null);
      if (kind === 'configure' || kind === 'revise') {
        setEditing(false);
        setReview(false);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Command failed; refresh before retrying');
    } finally {
      setBusy(false);
    }
  };
  const field = <K extends keyof Policy>(key: K, value: Policy[K]) =>
    setDraft((p) => ({ ...p, [key]: value }));
  const waiting = (reason: string) =>
    reason === 'autopilot_weekly_gpu_limit'
      ? 'Waiting for autopilot limit reset'
      : reason === 'autopilot_job_exceeds_weekly_cap'
        ? 'Job exceeds weekly allowance; change allocation or review cap'
        : label(reason);
  return (
    <section className="autopilot" aria-labelledby="autopilot-heading">
      <div className="autopilot-heading">
        <div>
          <h2 id="autopilot-heading">Research Autopilot</h2>
          <p className="muted">A bounded research project, from first review to paper handoff.</p>
        </div>
        {capable && !editing && (
          <button
            className={run ? 'secondary' : 'primary'}
            onClick={() => {
              setDraft(policy || starter());
              setEditing(true);
            }}
          >
            {run ? 'Review policy' : 'Set up Autopilot'}
          </button>
        )}
      </div>
      {!capable && (
        <p className="notice">
          Install the prepared Autopilot executor update to enable project setup. Existing
          experiments and read-only project chat remain available.
        </p>
      )}
      {error && (
        <p role="alert" className="notice warning">
          {error}
        </p>
      )}
      {notice && (
        <p role="status" className="muted">
          {notice}
        </p>
      )}
      {pending.length > 0 && (
        <p role="status" className="notice">
          {pending.length} command{pending.length > 1 ? 's' : ''} received; waiting for executor.
          Saved project state below has not changed yet.
        </p>
      )}
      {view?.pending_commands
        ?.filter((c) => c.state === 'rejected')
        .slice(0, 1)
        .map((c) => (
          <p key={c.request_id} role="alert" className="notice warning">
            Command refused: {c.error}
          </p>
        ))}
      {view?.observed_at && (
        <p className="muted small">
          Last observed {date(view.observed_at)}
          {Date.now() / 1000 - view.observed_at > 120 ? ' · Executor observation is old' : ''}
        </p>
      )}
      {editing && (
        <form
          onSubmit={(e) => {
            e.preventDefault();
            setReview(true);
          }}
          className="autopilot-setup"
        >
          <h3>{run?.status === 'draft' || !run ? 'Project setup' : 'Policy revision'}</h3>
          <label>
            Research subject
            <input
              required
              value={draft.subject}
              onChange={(e) => field('subject', e.target.value)}
            />
          </label>
          <label>
            Scope and central question
            <textarea
              required
              rows={3}
              value={draft.scope}
              onChange={(e) => field('scope', e.target.value)}
            />
          </label>
          <div className="autopilot-fields">
            <label>
              Publication goal
              <input
                required
                value={draft.publication_goal}
                onChange={(e) => field('publication_goal', e.target.value)}
              />
            </label>
            <label>
              Venue and year
              <input
                required
                value={draft.venue}
                onChange={(e) => field('venue', e.target.value)}
              />
            </label>
          </div>
          <label>
            Authoritative guideline links (one per line)
            <textarea
              value={draft.guideline_links.join('\n')}
              onChange={(e) => field('guideline_links', e.target.value.split('\n').filter(Boolean))}
            />
          </label>
          <div className="autopilot-fields">
            {(['start_at', 'handoff_at', 'submission_at'] as const).map((key) => (
              <label key={key}>
                {key === 'start_at'
                  ? 'Start'
                  : key === 'handoff_at'
                    ? 'Researcher handoff'
                    : 'Submission deadline'}
                <input
                  required
                  type="datetime-local"
                  value={draft[key] ? localDate(draft[key]) : ''}
                  onChange={(e) => field(key, new Date(e.target.value).getTime() / 1000)}
                />
              </label>
            ))}
          </div>
          <p className="muted">
            Dates entered in your browser timezone. Review buffer:{' '}
            {draft.submission_at > draft.handoff_at && draft.handoff_at
              ? ((draft.submission_at - draft.handoff_at) / 86400).toFixed(1) + ' days'
              : 'set both deadlines'}
            . Pauses and usage holds keep these dates.
          </p>
          <div className="autopilot-fields">
            <label>
              Maximum GPU-hours per week
              <input
                required
                type="number"
                min="0"
                step="any"
                value={draft.weekly_gpu_hours}
                onChange={(e) => field('weekly_gpu_hours', Number(e.target.value))}
              />
            </label>
            <label>
              Open experiments
              <input
                type="number"
                min="1"
                max="20"
                value={draft.max_open_experiments}
                onChange={(e) => field('max_open_experiments', Number(e.target.value))}
              />
            </label>
          </div>
          <div className="autopilot-fields">
            <label>
              Five-hour remaining reserve (%)
              <input
                type="number"
                min="0"
                max="90"
                value={draft.five_hour_reserve}
                onChange={(e) => field('five_hour_reserve', Number(e.target.value))}
              />
            </label>
            <label>
              Weekly remaining reserve (%)
              <input
                type="number"
                min="0"
                max="90"
                value={draft.weekly_reserve}
                onChange={(e) => field('weekly_reserve', Number(e.target.value))}
              />
            </label>
          </div>
          <p className="muted">
            Subscription usage is shared with your other work. Stricter owner reserves and resource
            limits also apply. Each job reserves its complete requested GPU allocation; one
            experiment can span multiple weeks.
          </p>
          <details>
            <summary>
              Research guidance and schedule <ChevronDown size={14} />
            </summary>
            {(['instructions', 'exclusions', 'prerequisites', 'paper_format'] as const).map(
              (key) => (
                <label key={key}>
                  {label(key)}
                  <textarea
                    rows={2}
                    value={draft[key]}
                    onChange={(e) => field(key, e.target.value)}
                  />
                </label>
              ),
            )}
            <div className="autopilot-fields">
              {['Foundation', 'Main argument', 'Ablations', 'Writing'].map((name, index) => (
                <label key={name}>
                  {name} (%)
                  <input
                    type="number"
                    min="0.01"
                    step="any"
                    value={draft.phase_percent[index]}
                    onChange={(e) =>
                      field(
                        'phase_percent',
                        draft.phase_percent.map((v, i) =>
                          i === index ? Number(e.target.value) : v,
                        ),
                      )
                    }
                  />
                </label>
              ))}
            </div>
            <div className="autopilot-fields">
              <label>
                Borrowing from ablations (percentage points)
                <input
                  type="number"
                  min="0"
                  step="any"
                  value={draft.borrowing_percent}
                  onChange={(e) => field('borrowing_percent', Number(e.target.value))}
                />
              </label>
              <label>
                Protected writing time (days)
                <input
                  type="number"
                  min="0"
                  step="any"
                  value={draft.protected_writing_seconds / 86400}
                  onChange={(e) =>
                    field('protected_writing_seconds', Math.round(Number(e.target.value) * 86400))
                  }
                />
              </label>
            </div>
            <label>
              Allowance week anchor
              <input
                required
                type="datetime-local"
                disabled={!!run && run.status !== 'draft'}
                value={localDate(draft.anchor_at)}
                onChange={(e) => field('anchor_at', new Date(e.target.value).getTime() / 1000)}
              />
            </label>
            <label>
              Display timezone
              <input
                required
                value={draft.timezone}
                onChange={(e) => field('timezone', e.target.value)}
              />
            </label>
            <label>
              Model
              <select
                value={draft.model || ''}
                onChange={(e) => field('model', e.target.value || null)}
              >
                <option value="">Executor default</option>
                {status?.models?.map((m) => (
                  <option key={m.id} value={m.id}>
                    {m.name}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Reasoning effort
              <select
                value={draft.reasoning_effort || ''}
                onChange={(e) => field('reasoning_effort', e.target.value || null)}
              >
                <option value="">Model default</option>
                {(
                  status?.models?.find((m) => m.id === (draft.model || status.default_model))
                    ?.reasoning_efforts || []
                ).map((v) => (
                  <option key={v}>{v}</option>
                ))}
              </select>
            </label>
            <label className="autopilot-check">
              <input
                type="checkbox"
                checked={draft.notifications}
                onChange={(e) => field('notifications', e.target.checked)}
              />
              Send attention and handoff push notices to devices following linked experiments
            </label>
            <label className="autopilot-check">
              <input
                type="checkbox"
                checked={draft.may_abandon}
                onChange={(e) => field('may_abandon', e.target.checked)}
              />
              Allow cancellation of Autopilot's own experiments
            </label>
          </details>
          {review && (
            <div className="notice">
              <strong>Review the saved authority</strong>
              <p>
                {draft.subject}: {draft.phase_percent.join(' / ')}% phase shares,{' '}
                {draft.weekly_gpu_hours} GPU-hours per fixed seven-day window, at most{' '}
                {draft.max_open_experiments} open experiments. Handoff{' '}
                {draft.handoff_at ? date(draft.handoff_at, draft.timezone) : 'required'}.
              </p>
              <p>
                Autopilot may create and approve experiments inside this scope
                {draft.may_abandon ? ', and cancel its own linked experiments' : ''}. It cannot
                enlarge these limits or submit a paper. Saving a draft starts no model work or
                compute.
              </p>
              <button
                type="button"
                className="primary"
                disabled={busy || pending.length > 0}
                onClick={() =>
                  void send(run && run.status !== 'draft' ? 'revise' : 'configure', {
                    policy: draft,
                  })
                }
              >
                {busy
                  ? 'Saving…'
                  : run && run.status !== 'draft'
                    ? 'Save policy revision'
                    : 'Save draft'}
              </button>
            </div>
          )}
          <div className="autopilot-actions">
            <button
              className="secondary"
              type="button"
              onClick={() => {
                setEditing(false);
                setReview(false);
              }}
            >
              Close
            </button>
            {!review && (
              <button className="primary" type="submit">
                Review policy
              </button>
            )}
          </div>
        </form>
      )}
      {run && policy && !editing && (
        <>
          <div className="autopilot-state">
            <strong>
              {label(run.status)} · {label(view?.calendar?.phase || run.phase)}
            </strong>
            <span>
              Week {(view?.calendar?.week || 0).toFixed(2)} ·{' '}
              {(view?.calendar?.weeks_remaining || 0).toFixed(2)} weeks remaining
            </span>
          </div>
          {run.foundation !== 'ready' && (
            <p className="notice">
              Foundation {label(run.foundation)}. Initial literature and protocol work must finish
              before main experiments.
            </p>
          )}
          {run.hold_reason && (
            <p className="notice warning">
              {label(run.hold_reason)}
              {run.hold_reason === 'autopilot_researcher_input' &&
                '. Review the pending question below and reply to continue.'}
            </p>
          )}
          {run.central_argument && <p className="autopilot-argument">{run.central_argument}</p>}
          {run.uncertainty && (
            <p>
              <strong>Open question:</strong> {run.uncertainty}
            </p>
          )}
          <ol className="autopilot-phases">
            {Object.entries(view?.calendar?.boundaries || {}).map(([phase, at]) => (
              <li key={phase} aria-current={phase === view?.calendar?.phase ? 'step' : undefined}>
                <strong>{label(phase)}</strong>
                <span>through {date(at, policy.timezone)}</span>
              </li>
            ))}
          </ol>
          <div className="autopilot-actions">
            {run.status === 'draft' && !review && (
              <button
                className="primary"
                disabled={busy || pending.length > 0}
                onClick={() => setReview(true)}
              >
                Review activation
              </button>
            )}
            {run.status === 'draft' && review && (
              <div className="notice">
                <p>
                  Authorize autonomous research within <strong>{policy.scope}</strong>, up to{' '}
                  {policy.weekly_gpu_hours} GPU-hours/week and {policy.max_open_experiments} open
                  experiments, until {date(policy.handoff_at, policy.timezone)}. Plans are approved
                  by Autopilot within this envelope. Fresh subscription permission and verified
                  cluster accounting are still required.
                </p>
                <button
                  className="primary"
                  disabled={busy || pending.length > 0}
                  onClick={() => void send('activate')}
                >
                  Activate Autopilot
                </button>
                <button className="secondary" onClick={() => setReview(false)}>
                  Keep draft
                </button>
              </div>
            )}
            {run.status === 'active' && (
              <button
                className="secondary"
                disabled={busy || pending.length > 0}
                onClick={() => void send('pause')}
              >
                <Pause size={15} />
                Pause Autopilot
              </button>
            )}
            {run.status === 'paused' && (
              <button
                className="primary"
                disabled={busy || pending.length > 0}
                onClick={() => void send('resume')}
              >
                <Play size={15} />
                Resume Autopilot
              </button>
            )}
            {['active', 'paused', 'handoff_ready', 'handoff_incomplete'].includes(run.status) && (
              <button
                className="secondary"
                disabled={busy || pending.length > 0}
                onClick={() => {
                  setControl({ eid: '', operation: 'stop' });
                  setText('');
                }}
              >
                <Square size={15} />
                Stop Autopilot…
              </button>
            )}
            {['handoff_ready', 'handoff_incomplete'].includes(run.status) && (
              <button className="secondary" disabled={busy} onClick={() => void send('complete')}>
                Mark researcher review complete
              </button>
            )}
          </div>
          {['active', 'paused', 'stopped', 'handoff_ready', 'handoff_incomplete'].includes(
            run.status,
          ) && (
            <p className="muted small">
              Pause and stop hold new work. Already submitted queued and running jobs continue and
              may finish after handoff. Explicit cancellation uses the experiment's existing
              cancellation flow.
            </p>
          )}
          {budget && (
            <section aria-labelledby="allowance-heading">
              <h3 id="allowance-heading">This week's allowance</h3>
              <dl className="autopilot-ledger">
                <div>
                  <dt>Maximum</dt>
                  <dd>{hours(budget.cap_seconds)} GPU-h</dd>
                </div>
                <div>
                  <dt>Consumed</dt>
                  <dd>{hours(budget.consumed_seconds)} GPU-h</dd>
                </div>
                <div>
                  <dt>Reserved</dt>
                  <dd>{hours(budget.reserved_seconds)} GPU-h</dd>
                </div>
                <div>
                  <dt>Available</dt>
                  <dd>{hours(budget.available_seconds)} GPU-h</dd>
                </div>
              </dl>
              <p className="muted">
                Resets {date(budget.reset_at, policy.timezone)}. Outstanding allocations carry
                across resets. Unknown accounting: {hours(budget.unknown_seconds)} GPU-h.
              </p>
              {budget.anomalies.map((a) => (
                <p className="notice warning" key={a.intent_id}>
                  {a.reason}
                </p>
              ))}
            </section>
          )}
          {(view?.stage_holds?.length || 0) > 0 && (
            <section>
              <h3>Held stages</h3>
              {view?.stage_holds?.map((s) => (
                <p key={s.experiment_id + s.stage_id}>
                  <button className="text-button" onClick={() => open(s.experiment_id)}>
                    {s.stage_id}
                  </button>{' '}
                  · {waiting(s.reason)}
                </p>
              ))}
            </section>
          )}
          {(view?.experiments?.length || 0) > 0 && (
            <section>
              <h3>Linked experiments</h3>
              <ul className="autopilot-experiments">
                {view?.experiments?.map((e) => (
                  <li key={e.experiment_id}>
                    <button className="text-button" onClick={() => open(e.experiment_id)}>
                      {e.experiment_id.slice(0, 8)}
                    </button>
                    <span>
                      {e.hold_reason === 'cancellation_pending'
                        ? 'Cancellation requested; awaiting confirmation'
                        : e.hold_reason === 'cancellation_unconfirmed'
                          ? 'Cancellation unconfirmed'
                          : label(e.phase)}
                      {e.deferred ? ' · deferred' : ''}
                    </span>
                    {!['complete', 'cancelled'].includes(e.phase) && (
                      <div>
                        <button
                          className="secondary"
                          onClick={() => {
                            setControl({
                              eid: e.experiment_id,
                              operation: e.deferred ? 'resume' : 'defer',
                            });
                            setText('');
                          }}
                        >
                          {e.deferred ? 'Resume' : 'Defer'}
                        </button>
                        <button
                          className="secondary"
                          onClick={() => {
                            setControl({ eid: e.experiment_id, operation: 'steer' });
                            setText('');
                          }}
                        >
                          Steer
                        </button>
                        <button
                          className="secondary"
                          onClick={() => {
                            setControl({ eid: e.experiment_id, operation: 'abandon' });
                            setText('');
                          }}
                        >
                          Cancel…
                        </button>
                      </div>
                    )}
                  </li>
                ))}
              </ul>
            </section>
          )}
          {control && (
            <form
              className="notice"
              onSubmit={(e) => {
                e.preventDefault();
                void send(
                  control.operation === 'stop' ? 'stop' : 'control',
                  control.operation === 'stop'
                    ? {}
                    : { experiment_id: control.eid, control: control.operation, text },
                );
              }}
            >
              <h3>
                {control.operation === 'abandon'
                  ? 'Cancel linked experiment'
                  : label(control.operation) + ' Autopilot work'}
              </h3>
              <p>
                {control.operation === 'abandon'
                  ? 'Cancellation stops pending work and requests cancellation of exact submitted jobs and active turns. Available results remain; confirmation may wait for remote accounting.'
                  : 'Submitted jobs keep running. Researcher pause controls remain separate.'}
              </p>
              <label>
                Reason
                <textarea required value={text} onChange={(e) => setText(e.target.value)} />
              </label>
              <button className="primary" disabled={busy || pending.length > 0}>
                Confirm {control.operation === 'abandon' ? 'cancellation' : control.operation}
              </button>
              <button type="button" className="secondary" onClick={() => setControl(null)}>
                Back
              </button>
            </form>
          )}
          <section>
            <h3>Working documents and paper</h3>
            {!view?.documents?.length && (
              <p className="muted">Documents appear after the first admitted foundation turn.</p>
            )}
            <ul className="autopilot-documents">
              {view?.documents?.map((d) => (
                <li key={d.name}>
                  <FileText size={16} />
                  <button
                    className="text-button"
                    onClick={async () => {
                      try {
                        const response = await fetch(
                          `${path}/documents/${encodeURIComponent(d.name)}/versions/${d.version}?run_id=${run.id}`,
                          { credentials: 'same-origin' },
                        );
                        if (!response.ok) throw new Error('Document is not available');
                        setDoc({ ...d, content: await response.text() });
                      } catch (e) {
                        setError(String(e));
                      }
                    }}
                  >
                    {label(d.name)} · v{d.version}
                  </button>
                  <a
                    href={`${path}/documents/${encodeURIComponent(d.name)}/versions/${d.version}?run_id=${run.id}`}
                    download
                    aria-label={`Download ${d.name}`}
                  >
                    <Download size={15} />
                  </a>
                </li>
              ))}
            </ul>
            {doc && (
              <details open>
                <summary>
                  {doc.name} · v{doc.version}
                </summary>
                <pre className="autopilot-document">{doc.content}</pre>
                <button className="secondary" onClick={() => setDoc(null)}>
                  Close document
                </button>
              </details>
            )}
            {view?.files?.slice(0, 1).map((file) => (
              <a key={file.id} className="secondary" href={`${path}/files/${file.id}`} download>
                <Download size={16} />
                Download compiled paper
              </a>
            ))}
            <a className="secondary" href={path + '/handoff'} download="autopilot-handoff.zip">
              <Download size={16} />
              Download current handoff
            </a>
            {run.handoff && (
              <p className="notice">
                {run.status === 'handoff_ready'
                  ? 'Controller reports a review-ready draft.'
                  : 'Handoff includes known gaps and missing deliverables.'}{' '}
                Researcher judgment is required before submission.
              </p>
            )}
          </section>
          {view?.backlog?.length ? (
            <details>
              <summary>Investigation backlog</summary>
              {view.backlog.map((item) => (
                <article key={item.id}>
                  <h4>{item.document.question}</h4>
                  <p>
                    {item.document.status} · priority {item.document.priority} · review{' '}
                    {date(item.document.review_at, policy.timezone)}
                  </p>
                  <p>Stop condition: {item.document.stop_condition}</p>
                </article>
              ))}
            </details>
          ) : null}
          <details>
            <summary>Research decisions</summary>
            {view?.decisions?.map((d) => (
              <p key={d.id}>
                {date(d.created_at)} ·{' '}
                {d.document.arguments?.rationale ||
                  d.document.arguments?.reason ||
                  label(d.document.name || 'Decision')}{' '}
                <span className="muted">
                  ({d.author.startsWith('agent:') ? 'Autopilot' : 'Researcher'})
                </span>
              </p>
            ))}
          </details>
          {view?.notices
            ?.filter((n) => !n.resolved_at)
            .map((n) => (
              <div key={n.id} className="notice">
                <p>{n.text}</p>
                {n.kind === 'researcher_input' && (
                  <button
                    className="secondary"
                    onClick={() => {
                      setAnswer(n.id);
                      setText('');
                    }}
                  >
                    Answer researcher question
                  </button>
                )}
              </div>
            ))}
          {!control && !['draft', 'completed', 'stopped'].includes(run.status) && (
            <form
              onSubmit={(e) => {
                e.preventDefault();
                void send(answer ? 'answer' : 'message', {
                  text,
                  ...(answer ? { input_id: answer } : {}),
                });
              }}
            >
              <h3>{answer ? 'Answer pending question' : 'Project guidance and questions'}</h3>
              <p className="muted">
                This conversation can steer Autopilot within the saved policy. Budget and deadline
                changes use Review policy. An advisory message does not cancel work. Read-only
                knowledge chat is below.
              </p>
              {view?.messages
                ?.slice()
                .reverse()
                .map((m) => (
                  <p className="autopilot-message" key={m.seq}>
                    <strong>Researcher:</strong> {JSON.parse(m.document).text}
                  </p>
                ))}
              {view?.responses
                ?.slice()
                .reverse()
                .map((response) => (
                  <p className="autopilot-message" key={response.id}>
                    <strong>Autopilot:</strong> {JSON.parse(response.result).summary}
                  </p>
                ))}
              <label>
                {answer ? 'Your answer' : 'Message to Autopilot'}
                <textarea
                  required
                  rows={3}
                  value={text}
                  onChange={(e) => setText(e.target.value)}
                />
              </label>
              <button className="primary" disabled={busy || pending.length > 0 || !text.trim()}>
                Send to Autopilot
              </button>
            </form>
          )}
        </>
      )}
    </section>
  );
}
