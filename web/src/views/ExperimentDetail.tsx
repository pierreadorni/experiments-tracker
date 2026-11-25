import { useRef, useState } from 'react';
import { Badge } from '../Badge';
import { Conversation } from '../Conversation';
import { Description } from '../Description';
import { ExperimentActions } from '../ExperimentActions';
import { FollowExperiment } from '../FollowExperiment';
import { BranchIntegration } from '../Integration';
import { StageTimeline } from '../Progress';
import { Protocol } from '../Protocol';
import { Report } from '../Report';
import { ResearchProgress } from '../ResearchProgress';
import { Select } from '../Select';
import { experimentKey, send } from '../relay';
import { downloadEvidenceBundle } from '../evidence';
import { date, experimentTitle, pretty, researchRequestQuestion, terminal } from '../format';
import { holdExplanation, holdGuidance, holdTitle } from '../holds';
import { timelineStages } from '../progress';
import { leadRunId } from '../lead';
import { Experiment } from '../types';
import {
  ArrowLeft,
  ArrowRight,
  Clock3,
  Download,
  FileCode2,
  FileJson,
  FileText,
  MessageSquare,
  Pause,
  Pencil,
  Play,
  Plus,
  Trash2,
  X,
} from 'lucide-react';
import type { ViewProps } from './props';

export function ExperimentDetail(
  props: Pick<
    ViewProps,
    | 'act'
    | 'answer'
    | 'busy'
    | 'choose'
    | 'compactViews'
    | 'conversationOpen'
    | 'deleting'
    | 'error'
    | 'experiments'
    | 'note'
    | 'reload'
    | 'report'
    | 'reportOptions'
    | 'setAnswer'
    | 'setCancelTarget'
    | 'setConversationOpen'
    | 'setDeleteTarget'
    | 'setError'
    | 'setNote'
    | 'setReportChoice'
    | 'setSelected'
    | 'setTab'
    | 'stageExecutionAvailable'
    | 'status'
    | 'tab'
    | 'theme'
  > & { exp: Experiment },
) {
  const {
    act,
    answer,
    busy,
    choose,
    compactViews,
    conversationOpen,
    deleting,
    error,
    exp,
    experiments,
    note,
    reload,
    report,
    reportOptions,
    setAnswer,
    setCancelTarget,
    setConversationOpen,
    setDeleteTarget,
    setError,
    setNote,
    setReportChoice,
    setSelected,
    setTab,
    stageExecutionAvailable,
    status,
    tab,
    theme,
  } = props;
  const [titleDraft, setTitleDraft] = useState<{ id: string; value: string } | null>(null);
  const actionsTrigger = useRef<HTMLButtonElement>(null);
  const editingTitle = titleDraft?.id === exp.id ? titleDraft : null;
  const titlePending = exp.pending_commands?.some((command) => command.kind === 'edit_title');
  const closeTitleEditor = () => {
    setTitleDraft(null);
    actionsTrigger.current?.focus();
  };
  return (
    <>
      <button className="back" onClick={() => setSelected(null)}>
        <ArrowLeft size={16} />
        All experiments
      </button>
      <div className="detail-heading">
        <div className="title-row">
          <h1>{experimentTitle(exp)}</h1>
          <ExperimentActions triggerRef={actionsTrigger}>
            <button
              className="text-button"
              disabled={busy || !status?.experiment_title_editing || titlePending}
              title={
                status?.experiment_title_editing
                  ? undefined
                  : 'Install the executor update to enable title editing'
              }
              onClick={() => {
                setError(null);
                setTitleDraft({ id: exp.id, value: experimentTitle(exp).slice(0, 100) });
              }}
            >
              <Pencil size={15} />
              Edit title
            </button>
            {!['complete', 'cancelled'].includes(exp.phase) && (
              <button
                className="text-button delete-experiment"
                disabled={
                  busy ||
                  !status?.experiment_cancellation ||
                  Boolean(exp.pending_commands?.some((c) => c.action === 'cancel'))
                }
                title={
                  status?.experiment_cancellation
                    ? 'Stop work and pending transfers'
                    : 'Install the executor update to enable cancellation'
                }
                onClick={() => {
                  setError(null);
                  setCancelTarget(exp);
                }}
              >
                <X size={15} />
                {exp.pending_commands?.some((c) => c.action === 'cancel')
                  ? 'Cancellation pending'
                  : 'Cancel experiment'}
              </button>
            )}
            {terminal.includes(exp.phase) && (
              <button
                className="text-button delete-experiment"
                disabled={
                  busy ||
                  deleting?.eid === exp.id ||
                  ['cancellation_pending', 'cancellation_unconfirmed'].includes(
                    exp.hold_reason || '',
                  ) ||
                  exp.report_versions?.some((r) =>
                    ['queued', 'building', 'publishing'].includes(r.state),
                  ) ||
                  exp.jobs.some((j) => ['submitting', 'submitted'].includes(j.state)) ||
                  exp.agent_actions.some((a) =>
                    ['dispatching', 'running', 'uncertain'].includes(a.state),
                  ) ||
                  exp.conversation?.some((m) => ['running', 'uncertain'].includes(m.state)) ||
                  Boolean(exp.pending_messages?.length)
                }
                onClick={() => {
                  setError(null);
                  setDeleteTarget(exp);
                }}
              >
                <Trash2 size={15} />
                {deleting?.eid === exp.id ? 'Deletion pending' : 'Delete experiment'}
              </button>
            )}
          </ExperimentActions>
        </div>
        {editingTitle && (
          <form
            className="title-editor"
            onSubmit={async (event) => {
              event.preventDefault();
              const title = editingTitle.value.trim();
              if (!title || busy || titlePending) return;
              const saved = await act(async () => {
                await send('edit_title', experimentKey(exp.id), { title });
              });
              if (saved) closeTitleEditor();
            }}
            onKeyDown={(event) => {
              if (event.key === 'Escape' && !busy) closeTitleEditor();
            }}
          >
            <label htmlFor="experiment-title">Experiment title</label>
            <input
              id="experiment-title"
              autoFocus
              required
              maxLength={100}
              value={editingTitle.value}
              onFocus={(event) => event.currentTarget.select()}
              onChange={(event) => setTitleDraft({ id: exp.id, value: event.target.value })}
              disabled={busy}
            />
            <div className="title-editor-buttons">
              <button
                type="button"
                className="text-button"
                onClick={closeTitleEditor}
                disabled={busy}
              >
                Cancel
              </button>
              <button
                className="primary"
                disabled={!editingTitle.value.trim() || busy || titlePending}
              >
                Save title
              </button>
            </div>
          </form>
        )}
        <div className="detail-meta">
          <Badge exp={exp} />
          {leadRunId(exp.contract) && <span>Autopilot lead</span>}
          {Boolean(exp.contract.synthetic_demo) && <span>Synthetic experiment</span>}
          <span>
            <Clock3 size={14} />
            {date(exp.created_at)}
          </span>
        </div>
        {experimentTitle(exp) !== exp.brief && (
          <Description key={`description:${exp.id}`} text={exp.brief} />
        )}
        <FollowExperiment key={`follow:${exp.id}`} id={exp.id} />
      </div>
      {exp.reference_experiment_id && (
        <p className="reference-link">
          Reference result{' '}
          <button
            className="text-button"
            disabled={!experiments.some((e) => e.id === exp.reference_experiment_id)}
            onClick={() => choose(exp.reference_experiment_id!)}
          >
            {exp.reference_experiment_id.slice(0, 8)} <ArrowRight size={14} />
          </button>
        </p>
      )}
      {exp.replaces_experiment_id && (
        <p className="reference-link">
          Corrected run ·{' '}
          <button className="text-button" onClick={() => choose(exp.replaces_experiment_id!)}>
            View earlier experiment <ArrowRight size={14} />
          </button>
        </p>
      )}
      {exp.steering
        ?.filter((item) => item.replacement_experiment_id)
        .map((item) => (
          <p className="reference-link" key={item.request_id}>
            Restarted with your correction ·{' '}
            <button className="text-button" onClick={() => choose(item.replacement_experiment_id!)}>
              Open replacement experiment <ArrowRight size={14} />
            </button>
          </p>
        ))}
      {Boolean(exp.pending_commands?.length) && (
        <section className="notice warning" role="status">
          <strong>Waiting for executor</strong>
          <span>
            {exp
              .pending_commands!.map((c) =>
                c.action
                  ? pretty(c.action)
                  : c.kind === 'edit_title'
                    ? 'Edit title'
                    : c.kind === 'create'
                      ? 'New experiment'
                      : c.kind === 'note'
                        ? 'Note delivery'
                        : c.kind === 'delete'
                          ? 'Delete experiment'
                          : c.kind === 'plan_review'
                            ? 'Plan review'
                            : c.kind === 'steer'
                              ? 'Steering instruction'
                              : c.kind === 'discuss'
                                ? 'Conversation message'
                                : c.kind === 'integrate'
                                  ? 'Integration request'
                                  : c.kind === 'revert_integration'
                                    ? 'Integration revert'
                                    : c.kind === 'approve_integration'
                                      ? 'Integration approval'
                                      : c.kind === 'dont_merge'
                                        ? 'Don’t merge update'
                                        : 'Answer delivery',
              )
              .join(' · ')}
            . Saved on the gateway; acacia has not applied these commands yet.
          </span>
        </section>
      )}
      {exp.hold_reason && (
        <div className="notice warning" role="status">
          <strong>{holdTitle(exp.hold_reason)}</strong>
          <span>{exp.error || holdExplanation(exp.hold_reason)}</span>
          <span>{holdGuidance(exp.hold_reason)}</span>
        </div>
      )}
      <BranchIntegration
        key={exp.id}
        exp={exp}
        status={status}
        busy={busy}
        error={error}
        onIntegrate={(reason) =>
          act(async () => {
            await send('integrate', experimentKey(exp.id), reason ? { reason } : {});
          })
        }
        onRevert={() =>
          act(async () => {
            await send('revert_integration', experimentKey(exp.id), {});
          })
        }
        onApprove={(approval) =>
          act(async () => {
            await send('approve_integration', experimentKey(exp.id), {
              action: 'approve',
              ...approval,
            });
          })
        }
        onDontMerge={(value) =>
          act(async () => {
            await send('dont_merge', experimentKey(exp.id), { value });
          })
        }
      />
      <div
        className={`experiment-workspace ${conversationOpen && exp.phase === 'complete' ? 'with-conversation' : ''}`}
      >
        <div className="experiment-pane">
          <div className="tabbar">
            <div
              className="tabs"
              role="tablist"
              aria-label="Experiment views"
              onKeyDown={(event) => {
                if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
                const buttons = [
                  ...event.currentTarget.querySelectorAll<HTMLButtonElement>('[role="tab"]'),
                ].filter((button) => button.getClientRects().length);
                const index = buttons.indexOf(document.activeElement as HTMLButtonElement);
                const next =
                  event.key === 'Home'
                    ? 0
                    : event.key === 'End'
                      ? buttons.length - 1
                      : (index + (event.key === 'ArrowRight' ? 1 : -1) + buttons.length) %
                        buttons.length;
                event.preventDefault();
                buttons[next].click();
                buttons[next].focus();
              }}
            >
              {['Progress', 'Report', 'Notes', 'Protocol', 'Evidence'].map((t) => (
                <button
                  key={t}
                  id={`tab-${t}`}
                  role="tab"
                  aria-selected={tab === t}
                  tabIndex={
                    tab === t ||
                    (compactViews && ['Protocol', 'Evidence'].includes(tab) && t === 'Progress')
                      ? 0
                      : -1
                  }
                  className={`${tab === t ? 'selected' : ''} ${['Protocol', 'Evidence'].includes(t) ? 'secondary-tab' : ''}`}
                  onClick={() => setTab(t)}
                >
                  {t}
                  {t === 'Notes' && exp.notes.length > 0 && <span>{exp.notes.length}</span>}
                </button>
              ))}
              <Select
                label="More experiment views"
                className={`more-views ${['Protocol', 'Evidence'].includes(tab) ? 'current' : ''}`}
                value={tab}
                displayValue={['Protocol', 'Evidence'].includes(tab) ? tab : 'More'}
                options={['Protocol', 'Evidence'].map((value) => ({ value, label: value }))}
                onChange={setTab}
              />
            </div>
            {exp.phase === 'complete' && (
              <button
                className={`discuss-button ${conversationOpen ? 'active' : ''}`}
                aria-label="Discuss results"
                aria-expanded={conversationOpen}
                onClick={() => setConversationOpen(!conversationOpen)}
              >
                <MessageSquare size={17} />
                <span>Discuss</span>
              </button>
            )}
          </div>
          <div role="tabpanel" aria-labelledby={`tab-${tab}`}>
            {tab === 'Progress' && exp.contract?.execution_contract === 4 && (
              <ResearchProgress exp={exp} />
            )}
            {tab === 'Progress' && exp.contract?.execution_contract !== 4 && (
              <div className="progress-view">
                <section className="panel progress-panel" aria-label="Progress">
                  <div className="section-heading">
                    <span className="muted">
                      {(timelineStages(exp).length === 0 ||
                        (exp.recipe_id === 'agent_experiment_v1' &&
                          !exp.snapshot &&
                          !exp.plan_review?.stages.length)) &&
                      !terminal.includes(exp.phase)
                        ? exp.plan_review?.state === 'awaiting_review'
                          ? 'Plan ready for review'
                          : 'Defining stages…'
                        : 'Stages'}
                    </span>
                  </div>
                  <StageTimeline key={exp.id} exp={exp} status={status} />
                </section>
                {!['complete', 'cancelled'].includes(exp.phase) && (
                  <section className="panel control-panel">
                    <h3>Automation controls</h3>
                    {!terminal.includes(exp.phase) && (
                      <>
                        <button
                          disabled={busy || !stageExecutionAvailable}
                          onClick={() =>
                            void act(async () => {
                              await send('action', experimentKey(exp.id), {
                                action: exp.pause_agent ? 'resume_agent' : 'pause_agent',
                              });
                            })
                          }
                        >
                          {exp.pause_agent ? <Play size={16} /> : <Pause size={16} />}
                          {exp.pause_agent ? 'Resume agent work' : 'Pause agent work'}
                        </button>
                        <button
                          disabled={busy || !stageExecutionAvailable}
                          onClick={() =>
                            void act(async () => {
                              await send('action', experimentKey(exp.id), {
                                action: exp.pause_compute
                                  ? 'resume_new_compute'
                                  : 'pause_new_compute',
                              });
                            })
                          }
                        >
                          {exp.pause_compute ? <Play size={16} /> : <Pause size={16} />}
                          {exp.pause_compute ? 'Resume new compute' : 'Pause new compute'}
                        </button>
                        <small>
                          Running scheduler jobs keep going. These controls never cancel a job.
                        </small>
                      </>
                    )}
                  </section>
                )}
              </div>
            )}
            {tab === 'Evidence' && (
              <section className="panel">
                <div className="section-heading">
                  <h2>
                    {exp.contract.execution_contract === 3
                      ? 'Published results & files'
                      : 'Validated artifacts'}
                  </h2>
                  {exp.phase === 'complete' && (
                    <button
                      className="text-button"
                      disabled={busy}
                      onClick={() => void act(() => downloadEvidenceBundle(exp))}
                    >
                      <Download size={17} />
                      {busy
                        ? 'Checking files…'
                        : exp.contract.execution_contract === 3
                          ? 'Download results'
                          : 'Download evidence bundle'}
                    </button>
                  )}
                </div>
                <p className="muted">
                  {exp.contract.execution_contract === 3 ? (
                    'Small displayed results and files are retained here. Downloads contain published objects and task context.'
                  ) : (
                    <>
                      Evidence revision {exp.evidence_revision}. The bundle is verified against the
                      saved file hashes.
                    </>
                  )}
                </p>
                {exp.artifacts.map((a) => (
                  <a
                    className="artifact-row"
                    key={a.id}
                    href={`/api/blobs/${a.id}`}
                    target="_blank"
                    rel="noreferrer"
                  >
                    {a.name.endsWith('.json') ? (
                      <FileJson size={20} />
                    ) : /\.(py|js|ts|sh)$/.test(a.name) ? (
                      <FileCode2 size={20} />
                    ) : (
                      <FileText size={20} />
                    )}
                    <div>
                      <strong>{a.name}</strong>
                    </div>
                    <span className="file-size">{(a.size / 1024).toFixed(1)} KiB</span>
                    <span className="muted small">
                      {status?.host === 'gateway'
                        ? a.remote_published
                          ? 'Published'
                          : 'Upload pending'
                        : exp.contract.execution_contract === 3
                          ? 'Registered'
                          : 'Local validated'}
                    </span>
                    <Download size={17} />
                  </a>
                ))}
                {exp.artifacts.length > 0 && (
                  <details className="technical-details">
                    <summary>Artifact identities & hashes</summary>
                    {exp.artifacts.map((a) => (
                      <dl className="protocol-list" key={a.id}>
                        <div>
                          <dt>{a.name}</dt>
                          <dd>
                            {a.id}
                            <br />
                            SHA-256 {a.sha256}
                          </dd>
                        </div>
                      </dl>
                    ))}
                  </details>
                )}
                {!exp.artifacts.length && <p>No registered files yet.</p>}
              </section>
            )}
            {tab === 'Report' && (
              <section className="panel report-panel">
                {report ? (
                  <>
                    <div className="report-toolbar">
                      {reportOptions.length > 1 && (
                        <Select
                          label="Report version"
                          value={report.id}
                          onChange={setReportChoice}
                          options={reportOptions.map((r) => ({
                            value: r.id,
                            label:
                              r.version === 1 ? 'Original · version 1' : `Version ${r.version}`,
                          }))}
                        />
                      )}
                      <a
                        className="text-button"
                        href={`/api/blobs/${report.id}`}
                        target="_blank"
                        rel="noreferrer"
                      >
                        Open report <ArrowRight size={16} />
                      </a>
                    </div>
                    {exp.report_versions
                      ?.filter((r) => r.state !== 'ready')
                      .map((r) => (
                        <p className="muted small" key={r.version}>
                          Report version {r.version}:{' '}
                          {r.state === 'failed' ? r.error : pretty(r.state)}
                        </p>
                      ))}
                    <Report id={report.id} theme={theme} />
                  </>
                ) : (
                  <div className="empty">
                    <FileText size={32} />
                    <h3>Interpretation is still pending.</h3>
                    <p>The final report will appear here when interpretation is ready.</p>
                  </div>
                )}
              </section>
            )}
            {tab === 'Notes' && (
              <section className="panel notes-panel">
                <h2>Notes</h2>
                {exp.notes.map((n) => (
                  <article className="note" key={n.id}>
                    <div>
                      <strong>{n.author}</strong>
                      <small>{date(n.created_at)}</small>
                    </div>
                    <p>{n.text}</p>
                  </article>
                ))}
                <form
                  onSubmit={(e) => {
                    e.preventDefault();
                    void act(async () => {
                      await send('note', experimentKey(exp.id), { text: note });
                      setNote('');
                    });
                  }}
                >
                  <label htmlFor="note">Add a note</label>
                  <textarea
                    id="note"
                    value={note}
                    onChange={(e) => setNote(e.target.value)}
                    maxLength={10000}
                    required
                    placeholder="An observation, question, or reminder…"
                  />
                  <button className="primary" disabled={busy || !note.trim()}>
                    Save note <Plus size={16} />
                  </button>
                </form>
              </section>
            )}
            {tab === 'Protocol' && <Protocol exp={exp} />}
          </div>
          {exp.pending_request && (
            <section className="panel decision">
              <h2>
                {exp.pending_request.expired
                  ? 'Expired agent request'
                  : 'Codex needs your decision'}
              </h2>
              <p>
                {exp.pending_request.expired
                  ? 'This request belonged to a previous process connection. It cannot be replayed; use the runbook to reconcile the saved action.'
                  : 'Answer this request to continue the current turn.'}
              </p>
              {exp.contract?.execution_contract === 4 ? (
                <div className="research-decision-summary">
                  {researchRequestQuestion(exp.pending_request.params) && (
                    <p>{researchRequestQuestion(exp.pending_request.params)}</p>
                  )}
                  {typeof (exp.pending_request.params as { objective?: unknown })?.objective ===
                    'string' && (
                    <p>
                      <strong>Proposed objective:</strong>{' '}
                      {(exp.pending_request.params as { objective: string }).objective}
                    </p>
                  )}
                  {exp.pending_request.method.includes('Approval') && (
                    <p className="muted">
                      This decision applies only to the exact request and proposed objective shown
                      above.
                    </p>
                  )}
                </div>
              ) : (
                <pre>{JSON.stringify(exp.pending_request.params, null, 2)}</pre>
              )}
              {!exp.pending_request.expired && (
                <form
                  onSubmit={(e) => {
                    e.preventDefault();
                    void act(async () => {
                      await send('answer', experimentKey(exp.id), {
                        pending_request_id: exp.pending_request!.id,
                        answer,
                      });
                      setAnswer('');
                    });
                  }}
                >
                  <label htmlFor="answer">
                    {exp.pending_request.method.includes('Approval')
                      ? 'Decision: accept, decline, or cancel'
                      : 'Your answer (multiple questions: JSON keyed by question ID)'}
                  </label>
                  {exp.contract?.execution_contract === 4 &&
                  exp.pending_request.method.includes('Approval') ? (
                    <div className="plan-actions">
                      {(['accept', 'decline', 'cancel'] as const).map((decision) => (
                        <button
                          className={decision === 'accept' ? 'primary' : undefined}
                          type="button"
                          key={decision}
                          disabled={busy}
                          onClick={() =>
                            void act(async () => {
                              await send('answer', experimentKey(exp.id), {
                                pending_request_id: exp.pending_request!.id,
                                answer: decision,
                              });
                            })
                          }
                        >
                          {decision[0].toUpperCase() + decision.slice(1)}
                        </button>
                      ))}
                    </div>
                  ) : (
                    <>
                      <input
                        id="answer"
                        value={answer}
                        onChange={(e) => setAnswer(e.target.value)}
                        required
                      />
                      <button className="primary" disabled={busy}>
                        Send answer
                      </button>
                    </>
                  )}
                </form>
              )}
            </section>
          )}
        </div>
        {exp.phase === 'complete' && (
          <Conversation
            key={exp.id}
            open={conversationOpen}
            exp={exp}
            status={status}
            reload={reload}
            openExperiment={choose}
            close={() => setConversationOpen(false)}
            openReport={(version) => {
              const report = exp.report_versions?.find((r) => r.version === version);
              setReportChoice(report?.report_artifact_id || null);
              setTab('Report');
            }}
          />
        )}
      </div>
    </>
  );
}
