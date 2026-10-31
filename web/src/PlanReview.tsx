import { useState } from 'react';
import { ArrowRight, Check, ChevronDown, ChevronRight, LoaderCircle } from 'lucide-react';
import type { Experiment } from './types';
import { Select } from './Select';
import { resourceSummary } from './progress';
import { Description } from './Description';

type Decision = 'approve' | 'revise' | 'cancel';

export function PlanReview({
  exp,
  busy,
  onDecision,
}: {
  exp: Experiment;
  busy: boolean;
  onDecision: (decision: Decision, feedback?: string) => Promise<boolean>;
}) {
  const [feedback, setFeedback] = useState('');
  const [revising, setRevising] = useState(false);
  const [approvedExpanded, setApprovedExpanded] = useState(false);
  const plan = exp.plan_review;
  if (!plan || plan.state === 'drafting' || plan.state === 'cancelled') return null;
  const awaiting = plan.state === 'awaiting_review';
  const clarification = !!plan.questions?.length;
  const approved = plan.state === 'approved';
  const expanded = !approved || approvedExpanded;
  const panelId = `plan-${exp.id}-${plan.revision}`;
  return (
    <section
      className={`panel plan-review ${awaiting ? 'awaiting' : ''}`}
      aria-label="Experiment plan"
    >
      <div className="section-heading">
        {approved ? (
          <button
            type="button"
            className="plan-toggle"
            aria-expanded={expanded}
            aria-controls={panelId}
            onClick={() => setApprovedExpanded(!approvedExpanded)}
          >
            <Check size={17} />
            <h2>Approved plan</h2>
            {expanded ? <ChevronDown size={18} /> : <ChevronRight size={18} />}
          </button>
        ) : (
          <div>
            <p className="eyebrow">{awaiting ? 'YOUR REVIEW' : 'EXPERIMENT PLAN'}</p>
            <h2>
              {awaiting
                ? clarification
                  ? 'Planning needs your input'
                  : plan.scope === 'implementation'
                    ? 'Review the amended plan'
                    : 'Review before implementation'
                : 'Approved plan'}
            </h2>
          </div>
        )}
        {awaiting && (
          <span className="plan-review-state">
            {clarification
              ? 'Planning waits for your answer'
              : plan.scope === 'implementation'
                ? 'Compute waits for your approval'
                : 'Implementation waits for your approval'}
          </span>
        )}
      </div>
      {expanded && (
        <div id={panelId}>
          <Description className="plan-summary" text={plan.summary} />
          {clarification && (
            <ul>
              {plan.questions!.map((question, index) => (
                <li key={index}>{question}</li>
              ))}
            </ul>
          )}
          {plan.protocol && (
            <details>
              <summary>Protocol and assumptions</summary>
              <p className="muted">{plan.protocol}</p>
            </details>
          )}
          {plan.stages.length > 0 && (
            <ol className="plan-stage-list">
              {plan.stages.map((stage) => (
                <li key={stage.id}>
                  <strong>{stage.title}</strong>
                  {stage.summary && <span>{stage.summary}</span>}
                  {(plan.schema_version || 1) >= 2 && (
                    <span className="muted small">
                      {stage.needs?.length ? `After: ${stage.needs.join(', ')}` : 'Can start first'}
                    </span>
                  )}
                  {stage.executor === 'agent' && (
                    <span className="muted small">Agent · interactive checks and fixes</span>
                  )}
                  {stage.executor !== 'agent' && stage.resources && (
                    <span className="muted small">{resourceSummary(stage.resources)}</span>
                  )}
                </li>
              ))}
            </ol>
          )}
          {awaiting && (
            <div className="plan-actions">
              {!revising && !clarification ? (
                <>
                  <button
                    className="primary"
                    disabled={busy}
                    onClick={() => void onDecision('approve')}
                  >
                    <Check size={16} /> Approve plan
                  </button>
                  <button disabled={busy} onClick={() => setRevising(true)}>
                    Request a change <ArrowRight size={16} />
                  </button>
                  <button
                    className="text-button"
                    disabled={busy}
                    onClick={() => void onDecision('cancel')}
                  >
                    Cancel experiment
                  </button>
                </>
              ) : (
                <form
                  className="plan-feedback"
                  onSubmit={(event) => {
                    event.preventDefault();
                    if (!feedback.trim()) return;
                    void onDecision('revise', feedback.trim()).then((saved) => {
                      if (saved) {
                        setFeedback('');
                        setRevising(false);
                      }
                    });
                  }}
                >
                  <label htmlFor={`plan-feedback-${exp.id}`}>
                    {clarification ? 'Your answer' : 'What should change?'}
                  </label>
                  <textarea
                    id={`plan-feedback-${exp.id}`}
                    value={feedback}
                    onChange={(event) => setFeedback(event.target.value)}
                    maxLength={4000}
                    required
                    rows={3}
                    placeholder={
                      clarification
                        ? 'Answer the questions above so planning can continue.'
                        : 'Describe the changes to the plan.'
                    }
                  />
                  <div>
                    <button className="primary" disabled={busy || !feedback.trim()}>
                      {busy && <LoaderCircle className="spinner" size={16} />}
                      {clarification ? 'Send answer' : 'Send change'}
                    </button>
                    <button
                      type="button"
                      disabled={busy}
                      onClick={() =>
                        clarification ? void onDecision('cancel') : setRevising(false)
                      }
                    >
                      {clarification ? 'Cancel experiment' : 'Back'}
                    </button>
                  </div>
                </form>
              )}
            </div>
          )}
          {plan.state === 'revision_requested' && (
            <p className="muted small">Codex is revising the plan from your feedback.</p>
          )}
        </div>
      )}
    </section>
  );
}

export function Steering({
  exp,
  busy,
  onSend,
}: {
  exp: Experiment;
  busy: boolean;
  onSend: (text: string, mode: 'later' | 'restart') => Promise<boolean>;
}) {
  const [text, setText] = useState('');
  const failed = exp.phase === 'failed';
  const [selectedMode, setMode] = useState<'later' | 'restart'>('later');
  const mode = failed ? 'restart' : selectedMode;
  const pending =
    exp.steering?.filter((item) => ['pending', 'cancelling'].includes(item.state)) || [];
  if (['complete', 'cancelled'].includes(exp.phase) || exp.plan_review?.state === 'awaiting_review')
    return null;
  return (
    <section className="panel steering-panel" aria-label="Steer experiment">
      <h3>Steer this experiment</h3>
      <p className="muted">
        {failed
          ? 'This run stopped. Describe the correction for a linked replacement; its new plan needs your approval.'
          : 'Add a request to the later analysis, or restart compute with a corrected scientific plan.'}
      </p>
      {pending.map((item) => (
        <p className="notice" role="status" key={item.request_id}>
          {item.mode === 'restart'
            ? 'Restart requested; reconciling the current work.'
            : 'Saved for the interpretation and report.'}{' '}
          {item.text}
        </p>
      ))}
      <form
        onSubmit={(event) => {
          event.preventDefault();
          if (!text.trim()) return;
          void onSend(text.trim(), mode).then((saved) => {
            if (saved) setText('');
          });
        }}
      >
        <label htmlFor={`steering-${exp.id}`}>Instruction to Codex</label>
        <textarea
          id={`steering-${exp.id}`}
          value={text}
          onChange={(event) => setText(event.target.value)}
          maxLength={50000}
          required
          rows={3}
          placeholder="For example: add a per-class IoU plot, or use DINO input scale 1 as the main baseline."
        />
        {!failed && (
          <>
            <label>How should this apply?</label>
            <Select
              label="How should this instruction apply?"
              value={mode}
              onChange={(value) => setMode(value as 'later' | 'restart')}
              options={[
                { value: 'later', label: 'Add to later analysis/report' },
                { value: 'restart', label: 'Restart with this correction' },
              ]}
            />
          </>
        )}
        <p className="muted small steering-mode-note">
          {mode === 'later'
            ? 'Codex will use this for interpretation and reporting. Saved training stages and jobs stay as planned.'
            : 'The executor will cancel this experiment’s recorded job, then create a linked replacement with a revised plan and new compute.'}
        </p>
        <button
          className="primary"
          disabled={busy || !text.trim() || pending.some((item) => item.mode === 'restart')}
        >
          {mode === 'restart' ? 'Restart with correction' : 'Send instruction'}{' '}
          <ArrowRight size={16} />
        </button>
      </form>
    </section>
  );
}
