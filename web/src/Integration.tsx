import { useRef, useState } from 'react';
import { ArrowRight, Check, GitMerge, Undo2 } from 'lucide-react';
import type { Experiment, Status } from './types';
import {
  approvalBlockedReason,
  approvalIdentity,
  dontMerge,
  dontMergeBlockedReason,
  integrateBlockedReason,
  integrationHeadline,
  revertBlockedReason,
  shortCommit,
  type IntegrationApproval,
} from './integrationState';
import { useDialog } from './useDialog';

type DialogMode = 'integrate' | 'revert' | 'approve';
const COPY = {
  integrate: {
    title: 'Integrate this branch?',
    detail:
      'The executor queues this branch until the experiment is complete, then prepares a merge separately. It updates the base branch only after any configured check passes and any required approval is given.',
    confirm: 'Queue integration',
  },
  revert: {
    title: 'Revert this integration?',
    detail:
      'The backend queues a revert of the exact applied integration merge commit, and the branch becomes available for a later integration again. The executor must apply it.',
    confirm: 'Queue revert',
  },
  approve: {
    title: 'Approve this integration?',
    detail:
      'The backend marks exactly this prepared candidate ready, and the executor applies it with the saved base. A changed request or commit cannot reuse this approval.',
    confirm: 'Approve candidate',
  },
};

export function BranchIntegration({
  exp,
  status,
  busy,
  error,
  onIntegrate,
  onRevert,
  onApprove,
  onDontMerge,
}: {
  exp: Experiment;
  status: Status | null;
  busy: boolean;
  error: string | null;
  onIntegrate: (reason?: string) => Promise<boolean>;
  onRevert: () => Promise<boolean>;
  onApprove: (approval: IntegrationApproval) => Promise<boolean>;
  onDontMerge: (value: boolean) => Promise<boolean>;
}) {
  const [dialog, setDialog] = useState<DialogMode | null>(null);
  const [approval, setApproval] = useState<IntegrationApproval | null>(null);
  if (!exp.branch && !exp.integration) return null;
  const integrateReason = integrateBlockedReason(exp, status);
  const revertReason = revertBlockedReason(exp, status);
  const flagReason = dontMergeBlockedReason(exp, status);
  const approvable = approvalIdentity(exp);
  const approveReason = approvable ? approvalBlockedReason(exp, status) : null;
  const flagged = dontMerge(exp);
  return (
    <section className="integration-panel" aria-label="Branch and integration">
      <div className="section-heading">
        <div>
          <h2>Branch and integration</h2>
          <p className="integration-identity">
            <code>{exp.branch}</code> · head{' '}
            <code title={exp.head_commit}>{shortCommit(exp.head_commit)}</code>
          </p>
        </div>
        <span className="muted small" role="status">
          {integrationHeadline(exp)}
        </span>
      </div>
      <div className="integration-actions">
        <button
          className="primary"
          disabled={busy || Boolean(integrateReason)}
          title={integrateReason || 'Queue this branch for integration'}
          onClick={() => setDialog('integrate')}
        >
          <GitMerge size={16} />
          Integrate…
        </button>
        <button
          className="text-button"
          disabled={busy || Boolean(revertReason)}
          title={revertReason || 'Queue a revert of the applied integration'}
          onClick={() => setDialog('revert')}
        >
          <Undo2 size={15} />
          Revert integration…
        </button>
        {approvable && (
          <button
            className="text-button"
            disabled={busy || Boolean(approveReason)}
            title={approveReason || 'Approve exactly this prepared integration candidate'}
            onClick={() => {
              setApproval(approvable);
              setDialog('approve');
            }}
          >
            <Check size={15} />
            Approve integration…
          </button>
        )}
        <button
          className="text-button"
          aria-pressed={flagged}
          disabled={busy || Boolean(flagReason)}
          title={flagReason || 'Block future integration of this branch'}
          onClick={() => void onDontMerge(!flagged)}
        >
          {flagged ? 'Don’t merge · on' : 'Don’t merge'}
        </button>
      </div>
      <p className="muted small">
        Merging is queued and waits for completion, the project check and the executor.
      </p>
      {dialog && (
        <IntegrationDialog
          mode={dialog}
          branch={exp.branch || ''}
          approval={approval}
          error={error}
          onClose={() => {
            setDialog(null);
            setApproval(null);
          }}
          onConfirm={
            dialog === 'integrate'
              ? onIntegrate
              : dialog === 'revert'
                ? onRevert
                : () => (approval ? onApprove(approval) : Promise.resolve(false))
          }
        />
      )}
    </section>
  );
}

function IntegrationDialog({
  mode,
  branch,
  approval,
  error,
  onClose,
  onConfirm,
}: {
  mode: DialogMode;
  branch: string;
  approval: IntegrationApproval | null;
  error: string | null;
  onClose: () => void;
  onConfirm: (reason?: string) => Promise<boolean>;
}) {
  const dialog = useRef<HTMLElement>(null);
  const [reason, setReason] = useState('');
  const [saving, setSaving] = useState(false);
  const close = () => {
    if (!saving) onClose();
  };
  useDialog(dialog, true, close);
  const submit = () => {
    setSaving(true);
    void onConfirm(reason.trim() || undefined)
      .then((saved) => {
        if (saved) onClose();
      })
      .finally(() => setSaving(false));
  };
  const copy = COPY[mode];
  return (
    <div data-overlay-backdrop className="modal-backdrop" onClick={close}>
      <section
        ref={dialog}
        className="modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="integration-title"
        aria-describedby="integration-description"
        onClick={(event) => event.stopPropagation()}
      >
        <h2 id="integration-title">{copy.title}</h2>
        <p>
          <code>{branch}</code>
        </p>
        <p id="integration-description">{copy.detail}</p>
        {mode === 'approve' && approval && (
          <p className="muted small">
            Request <code>{approval.integration_request_id}</code> · candidate{' '}
            <code title={approval.candidate_commit}>{shortCommit(approval.candidate_commit)}</code>
          </p>
        )}
        {mode === 'integrate' && (
          <>
            <label htmlFor="integration-reason">Reason for integrating (optional)</label>
            <textarea
              id="integration-reason"
              value={reason}
              onChange={(event) => setReason(event.target.value)}
              maxLength={2000}
              rows={3}
              placeholder="For example: the report is ready and the measured improvement is worth keeping."
            />
          </>
        )}
        {error && (
          <p role="alert" className="error">
            {error}
          </p>
        )}
        <div className="delete-dialog-actions">
          <button type="button" disabled={saving} onClick={close}>
            Cancel
          </button>
          <button
            type="button"
            className="primary"
            disabled={saving || (mode === 'approve' && !approval)}
            onClick={submit}
          >
            {saving ? 'Saving request…' : copy.confirm}
            <ArrowRight size={16} />
          </button>
        </div>
      </section>
    </div>
  );
}
