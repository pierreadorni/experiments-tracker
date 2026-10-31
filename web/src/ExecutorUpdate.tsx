import { useEffect, useRef, useState } from 'react';
import { LoaderCircle, RefreshCw, X } from 'lucide-react';
import { api, command } from './api';
import { useDialog } from './useDialog';

export type MaintenanceStatus = {
  current_commit?: string | null;
  available?: boolean;
  release?: { commit_id: string; summary: string; state: string; error: string | null } | null;
  operation?: {
    request_id: string;
    release_id: string;
    state: string;
    error: string | null;
    updated_at: number;
  } | null;
  pending?: { request_id: string; state: string; error: string | null } | null;
  blockers?: string[];
  running_jobs?: number;
  observed_at?: number;
};
const labels: Record<string, string> = {
  preparing: 'Preparing update',
  testing: 'Testing update',
  ready: 'Update available',
  waiting: 'Waiting for a safe restart',
  backing_up: 'Backing up database',
  verifying_backup: 'Verifying backup',
  restarting: 'Stopping executor',
  starting: 'Starting updated executor',
  verifying: 'Checking recovery',
  complete: 'Update installed',
  blocked: 'Update needs attention',
  failed: 'Update stopped',
  cancelled: 'Update cancelled',
};
export function updateLabel(status: MaintenanceStatus): string {
  const operation = status.operation;
  if (operation && !['complete', 'failed', 'cancelled'].includes(operation.state))
    return labels[operation.state] || operation.state;
  if (status.pending?.state === 'pending') return 'Install request queued';
  if (status.pending?.state === 'rejected') return 'Install request rejected';
  if (operation?.state === 'failed') return labels.failed;
  if (status.release && status.release.commit_id !== status.current_commit)
    return labels[status.release.state] || 'Update preparation stopped';
  return status.current_commit ? 'Up to date' : 'Development checkout';
}

export function ExecutorUpdate({
  compact = false,
  onOpen,
}: {
  compact?: boolean;
  onOpen?: () => void;
}) {
  const [status, setStatus] = useState<MaintenanceStatus>({});
  const [error, setError] = useState('');
  const [confirm, setConfirm] = useState(false);
  const [busy, setBusy] = useState(false);
  const [queued, setQueued] = useState(false);
  const dialog = useRef<HTMLElement>(null);
  useDialog(dialog, confirm, () => setConfirm(false));
  useEffect(() => {
    let active = true;
    const poll = async () => {
      try {
        const next = await api<MaintenanceStatus>('/api/maintenance');
        if (active) {
          setStatus(next);
          if (next.operation || next.pending?.state === 'rejected') setQueued(false);
          setError('');
        }
      } catch {
        if (active) setError('Update status is unavailable. Last reported progress is shown.');
      }
    };
    void poll();
    const timer = window.setInterval(() => void poll(), 5000);
    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, []);
  const stale = Boolean(status.observed_at && Date.now() / 1000 - status.observed_at > 45);
  const operation = status.operation;
  const installing = Boolean(
    operation && !['complete', 'failed', 'cancelled'].includes(operation.state),
  );
  const pending = status.pending?.state === 'pending' || queued;
  const canInstall = Boolean(
    status.available &&
      status.release?.state === 'ready' &&
      status.release.commit_id !== status.current_commit &&
      !installing &&
      !pending,
  );
  const label = status.observed_at
    ? updateLabel(status)
    : error
      ? 'Update status unavailable'
      : 'Checking for updates…';
  const spinning =
    (installing && operation?.state !== 'blocked') ||
    ['preparing', 'testing'].includes(status.release?.state || '');
  if (compact)
    return (
      <button className="text-button executor-update-link" onClick={onOpen}>
        <RefreshCw size={14} />
        {label}
      </button>
    );
  return (
    <section className="panel executor-update">
      <h2>Application updates</h2>
      <p role="status" aria-live="polite" className="update-status">
        {spinning && <LoaderCircle size={18} className="update-spinner" aria-hidden="true" />}
        <strong>{label}</strong>
        {status.current_commit && <code>{status.current_commit.slice(0, 7)}</code>}
      </p>
      {status.release && status.release.commit_id !== status.current_commit && (
        <>
          <p>{status.release.summary}</p>
          <p className="muted">
            Pending commit <code>{status.release.commit_id.slice(0, 7)}</code>
            {status.release.state === 'ready' && ' · All release checks passed'}
          </p>
        </>
      )}
      {(stale || error) && (
        <p className="muted">
          {error || 'Progress has not refreshed recently. This is the last reported state.'}
        </p>
      )}
      {operation?.error && <p className="error">{operation.error}</p>}
      {status.pending?.error && <p className="error">{status.pending.error}</p>}
      {status.release?.error && <p className="error">{status.release.error}</p>}
      {installing && (
        <p>Saved jobs continue in Slurm. New dispatch stays held until recovery checks pass.</p>
      )}
      {operation?.state === 'waiting' && Boolean(status.blockers?.length) && (
        <ul>
          {status.blockers!.map((reason) => (
            <li key={reason}>{reason}</li>
          ))}
        </ul>
      )}
      {pending && !installing && (
        <p role="status">
          Installation authorization saved; waiting for the executor to accept it.
        </p>
      )}
      <button className="primary" disabled={!canInstall || busy} onClick={() => setConfirm(true)}>
        Install update…
      </button>
      {!status.available && (
        <p className="muted">
          Automatic installation becomes available after the maintenance-enabled executor is
          activated.
        </p>
      )}
      {confirm && (
        <div className="modal-backdrop" data-overlay-backdrop>
          <section
            className="modal"
            role="dialog"
            aria-modal="true"
            aria-labelledby="install-update-title"
            ref={dialog}
          >
            <div className="section-heading">
              <h2 id="install-update-title">Install tested update?</h2>
              <button
                className="icon-button"
                aria-label="Close install confirmation"
                onClick={() => setConfirm(false)}
              >
                <X size={20} />
              </button>
            </div>
            <p>{status.release?.summary}</p>
            <p>
              Commit <code>{status.release?.commit_id.slice(0, 7)}</code> passed the full release
              checks.
            </p>
            <p>
              Last reported: {status.running_jobs ?? 0} submitted Slurm jobs. They survive the
              executor restart.
            </p>
            <p>
              This authorizes waiting for active agent work to settle, backing up and verifying the
              database, restarting, and checking recovery. New work waits during installation.
            </p>
            {stale && <p>The executor will recheck current activity before stopping.</p>}
            <div className="delete-dialog-actions">
              <button onClick={() => setConfirm(false)} disabled={busy}>
                Cancel
              </button>
              <button
                className="primary"
                disabled={!canInstall || busy}
                onClick={async () => {
                  if (!status.release || busy) return;
                  setBusy(true);
                  try {
                    await command('/api/maintenance/install', {
                      release_id: status.release.commit_id,
                    });
                    setQueued(true);
                    setConfirm(false);
                    setStatus(await api<MaintenanceStatus>('/api/maintenance'));
                  } catch (err) {
                    setError(
                      err instanceof Error
                        ? err.message
                        : 'Could not save installation authorization',
                    );
                  } finally {
                    setBusy(false);
                  }
                }}
              >
                {busy ? 'Saving authorization…' : 'Authorize installation'}
              </button>
            </div>
            {error && (
              <p role="alert" className="error">
                {error}
              </p>
            )}
          </section>
        </div>
      )}
    </section>
  );
}
