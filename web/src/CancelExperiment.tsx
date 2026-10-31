import { useRef } from 'react';
import { useDialog } from './useDialog';

export function CancelExperiment({
  title,
  busy,
  error,
  onClose,
  onConfirm,
}: {
  title: string;
  busy: boolean;
  error: string | null;
  onClose: () => void;
  onConfirm: () => Promise<void>;
}) {
  const dialog = useRef<HTMLElement>(null);
  const close = () => {
    if (!busy) onClose();
  };
  useDialog(dialog, true, close);
  return (
    <div data-overlay-backdrop className="modal-backdrop" onClick={close}>
      <section
        ref={dialog}
        className="modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="cancel-title"
        aria-describedby="cancel-description"
        onClick={(event) => event.stopPropagation()}
      >
        <h2 id="cancel-title">Cancel experiment?</h2>
        <p>
          <strong>{title}</strong>
        </p>
        <p id="cancel-description">
          Stop agent work, cancel this experiment’s recorded jobs and remove all pending transfers.
          Saved results and files remain available.
        </p>
        <p>
          The executor must receive this request. If the server is unreachable, remote jobs may keep
          running until cancellation can be confirmed.
        </p>
        {error && (
          <p role="alert" className="error">
            {error}
          </p>
        )}
        <div className="delete-dialog-actions">
          <button type="button" disabled={busy} onClick={onClose}>
            Keep experiment
          </button>
          <button
            type="button"
            className="primary"
            disabled={busy}
            onClick={() => void onConfirm()}
          >
            {busy ? 'Requesting cancellation…' : 'Cancel experiment'}
          </button>
        </div>
      </section>
    </div>
  );
}
