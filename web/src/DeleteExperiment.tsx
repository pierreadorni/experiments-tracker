import { useRef } from 'react';
import { useDialog } from './useDialog';

export function DeleteExperiment({
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
        aria-labelledby="delete-title"
        aria-describedby="delete-description"
        onClick={(event) => event.stopPropagation()}
      >
        <h2 id="delete-title">Delete experiment?</h2>
        <p>
          <strong>{title}</strong>
        </p>
        <p id="delete-description">
          This removes the experiment from workspace lists, results and inbox views, and disables
          its artifact links. Audit records and output files remain retained for recovery and
          existing references. This does not erase files from storage.
        </p>
        <p>There is no restore action in the app. The executor must apply the deletion.</p>
        {error && (
          <p role="alert" className="error">
            {error}
          </p>
        )}
        <div className="delete-dialog-actions">
          <button type="button" disabled={busy} onClick={onClose}>
            Cancel
          </button>
          <button
            type="button"
            className="primary"
            disabled={busy}
            onClick={() => void onConfirm()}
          >
            {busy ? 'Submitting deletion…' : 'Delete experiment'}
          </button>
        </div>
      </section>
    </div>
  );
}
