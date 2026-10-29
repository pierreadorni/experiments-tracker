import { useEffect, useRef, useState } from 'react';
import { X } from 'lucide-react';
import { api, RequestError } from './api';
import { Select } from './Select';
import { useDialog } from './useDialog';
import type { Project, ProjectReceipt } from './types';

export function NewProject({
  onClose,
  onReady,
}: {
  onClose: () => void;
  onReady: (project: Project) => Promise<void>;
}) {
  const [name, setName] = useState('');
  const [mode, setMode] = useState('new');
  const [folder, setFolder] = useState('');
  const [instructions, setInstructions] = useState('');
  const [requestId, setRequestId] = useState<string | null>(() =>
    new URLSearchParams(location.search).get('projectRequest'),
  );
  const [state, setState] = useState('');
  const [error, setError] = useState<string | null>(null);
  const dialog = useRef<HTMLElement>(null);
  const submitting = useRef(false);
  const ready = useRef(onReady);
  ready.current = onReady;
  useDialog(dialog, true, onClose);
  const clearRequest = () => {
    const url = new URL(location.href);
    url.searchParams.delete('projectRequest');
    history.replaceState(null, '', url);
    setRequestId(null);
  };
  useEffect(() => {
    if (!requestId) return;
    let active = true;
    const poll = async () => {
      try {
        const receipt = await api<ProjectReceipt>(`/api/projects/requests/${requestId}`);
        if (!active) return;
        setState(receipt.state);
        if (receipt.state === 'rejected') {
          setError(receipt.error || 'The project could not be added.');
          clearRequest();
        } else if (receipt.state === 'applied' && receipt.project) {
          clearRequest();
          await ready.current(receipt.project);
        }
      } catch (e) {
        if (active && e instanceof RequestError && e.status === 404 && !submitting.current) {
          setError('No saved project request was found. You can submit it again.');
          clearRequest();
          return;
        }
        if (active) setState('waiting_for_executor');
      }
    };
    void poll();
    const timer = setInterval(() => void poll(), 2000);
    return () => {
      active = false;
      clearInterval(timer);
    };
  }, [requestId]);
  return (
    <div data-overlay-backdrop className="modal-backdrop" onClick={onClose}>
      <section
        ref={dialog}
        className="modal project-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="project-title"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="section-heading">
          <h2 id="project-title">New project</h2>
          <button
            type="button"
            className="icon-button"
            aria-label="Close new project"
            onClick={onClose}
          >
            <X size={20} />
          </button>
        </div>
        {requestId ? (
          <p role="status">
            {state === 'applied'
              ? 'Opening project…'
              : 'Project request saved. Waiting for the executor…'}{' '}
            You can close this window; creating a project does not start an experiment.
          </p>
        ) : (
          <form
            onSubmit={(event) => {
              event.preventDefault();
              const id = crypto.randomUUID();
              const url = new URL(location.href);
              url.searchParams.set('projectRequest', id);
              history.replaceState(null, '', url);
              setError(null);
              submitting.current = true;
              setRequestId(id);
              setState('waiting_for_executor');
              void api<ProjectReceipt>('/api/projects', {
                method: 'POST',
                body: JSON.stringify({
                  schema_version: 1,
                  request_id: id,
                  name: name.trim(),
                  mode,
                  ...(mode === 'existing' ? { folder: folder.trim() } : {}),
                  allow_edits: true,
                  instructions: instructions.trim(),
                }),
              })
                .catch((e) => {
                  setError(e instanceof Error ? e.message : String(e));
                  if (e instanceof RequestError && e.status >= 400 && e.status < 500)
                    clearRequest();
                  // Preserve the saved identity after a lost response; polling establishes the outcome.
                })
                .finally(() => {
                  submitting.current = false;
                });
            }}
          >
            <label htmlFor="project-name">Project name</label>
            <input
              id="project-name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              required
              maxLength={80}
              autoComplete="off"
            />
            <label>Project folder</label>
            <Select
              label="Project folder"
              value={mode}
              onChange={setMode}
              options={[
                { value: 'new', label: 'Create a new folder' },
                { value: 'existing', label: 'Use an existing folder' },
              ]}
            />
            {mode === 'existing' ? (
              <>
                <label htmlFor="project-folder">Folder on the executor</label>
                <input
                  id="project-folder"
                  value={folder}
                  onChange={(e) => setFolder(e.target.value)}
                  required
                  maxLength={1000}
                  placeholder="~/bench/anyup-rs"
                  autoComplete="off"
                />
                <p role="note" className="notice warning">
                  This folder may already contain files. Adding it authorizes agents to modify those
                  files for your experiments. For worktree isolation with parallel experiments, this
                  folder needs a Git repository with an initial commit. Imported folders are never
                  initialized automatically.
                </p>
              </>
            ) : (
              <p className="muted">
                A folder named after your project will be created in the executor’s projects
                directory, with Git and an initial commit ready for separate worktrees. Agents can
                edit files there.
              </p>
            )}
            <label htmlFor="project-instructions">Project instructions (optional)</label>
            <textarea
              id="project-instructions"
              value={instructions}
              onChange={(e) => setInstructions(e.target.value)}
              maxLength={8000}
              rows={5}
              placeholder="Describe project conventions, scientific defaults, and resource preferences."
            />
            {error && (
              <p role="alert" className="error">
                {error}
              </p>
            )}
            <button className="primary full">
              {mode === 'new' ? 'Create project' : 'Use folder and allow edits'}
            </button>
          </form>
        )}
        {requestId && error && (
          <p role="alert" className="error">
            {error} Checking the saved request before retrying.
          </p>
        )}
      </section>
    </div>
  );
}
