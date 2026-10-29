import { useEffect, useRef, useState } from 'react';
import { X } from 'lucide-react';
import { api, RequestError } from './api';
import { useDialog } from './useDialog';
import { defaultExecution, type ExecutionSettings, type Project } from './types';

type SettingsReceipt = {
  schema_version: 1;
  request_id: string;
  project_id: string;
  state: 'waiting_for_executor' | 'applied' | 'rejected';
  error: string | null;
  settings: {
    instructions: string;
    revision: number;
    execution?: ExecutionSettings;
    deleted?: boolean;
  } | null;
};

export function ProjectSettings({
  project,
  onClose,
  onSaved,
  onDeleted,
}: {
  project: Project;
  onClose: () => void;
  onSaved: () => Promise<void>;
  onDeleted: () => Promise<void>;
}) {
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [instructions, setInstructions] = useState(project.instructions || '');
  const [execution, setExecution] = useState<ExecutionSettings>(
    project.execution || defaultExecution,
  );
  const field = (key: keyof ExecutionSettings, value: string | number) =>
    setExecution((current) => ({ ...current, [key]: value }));
  const [requestId, setRequestId] = useState<string | null>(() =>
    new URLSearchParams(location.search).get('projectSettingsRequest'),
  );
  const [error, setError] = useState<string | null>(null);
  const [state, setState] = useState('');
  const dialog = useRef<HTMLElement>(null);
  const submitting = useRef(false);
  const saved = useRef(onSaved);
  saved.current = onSaved;
  const deleted = useRef(onDeleted);
  deleted.current = onDeleted;
  useDialog(dialog, true, onClose);

  const clearRequest = () => {
    const url = new URL(location.href);
    url.searchParams.delete('projectSettingsRequest');
    history.replaceState(null, '', url);
    setRequestId(null);
  };

  useEffect(() => {
    if (!requestId) return;
    let active = true;
    const poll = async () => {
      try {
        const receipt = await api<SettingsReceipt>(`/api/projects/settings/requests/${requestId}`);
        if (!active) return;
        if (receipt.project_id !== project.id) {
          setError('This saved request belongs to another project.');
          clearRequest();
          return;
        }
        setState(receipt.state);
        if (receipt.state === 'rejected') {
          setError(receipt.error || 'Project settings could not be saved.');
          clearRequest();
        } else if (receipt.state === 'applied' && receipt.settings) {
          if (receipt.settings.deleted) {
            clearRequest();
            await deleted.current();
            return;
          }
          setInstructions(receipt.settings.instructions);
          setExecution(receipt.settings.execution || defaultExecution);
          clearRequest();
          await saved.current();
        }
      } catch (e) {
        if (active && e instanceof RequestError && e.status === 404 && !submitting.current) {
          setError('No saved settings request was found. You can submit it again.');
          clearRequest();
        } else if (active) setState('waiting_for_executor');
      }
    };
    void poll();
    const timer = setInterval(() => void poll(), 2000);
    return () => {
      active = false;
      clearInterval(timer);
    };
  }, [requestId, project.id]);

  return (
    <div data-overlay-backdrop className="modal-backdrop" onClick={onClose}>
      <section
        ref={dialog}
        className="modal project-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="project-settings-title"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="section-heading">
          <div>
            <h2 id="project-settings-title">Project settings</h2>
            <p className="project-settings-name">{project.name}</p>
          </div>
          <button
            type="button"
            className="icon-button"
            aria-label="Close project settings"
            onClick={onClose}
          >
            <X size={20} />
          </button>
        </div>
        {requestId ? (
          <p role="status">
            {state === 'applied' ? 'Settings saved.' : 'Change saved; waiting for the executor…'}
          </p>
        ) : (
          <form
            onSubmit={(event) => {
              event.preventDefault();
              const id = crypto.randomUUID();
              const url = new URL(location.href);
              url.searchParams.set('projectSettingsRequest', id);
              history.replaceState(null, '', url);
              setError(null);
              submitting.current = true;
              setRequestId(id);
              setState('waiting_for_executor');
              void api<SettingsReceipt>(
                `/api/projects/${encodeURIComponent(project.id)}/settings`,
                {
                  method: 'PATCH',
                  body: JSON.stringify({
                    schema_version: 1,
                    request_id: id,
                    instructions,
                    execution,
                    delete: confirmDelete,
                  }),
                },
              )
                .catch((e) => {
                  setError(e instanceof Error ? e.message : String(e));
                  if (e instanceof RequestError && e.status >= 400 && e.status < 500)
                    clearRequest();
                })
                .finally(() => {
                  submitting.current = false;
                });
            }}
          >
            <section className="project-settings-section">
              <div className="project-settings-section-heading">
                <h3>Execution</h3>
                <p>Choose where new experiments run.</p>
              </div>
              <label htmlFor="execution-context">Run new experiments on</label>
              <select
                id="execution-context"
                value={execution.context}
                onChange={(event) => field('context', event.target.value)}
              >
                <option value="acacia">Acacia</option>
                <option value="jean-zay">Jean Zay · SSH / rsync</option>
              </select>
              <p className="muted">
                Agents, code and reports stay on acacia. Existing experiments keep their saved
                execution machine.
              </p>
              {execution.context === 'jean-zay' && (
                <>
                  <label htmlFor="execution-host">SSH host alias</label>
                  <input
                    id="execution-host"
                    value={execution.ssh_host}
                    maxLength={100}
                    onChange={(event) => field('ssh_host', event.target.value)}
                  />
                  <label htmlFor="execution-folder">Remote project folder</label>
                  <input
                    id="execution-folder"
                    value={execution.remote_project}
                    maxLength={1000}
                    placeholder="$WORK/source-supervision"
                    onChange={(event) => field('remote_project', event.target.value)}
                  />
                  <p className="muted">
                    Each run gets its own checkout and outputs beneath this folder. The agent can
                    configure transfers during implementation.
                  </p>
                  <label htmlFor="rsync-up">Rsync up · code and configs</label>
                  <textarea
                    id="rsync-up"
                    rows={3}
                    value={execution.rsync_up}
                    maxLength={8000}
                    placeholder="rsync -az --exclude=.git/ --exclude=.venv/ --exclude=.experimenttracker/ {local_project}/ {host}:{remote_project}/"
                    onChange={(event) => field('rsync_up', event.target.value)}
                  />
                  <label htmlFor="rsync-down">Rsync down · events and light artifacts</label>
                  <textarea
                    id="rsync-down"
                    rows={3}
                    value={execution.rsync_down}
                    maxLength={8000}
                    placeholder="rsync -az --include=*/ --include=*.tfevents* --include=*.png --exclude=* {host}:{remote_run}/ {local_run}/"
                    onChange={(event) => field('rsync_down', event.target.value)}
                  />
                  <p className="muted">
                    Commands run without a shell. Use the placeholders shown above. Required results
                    and job logs return automatically; large checkpoints stay on Jean Zay.
                  </p>
                  <label htmlFor="execution-poll">Check and download every (minutes)</label>
                  <input
                    id="execution-poll"
                    type="number"
                    min={10}
                    max={1440}
                    step={1}
                    value={execution.poll_seconds / 60}
                    onChange={(event) => field('poll_seconds', Number(event.target.value) * 60)}
                  />
                </>
              )}
            </section>
            <section className="project-settings-section">
              <div className="project-settings-section-heading">
                <h3>Agent instructions</h3>
                <p>
                  Guide future work with project conventions, scientific defaults, and resource
                  preferences. Existing experiments keep their saved evidence.
                </p>
              </div>
              <label htmlFor="project-settings-instructions">Instructions</label>
              <textarea
                id="project-settings-instructions"
                value={instructions}
                onChange={(event) => setInstructions(event.target.value)}
                maxLength={8000}
                rows={9}
                placeholder="For GPU jobs, consider all authorized devices that satisfy the stage requirements and prefer shorter queue waits."
              />
            </section>
            {error && (
              <p role="alert" className="error">
                {error}
              </p>
            )}
            {confirmDelete ? (
              <div className="project-delete-confirm">
                <h3>Delete {project.name}?</h3>
                <p>
                  This removes the project from the app. Its folder, files and audit history remain
                  on disk. Delete its experiments first and wait for project agent work to finish.
                  There is no restore action in the app.
                </p>
                <div className="delete-dialog-actions">
                  <button type="button" onClick={() => setConfirmDelete(false)}>
                    Cancel
                  </button>
                  <button className="project-delete-button">Confirm project deletion</button>
                </div>
              </div>
            ) : (
              <>
                <div className="project-settings-actions">
                  <button className="primary">Save settings</button>
                </div>
                <div className="project-delete-area">
                  <div>
                    <h3>Delete project</h3>
                    <p>Remove this project from the app.</p>
                  </div>
                  <button
                    type="button"
                    className="project-delete-link"
                    onClick={() => setConfirmDelete(true)}
                  >
                    Delete project…
                  </button>
                </div>
              </>
            )}
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
