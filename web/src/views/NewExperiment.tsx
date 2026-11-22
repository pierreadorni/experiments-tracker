import { Select } from '../Select';
import { loadStatus, send } from '../relay';
import { contractFourCreateIssue } from '../createContract.mjs';
import { experimentTitle } from '../format';
import { Status } from '../types';
import { ArrowRight, X } from 'lucide-react';
import type { ViewProps } from './props';

export function NewExperiment(
  props: Pick<
    ViewProps,
    | 'act'
    | 'brief'
    | 'busy'
    | 'completed'
    | 'choose'
    | 'contractFourProject'
    | 'currentProject'
    | 'effort'
    | 'error'
    | 'modalRef'
    | 'model'
    | 'projectId'
    | 'referenceId'
    | 'researchLoopInputError'
    | 'setBrief'
    | 'setCreating'
    | 'setEffort'
    | 'setModel'
    | 'setReferenceId'
    | 'setStatus'
    | 'setWorkspaceMode'
    | 'status'
    | 'workspaceMode'
  >,
) {
  const {
    act,
    brief,
    busy,
    completed,
    choose,
    contractFourProject,
    currentProject,
    effort,
    error,
    modalRef,
    model,
    projectId,
    referenceId,
    researchLoopInputError,
    setBrief,
    setCreating,
    setEffort,
    setModel,
    setReferenceId,
    setStatus,
    setWorkspaceMode,
    status,
    workspaceMode,
  } = props;
  return (
    <div
      data-overlay-backdrop
      className="modal-backdrop"
      onClick={() => !busy && setCreating(false)}
    >
      <section
        ref={modalRef}
        className="modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="new-title"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="section-heading">
          <div>
            <h2 id="new-title">New experiment</h2>
            <p className="muted small">{currentProject?.name || projectId}</p>
          </div>
          <button
            className="icon-button"
            aria-label="Close new experiment"
            disabled={busy}
            onClick={() => setCreating(false)}
          >
            <X size={20} />
          </button>
        </div>
        <form
          onSubmit={(e) => {
            e.preventDefault();
            void act(async () => {
              const freshStatus = await loadStatus();
              setStatus(freshStatus);
              // Keep a displayed C4 intent even if the project setting changes mid-form.
              const explicitContractFour =
                contractFourProject ||
                freshStatus.research_loop_projects?.includes(projectId) ||
                freshStatus.execution_contract_create_default === 4;
              if (explicitContractFour) {
                const issue = contractFourCreateIssue(
                  freshStatus,
                  projectId,
                  workspaceMode,
                  referenceId,
                );
                if (issue)
                  throw new Error(
                    `${issue} The explicit contract-4 create was refused; no contract-3 retry was made.`,
                  );
              }
              const receipt = await send('create', null, {
                brief,
                project_id: projectId,
                execution: currentProject?.execution,
                workspace_mode: workspaceMode,
                ...(model ? { model, reasoning_effort: effort } : {}),
                recipe_id: 'agent_experiment_v1',
                ...(explicitContractFour ? { execution_contract: 4 } : {}),
                ...(referenceId ? { reference_experiment_id: referenceId } : {}),
              }).catch((error: unknown) => {
                if (!explicitContractFour) throw error;
                const detail = error instanceof Error ? error.message : String(error);
                throw new Error(
                  `The explicit contract-4 create was refused. No contract-3 retry was made. Check project eligibility and update the gateway if it does not accept this intent. ${detail}`,
                );
              });
              setCreating(false);
              // A hosted create is still pending; its row shows in the list until it applies.
              if (receipt.result?.experiment_id) choose(receipt.result.experiment_id);
            });
          }}
        >
          <p className="brief-guidance" id="brief-guidance">
            {status?.mode === 'demo'
              ? 'Synthetic demonstration. No model calls or Slurm jobs.'
              : contractFourProject
                ? 'Describe the research question, approach, data and evidence that would answer it. The research loop works through the investigation in this project workspace and may pause for your input.'
                : 'Describe what you want to learn. You’ll review the plan before implementation starts.'}
          </p>
          {contractFourProject && researchLoopInputError && (
            <p className="error" role="alert">
              {researchLoopInputError}
            </p>
          )}
          <label htmlFor="brief">Research brief</label>
          <textarea
            id="brief"
            value={brief}
            onChange={(e) => setBrief(e.target.value)}
            minLength={10}
            maxLength={4000}
            required
            rows={5}
            aria-describedby="brief-guidance"
            placeholder="Describe the question, methods, data and outputs for this project."
          />
          <p className="muted execution-summary">
            {status?.models?.find((item) => item.id === model)?.name || 'Waiting for model catalog'}
            {' · '}
            {effort} reasoning{' · '}
            {workspaceMode === 'auto'
              ? 'Automatic working folder isolation'
              : workspaceMode === 'worktree'
                ? 'Separate Git worktree'
                : 'Shared working folder'}
          </p>
          <details className="execution-options">
            <summary>Execution options</summary>
            <p className="muted small execution-location">
              Compute on{' '}
              {currentProject?.execution?.context === 'jean-zay'
                ? `Jean Zay · checked every ${currentProject.execution.poll_seconds / 60} minutes`
                : 'Acacia'}
              . Agents and results stay on acacia.
            </p>
            <label>Working folder</label>
            <Select
              label="Working folder"
              value={workspaceMode}
              options={[
                { value: 'auto', label: 'Automatic isolation when this project is busy' },
                { value: 'worktree', label: 'Always use a separate Git worktree' },
                { value: 'shared', label: 'Shared folder · wait while this project is busy' },
              ]}
              onChange={setWorkspaceMode}
            />
            {currentProject?.supports_worktrees === false && workspaceMode !== 'shared' && (
              <p className="muted">
                This project needs Git with an initial commit for worktree isolation. Initialize Git
                and commit its source to implement parallel experiments; otherwise implementation
                waits while this project is busy.
              </p>
            )}
            <div className="model-fields">
              <div>
                <label>Codex model</label>
                <Select
                  label="Codex model"
                  value={model}
                  disabled={!status?.models?.length}
                  displayValue={!status?.models?.length ? 'Waiting for model catalog' : undefined}
                  options={(status?.models || []).map((m) => ({ value: m.id, label: m.name }))}
                  onChange={(value) => {
                    const choice = status?.models?.find((m) => m.id === value);
                    setModel(value);
                    if (choice)
                      setEffort(
                        choice.reasoning_efforts.includes('medium')
                          ? 'medium'
                          : choice.default_effort,
                      );
                  }}
                />
              </div>
              <div>
                <label>Reasoning effort</label>
                <Select
                  label="Reasoning effort"
                  value={effort}
                  disabled={!model}
                  onChange={setEffort}
                  options={(
                    status?.models?.find((m) => m.id === model)?.reasoning_efforts || [effort]
                  ).map((value) => ({
                    value,
                    label: value.charAt(0).toUpperCase() + value.slice(1),
                  }))}
                />
              </div>
            </div>
            <label>Reference experiment (optional)</label>
            <Select
              label="Reference experiment (optional)"
              value={referenceId}
              onChange={setReferenceId}
              options={[
                { value: '', label: 'No reference' },
                ...completed.map((e) => ({
                  value: e.id,
                  label: experimentTitle(e),
                })),
              ]}
            />
            {referenceId && (
              <p className="muted">
                The saved code, results and interpretation will be included as context.
              </p>
            )}
          </details>
          {status?.mode !== 'demo' && (
            <p className="resource-note">
              Managed Codex · interactive checks and Slurm CPU/GPU stages
            </p>
          )}
          {error && (
            <p role="alert" className="error">
              {error}
            </p>
          )}
          <button
            className="primary full"
            disabled={
              busy || !model || !currentProject?.available || Boolean(researchLoopInputError)
            }
          >
            {busy
              ? 'Submitting…'
              : status?.mode === 'demo'
                ? 'Prepare and run experiment'
                : 'Plan experiment'}
            <ArrowRight size={18} />
          </button>
        </form>
      </section>
    </div>
  );
}
