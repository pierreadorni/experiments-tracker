export function holdTitle(reason: string) {
  const titles: Record<string, string> = {
    model_unavailable: 'Selected model unavailable',
    quota_hold: 'Waiting for subscription quota',
    agent_paused: 'Agent work paused',
    new_compute_paused: 'New compute paused',
    accounting_pending: 'Confirming the job outcome',
    submission_uncertain: 'Checking whether the job was submitted',
    agent_dispatch_uncertain: 'Checking whether agent work started',
    waiting_for_executor: 'Waiting for the executor',
    agent_session_unavailable: 'Experiment conversation unavailable',
    agent_stage_blocked: 'Agent stage needs attention',
    agent_stage_failed: 'Agent stage failed',
    implementation_blocked: 'Implementation needs attention',
    agent_busy: 'Waiting for agent availability',
    compute_busy: 'Waiting for a compute slot',
    project_busy: 'Waiting for this project’s working folder',
    workspace_unavailable: 'Working folder unavailable',
    project_unavailable: 'Project folder unavailable',
    plan_review: 'Your plan review is needed',
    publication_pending: 'Publishing experiment evidence',
    unsupported_request: 'This brief needs different capabilities',
    agent_correction_stalled: 'Agent corrections need your guidance',
    agent_budget_exhausted: 'Agent work reached its configured limit',
  };
  if (reason.startsWith('steering_')) return 'Applying your correction';
  if (reason.startsWith('automatic_repair')) return 'Repairing the experiment';
  return titles[reason] || 'Experiment waiting to continue';
}
export function holdGuidance(reason: string) {
  if (reason.includes('uncertain'))
    return 'Do not resubmit. The saved submission or turn must be checked before work can continue.';
  if (reason === 'plan_review') return 'Review the proposed plan below to continue.';
  if (reason === 'agent_paused' || reason === 'new_compute_paused')
    return 'Use the matching Resume control below when you are ready.';
  if (reason === 'project_unavailable' || reason === 'workspace_unavailable')
    return 'Check this project’s folder and Git setup in Project settings.';
  if (reason === 'agent_session_unavailable')
    return 'Restore access to the saved conversation, then use Retry with agent below.';
  if (reason === 'agent_stage_blocked' || reason === 'agent_stage_failed')
    return 'Resolve the issue described above, then use Retry with agent below.';
  if (reason === 'implementation_blocked')
    return 'Resolve the issue described above, then use Retry with agent below.';
  if (reason === 'agent_correction_stalled')
    return 'Provide a correction below or use Retry with agent when the cause is resolved.';
  if (reason === 'model_unavailable')
    return 'Check the managed runtime model catalog before creating a replacement experiment.';
  if (reason === 'agent_budget_exhausted')
    return 'Review the configured agent limit before requesting more work.';
  if (reason === 'unsupported_request')
    return 'Review the recorded limitation and revise the brief or project inputs.';
  if (
    [
      'quota_hold',
      'agent_busy',
      'compute_busy',
      'project_busy',
      'accounting_pending',
      'waiting_for_executor',
      'publication_pending',
      'agent_continuation',
      'agent_prerequisite',
      'automatic_repair_pending',
      'automatic_repair',
    ].includes(reason) ||
    reason.startsWith('steering_')
  )
    return 'Your work is saved. Progress updates automatically; no resubmission is needed.';
  return 'Review the recorded reason above before taking further action.';
}
export function holdExplanation(reason: string) {
  const explanations: Record<string, string> = {
    model_unavailable:
      'The chosen model or reasoning effort is unavailable in the managed runtime. No alternative model was used.',
    quota_hold:
      'Agent work is waiting for a fresh quota snapshot above the reserve. Compute observation continues.',
    agent_paused: 'Agent work is paused. Running compute continues.',
    new_compute_paused: 'New submissions are paused. Existing scheduler jobs continue.',
    automatic_repair_pending:
      'Preparing automatic repair after the saved jobs and turns are reconciled.',
    automatic_repair: 'Repairing the working implementation and reusing checked successful stages.',
    agent_continuation:
      'Agent continuation queued; previous work is saved. It resumes automatically when quota and the agent slot are available.',
    agent_prerequisite:
      'Waiting to retry an agent prerequisite automatically. No turn has started.',
    agent_correction_stalled:
      'The same correction failed repeatedly. Retry with agent or provide guidance.',
    agent_budget_exhausted: 'The configured agent turn budget is exhausted.',
    accounting_pending: 'The job left the queue; terminal accounting has not arrived yet.',
    submission_uncertain: 'Submission needs reconciliation before any retry.',
    waiting_for_executor: 'Your brief is saved and waiting for the executor.',
    agent_session_unavailable:
      'Codex could not resume this conversation. Unarchive it if needed, then select Retry with agent. No turn was started; other experiments continue.',
    agent_busy: 'Waiting for the current Codex turn or its saved outcome to be reconciled.',
    compute_busy: 'Waiting for a free executor job slot. Other experiments continue.',
    project_busy: 'Implementation is waiting for active experiments in this project to finish.',
    workspace_unavailable:
      'The executor could not create or verify the separate working folder. See the recorded reason.',
    plan_review: 'Review the proposed plan to continue.',
    steering_restart:
      'Your correction is saved. The executor is resolving current work before starting the replacement plan.',
    steering_waiting_for_agent:
      'Waiting for the saved agent turn to finish or be reconciled before restarting.',
    steering_waiting_for_job_identity:
      'The submission outcome must be reconciled before its job can be cancelled.',
    steering_cancelling_job:
      'Cancellation was requested for the recorded job. Waiting for terminal scheduler accounting.',
    steering_cancellation_uncertain:
      'Cancellation could not be confirmed. The executor checks the exact job identity before another attempt.',
    project_unavailable:
      'The saved project folder is missing or changed. Restore the original folder to continue safely.',
    cancellation_pending:
      'Cancellation is saved. Pending transfers are removed; the executor is stopping recorded work.',
    cancellation_unconfirmed:
      'Pending transfers are removed. A recorded job or agent turn still needs its stopped outcome confirmed.',
    publication_pending: 'Evidence is ready; required artifact publication is still pending.',
    unsupported_request:
      'The available recipe does not satisfy this brief. No compute was launched for this request.',
  };
  return explanations[reason] || 'The workflow is held at its recorded stage.';
}
