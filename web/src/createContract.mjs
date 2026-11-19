export function contractFourForProject(status, projectId, nowSeconds = Date.now() / 1000) {
  if (
    status?.execution_contract_create !== 4 ||
    !(
      status.execution_contract_create_default === 4 ||
      (Array.isArray(status.research_loop_projects) &&
        status.research_loop_projects.includes(projectId))
    )
  )
    return false;
  if (status.mode !== 'gateway') return true;
  const observedAt = status.heartbeat_received_at;
  const age = nowSeconds - observedAt;
  return Number.isFinite(observedAt) && age >= 0 && age <= 90;
}

export function contractFourCreateIssue(
  status,
  projectId,
  workspaceMode,
  referenceId,
  nowSeconds = Date.now() / 1000,
) {
  if (!contractFourForProject(status, projectId, nowSeconds))
    return 'Contract 4 is not currently advertised for this project.';
  if (referenceId) return 'Contract 4 does not support reference experiments.';
  if (workspaceMode === 'shared') return 'Contract 4 requires its own named branch workspace.';
  return null;
}
