export function contractFourCreateIssue(
  status: {
    execution_contract_create?: number;
    research_loop_projects?: string[];
    mode?: string;
    heartbeat_received_at?: number | null;
  } | null,
  projectId: string,
  workspaceMode: string,
  referenceId: string,
  nowSeconds?: number,
): string | null;
export function contractFourForProject(
  status: {
    execution_contract_create?: number;
    research_loop_projects?: string[];
    mode?: string;
    heartbeat_received_at?: number | null;
  } | null,
  projectId: string,
  nowSeconds?: number,
): boolean;
