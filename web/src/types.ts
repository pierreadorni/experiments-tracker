export type Artifact = {
  stage_id: string;
  id: string;
  name: string;
  size: number;
  sha256: string;
  media_type: string;
  local_valid: boolean;
  remote_published: boolean;
};
export type Stage = { id: string; state: string; error: string | null; updated_at: number };
export type Job = {
  tensorboard?: {
    scalar: { tag: string; points: [number, number][] } | null;
    image: { tag: string; step: number; wall_time: number; src: string } | null;
    image_pending?: boolean;
    updated_at: number;
    limited: boolean;
  } | null;
  progress?: {
    completed: number;
    total: number;
    reported_at: number;
    eta_seconds: number | null;
  } | null;
  created_at: number;
  started_at?: number | null;
  ended_at?: number | null;
  estimated_start_at?: number | null;
  pending_reason?: string | null;
  error?: string | null;
  intent_id: string;
  stage_id: string;
  job_id: string | null;
  scheduler_state: string | null;
  state: string;
  exit_code: string | null;
  observed_at: number | null;
  resources: {
    cpus_per_job: number;
    memory_mb_per_job: number;
    walltime_minutes_per_job: number;
    gpus_per_job: number;
    partition?: string;
    gpu_type?: string | null;
    nodes?: number;
    tasks_per_node?: number;
  };
};
export type ReportVersion = {
  version: number;
  state: 'queued' | 'building' | 'publishing' | 'ready' | 'failed';
  artifact_ids: string[];
  report_artifact_id: string | null;
  error: string | null;
  created_at: number;
  updated_at: number;
};
export type ExecutionSettings = {
  context: 'acacia' | 'jean-zay';
  ssh_host: string;
  remote_project: string;
  rsync_up: string;
  rsync_down: string;
  poll_seconds: number;
};
export const defaultExecution: ExecutionSettings = {
  context: 'acacia',
  ssh_host: 'jz',
  remote_project: '',
  rsync_up: '',
  rsync_down: '',
  poll_seconds: 600,
};
export type Project = {
  execution?: ExecutionSettings;
  id: string;
  name: string;
  available: boolean;
  writable?: boolean;
  non_empty?: boolean;
  supports_worktrees?: boolean;
  folder?: string;
  instructions?: string;
  revision?: number;
};
export type ProjectReceipt = {
  schema_version: 1;
  request_id: string;
  project_id: string;
  state: 'waiting_for_executor' | 'applied' | 'rejected';
  error: string | null;
  project: Project | null;
};
export type Experiment = {
  partial_results?: {
    tasks: Record<
      string,
      {
        intent_id: string;
        metric_count?: number;
        metrics_truncated?: boolean;
        result: {
          protocol?: string;
          metrics: { name: string; variant: string; value: number; unit: string }[];
          sample_count?: number | null;
        };
      }
    >;
  } | null;
  output_refs?: { id: string; intent_id: string; name: string; kind: string; created_at: number }[];
  title?: string | null;
  plan_review?: {
    questions?: string[];
    scope?: 'plan' | 'implementation';
    state: 'drafting' | 'awaiting_review' | 'approved' | 'cancelled' | 'revision_requested';
    summary: string;
    protocol: string;
    schema_version?: 1 | 2 | 3;
    stages: {
      id: string;
      title: string;
      summary?: string;
      resources?: string | Record<string, unknown> | null;
      needs?: string[] | null;
    }[];
    revision: number;
    feedback?: string | null;
  } | null;
  replaces_experiment_id?: string | null;
  steering?: {
    request_id: string;
    text: string;
    mode: 'later' | 'restart';
    state: string;
    replacement_experiment_id: string | null;
  }[];
  project_id?: string;
  conversation?: Discussion[];
  conversation_total?: number;
  pending_messages?: { id: string; text: string; created_at: number }[];
  active_report_id?: string | null;
  report_versions?: ReportVersion[];
  deleted_at?: number | null;
  stage_activity?: {
    stage_id: string;
    kind: string;
    summary: string;
    state: string;
    created_at: number;
    updated_at: number;
  }[];
  reference_experiment_id?: string | null;
  pending_commands?: {
    request_id: string;
    kind: string;
    action: string | null;
    received_at: number;
  }[];
  id: string;
  brief: string;
  recipe_id: string;
  phase: string;
  hold_reason: string | null;
  revision: number;
  evidence_revision: number;
  created_at: number;
  updated_at: number;
  observed_at: number | null;
  pause_agent: boolean;
  pause_compute: boolean;
  error: string | null;
  model: string | null;
  scientific_outcome: string | null;
  stages: Stage[];
  jobs: Job[];
  artifacts: Artifact[];
  notes: { id: string; author: string; text: string; created_at: number }[];
  events: { seq: number; kind: string; created_at: number; payload: string }[];
  agent_actions: {
    id: string;
    phase: string;
    state: string;
    thread_id: string | null;
    turn_id: string | null;
    error: string | null;
  }[];
  pending_request: {
    id: string;
    method: string;
    params: unknown;
    expired: boolean;
    answer?: string;
  } | null;
  analysis: {
    title: string;
    summary: string;
    observations: { text: string; evidence_ids: string[] }[];
    limitations: string[];
    next_ideas: string[];
  } | null;
  contract: Record<string, unknown>;
  snapshot: Record<string, unknown> | null;
};
export type Discussion = {
  id: string;
  seq: number;
  text: string;
  model: string | null;
  reasoning_effort: string | null;
  state: 'queued' | 'waiting' | 'running' | 'completed' | 'failed' | 'uncertain';
  hold_reason: string | null;
  error: string | null;
  reply: string | null;
  evidence_ids: string[];
  report_version: number | null;
  artifact_ids?: string[];
  followup_experiment_id?: string | null;
  created_at: number;
  updated_at: number;
  activity: { summary: string; state: string }[];
};
export type ModelOption = {
  id: string;
  name: string;
  reasoning_efforts: string[];
  default_effort: string;
};
export type Status = {
  quota_reserve?: { five_hour_percent: number; weekly_percent: number };
  compute_policy?: {
    agent_selected_resources: boolean;
    max_jobs_per_experiment: number | null;
    max_concurrent_jobs: number | null;
    defaults: Record<string, unknown>;
    limits: Record<string, unknown>;
  };
  cluster?: {
    observed_at: number;
    source: string;
    nodes: string;
    partitions: string;
    associations: string;
    errors: Record<string, string>;
  } | null;
  cluster_error?: string | null;
  projects?: Project[];
  models?: ModelOption[];
  default_model?: string | null;
  models_observed_at?: number | null;
  job_observations?: Record<string, { job_id: string | null; observed_at: number | null }>;
  mode: 'demo' | 'live' | 'gateway';
  executor_id: string;
  last_tick: number | null;
  agent_ready: boolean;
  agent_error: string | null;
  quota: {
    admitted: boolean;
    reason: string | null;
    remaining: Record<string, number>;
    observed_at: number | null;
  };
  auth_mode?: 'sites' | 'token';
  gateway_configured?: boolean;
};
export type InboxEntry = {
  id: string;
  experiment_id: string;
  kind: string;
  text: string;
  created_at: number;
};
