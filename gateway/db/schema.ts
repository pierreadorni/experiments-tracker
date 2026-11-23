import { sql } from 'drizzle-orm';
import { sqliteTable, text, integer, real, index, uniqueIndex } from 'drizzle-orm/sqlite-core';

export const projections = sqliteTable('projections', {
  id: text('id').primaryKey(),
  revision: integer('revision').notNull(),
  payload: text('payload').notNull(),
  updatedAt: real('updated_at').notNull(),
  deletedAt: real('deleted_at'),
});
export const commands = sqliteTable(
  'commands',
  {
    requestId: text('request_id').primaryKey(),
    experimentId: text('experiment_id')
      .notNull()
      .references(() => projections.id),
    executorId: text('executor_id').notNull(),
    kind: text('kind').notNull(),
    payload: text('payload').notNull(),
    state: text('state').notNull().default('pending'),
    error: text('error'),
    receivedAt: real('received_at').notNull(),
  },
  (t) => [index('idx_commands_executor_state').on(t.executorId, t.state)],
);
export const events = sqliteTable('events', {
  id: text('id').primaryKey(),
  experimentId: text('experiment_id')
    .notNull()
    .references(() => projections.id),
  revision: integer('revision').notNull(),
  receivedAt: real('received_at').notNull(),
});
export const artifacts = sqliteTable(
  'artifacts',
  {
    id: text('id').primaryKey(),
    experimentId: text('experiment_id')
      .notNull()
      .references(() => projections.id),
    name: text('name').notNull(),
    sha256: text('sha256').notNull(),
    size: integer('size').notNull(),
    mediaType: text('media_type').notNull(),
    objectKey: text('object_key').notNull(),
    available: integer('available').notNull().default(0),
  },
  (t) => [index('idx_artifacts_experiment').on(t.experimentId)],
);
export const notes = sqliteTable(
  'notes',
  {
    id: text('id').primaryKey(),
    experimentId: text('experiment_id')
      .notNull()
      .references(() => projections.id),
    author: text('author').notNull(),
    text: text('text').notNull(),
    createdAt: real('created_at').notNull(),
  },
  (t) => [index('idx_notes_experiment').on(t.experimentId)],
);
export const inbox = sqliteTable('inbox', {
  id: text('id').primaryKey(),
  experimentId: text('experiment_id')
    .notNull()
    .references(() => projections.id),
  kind: text('kind').notNull(),
  text: text('text').notNull(),
  createdAt: real('created_at').notNull(),
});
export const executorStatus = sqliteTable('executor_status', {
  id: text('id').primaryKey(),
  payload: text('payload').notNull(),
  receivedAt: real('received_at').notNull(),
});

export const conversation = sqliteTable(
  'conversation',
  {
    id: text('id').primaryKey(),
    experimentId: text('experiment_id')
      .notNull()
      .references(() => projections.id),
    seq: integer('seq').notNull(),
    revision: integer('revision').notNull(),
    payload: text('payload').notNull(),
  },
  (t) => [uniqueIndex('idx_conversation_experiment_seq').on(t.experimentId, t.seq)],
);

export const projectRequests = sqliteTable('project_requests', {
  requestId: text('request_id').primaryKey(),
  projectId: text('project_id').notNull().unique(),
  executorId: text('executor_id').notNull(),
  payload: text('payload').notNull(),
  state: text('state').notNull().default('pending'),
  error: text('error'),
  result: text('result'),
  receivedAt: real('received_at').notNull(),
});

export const settingsCommands = sqliteTable('settings_commands', {
  requestId: text('request_id').primaryKey(),
  executorId: text('executor_id').notNull(),
  kind: text('kind').notNull(),
  payload: text('payload').notNull(),
  state: text('state').notNull().default('pending'),
  error: text('error'),
  receivedAt: real('received_at').notNull(),
});

export const projectSettingsRequests = sqliteTable('project_settings_requests', {
  requestId: text('request_id').primaryKey(),
  projectId: text('project_id').notNull(),
  executorId: text('executor_id').notNull(),
  payload: text('payload').notNull(),
  state: text('state').notNull().default('pending'),
  error: text('error'),
  result: text('result'),
  receivedAt: real('received_at').notNull(),
});

export const pushSubscriptions = sqliteTable('push_subscriptions', {
  id: text('id').primaryKey(),
  owner: text('owner').notNull(),
  subscription: text('subscription').notNull(),
  updatedAt: real('updated_at').notNull(),
});
// Unused since T5.2f: follows live in relay_follows. Kept so drizzle-kit generates no DROP.
export const pushFollows = sqliteTable(
  'push_follows',
  {
    subscriptionId: text('subscription_id')
      .notNull()
      .references(() => pushSubscriptions.id, { onDelete: 'cascade' }),
    experimentId: text('experiment_id')
      .notNull()
      .references(() => projections.id),
    createdAt: real('created_at').notNull(),
  },
  (t) => [uniqueIndex('idx_push_follows_identity').on(t.subscriptionId, t.experimentId)],
);
export const pushReceipts = sqliteTable('push_receipts', {
  requestId: text('request_id').primaryKey(),
  owner: text('owner').notNull(),
  payloadHash: text('payload_hash').notNull(),
});
// Unused since T5.2f: deliveries live in relay_push_outbox. Kept so drizzle-kit generates no DROP.
export const pushOutbox = sqliteTable(
  'push_outbox',
  {
    id: text('id').primaryKey(),
    subscriptionId: text('subscription_id')
      .notNull()
      .references(() => pushSubscriptions.id, { onDelete: 'cascade' }),
    experimentId: text('experiment_id')
      .notNull()
      .references(() => projections.id),
    revision: integer('revision').notNull(),
    payload: text('payload').notNull(),
    state: text('state').notNull().default('pending'),
    attempts: integer('attempts').notNull().default(0),
    nextAttemptAt: real('next_attempt_at').notNull(),
    lease: text('lease'),
    lastStatus: integer('last_status'),
    createdAt: real('created_at').notNull(),
  },
  (t) => [index('idx_push_outbox_pending').on(t.state, t.nextAttemptAt)],
);

export const projectMemory = sqliteTable('project_memory', {
  projectId: text('project_id').primaryKey(),
  executorId: text('executor_id').notNull(),
  revision: integer('revision').notNull(),
  payload: text('payload').notNull(),
});
export const projectMemoryCommands = sqliteTable('project_memory_commands', {
  requestId: text('request_id').primaryKey(),
  projectId: text('project_id').notNull(),
  executorId: text('executor_id').notNull(),
  payload: text('payload').notNull(),
  state: text('state').notNull().default('pending'),
  error: text('error'),
  receivedAt: real('received_at').notNull(),
});
export const projectMemoryReports = sqliteTable('project_memory_reports', {
  identity: text('identity').primaryKey(),
  projectId: text('project_id').notNull(),
  version: integer('version').notNull(),
  payload: text('payload').notNull(),
});

export const maintenanceStatus = sqliteTable('maintenance_status', {
  executorId: text('executor_id').primaryKey(),
  payload: text('payload').notNull(),
  observedAt: real('observed_at').notNull(),
});
export const maintenanceCommands = sqliteTable(
  'maintenance_commands',
  {
    requestId: text('request_id').primaryKey(),
    executorId: text('executor_id').notNull(),
    releaseId: text('release_id').notNull(),
    state: text('state').notNull().default('pending'),
    error: text('error'),
    receivedAt: real('received_at').notNull(),
  },
  (t) => [
    uniqueIndex('one_pending_maintenance')
      .on(t.executorId)
      .where(sql`${t.state} = 'pending'`),
  ],
);

export const autopilotViews = sqliteTable('autopilot_views', {
  projectId: text('project_id').primaryKey(),
  executorId: text('executor_id').notNull(),
  runId: text('run_id').notNull(),
  revision: integer('revision').notNull(),
  payload: text('payload').notNull(),
  observedAt: real('observed_at').notNull(),
});
export const autopilotCommands = sqliteTable('autopilot_commands', {
  requestId: text('request_id').primaryKey(),
  projectId: text('project_id').notNull(),
  executorId: text('executor_id').notNull(),
  payload: text('payload').notNull(),
  state: text('state').notNull().default('pending'),
  error: text('error'),
  receivedAt: real('received_at').notNull(),
});
export const autopilotDocuments = sqliteTable('autopilot_documents', {
  identity: text('identity').primaryKey(),
  projectId: text('project_id').notNull(),
  runId: text('run_id').notNull(),
  name: text('name').notNull(),
  version: integer('version').notNull(),
  objectKey: text('object_key').notNull(),
});

// Relay tables (T5.2a): one document store and one command queue.
export const relayProjections = sqliteTable('relay_projections', {
  key: text('key').primaryKey(),
  executorId: text('executor_id').notNull(),
  revision: integer('revision').notNull(),
  payload: text('payload'),
  digest: text('digest').notNull(),
  notifyFingerprint: text('notify_fingerprint'),
  updatedAt: real('updated_at').notNull(),
});
export const relayCommands = sqliteTable(
  'relay_commands',
  {
    requestId: text('request_id').primaryKey(),
    executorId: text('executor_id').notNull(),
    kind: text('kind').notNull(),
    target: text('target'),
    payload: text('payload').notNull(),
    state: text('state').notNull().default('pending'),
    result: text('result'),
    error: text('error'),
    receivedAt: real('received_at').notNull(),
    settledAt: real('settled_at'),
  },
  (t) => [index('idx_relay_commands_state').on(t.executorId, t.state, t.receivedAt)],
);
export const relayFollows = sqliteTable(
  'relay_follows',
  {
    subscriptionId: text('subscription_id')
      .notNull()
      .references(() => pushSubscriptions.id, { onDelete: 'cascade' }),
    key: text('key').notNull(),
    createdAt: real('created_at').notNull(),
  },
  (t) => [uniqueIndex('idx_relay_follows_identity').on(t.subscriptionId, t.key)],
);
export const relayPushOutbox = sqliteTable(
  'relay_push_outbox',
  {
    id: text('id').primaryKey(),
    subscriptionId: text('subscription_id')
      .notNull()
      .references(() => pushSubscriptions.id, { onDelete: 'cascade' }),
    key: text('key').notNull(),
    revision: integer('revision').notNull(),
    payload: text('payload').notNull(),
    state: text('state').notNull().default('pending'),
    attempts: integer('attempts').notNull().default(0),
    nextAttemptAt: real('next_attempt_at').notNull(),
    lease: text('lease'),
    lastStatus: integer('last_status'),
    createdAt: real('created_at').notNull(),
  },
  (t) => [index('idx_relay_push_outbox_pending').on(t.state, t.nextAttemptAt)],
);
