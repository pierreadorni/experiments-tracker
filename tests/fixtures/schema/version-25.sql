-- Synthetic database at schema version 25 (T5.5b). Frozen: never regenerate; every later migration must upgrade it.
BEGIN TRANSACTION;
CREATE TABLE "agent_actions"(
                        id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL REFERENCES experiments(id),
                        phase TEXT NOT NULL, evidence_revision INTEGER NOT NULL, state TEXT NOT NULL,
                        thread_id TEXT, turn_id TEXT, input TEXT NOT NULL, result TEXT, error TEXT,
                        attempts INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL, updated_at REAL NOT NULL,
                        previous_action_id TEXT UNIQUE REFERENCES agent_actions(id), interrupt_reason TEXT, autopilot_control_revision INTEGER);
INSERT INTO "agent_actions" VALUES('00000000-0000-4000-8000-0000000000b1','00000000-0000-4000-8000-000000000001','research',1,'completed','thread-b1','turn-b1','{}',NULL,NULL,0,0.0,0.0,NULL,NULL,NULL);
CREATE TABLE agent_usage(
                    action_id TEXT PRIMARY KEY REFERENCES agent_actions(id),
                    thread_id TEXT NOT NULL, turn_id TEXT NOT NULL,
                    payload TEXT NOT NULL, observed_at REAL NOT NULL);
CREATE TABLE agent_wakeups(
                        id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL REFERENCES experiments(id),
                        reason TEXT NOT NULL, due_at REAL NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
                        action_id TEXT REFERENCES agent_actions(id), created_at REAL NOT NULL);
CREATE TABLE artifact_registrations(
                    artifact_id TEXT PRIMARY KEY REFERENCES artifacts(id),
                    event_id TEXT NOT NULL REFERENCES events(id),
                    sha256 TEXT NOT NULL, size INTEGER NOT NULL);
CREATE TABLE artifacts(
          id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL REFERENCES experiments(id), stage_id TEXT NOT NULL,
          path TEXT NOT NULL, name TEXT NOT NULL, sha256 TEXT NOT NULL, size INTEGER NOT NULL,
          media_type TEXT NOT NULL, evidence_revision INTEGER NOT NULL, local_valid INTEGER NOT NULL,
          remote_published INTEGER NOT NULL DEFAULT 0, UNIQUE(experiment_id,name,evidence_revision));
CREATE TABLE autopilot_accounting(
            intent_id TEXT NOT NULL REFERENCES autopilot_allocations(intent_id), observed_at REAL NOT NULL,
            cluster TEXT NOT NULL, job_id TEXT NOT NULL, document TEXT NOT NULL,
            PRIMARY KEY(intent_id,observed_at));
CREATE TABLE autopilot_actions(
            id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES autopilot_runs(id), cursor INTEGER NOT NULL,
            control_revision INTEGER NOT NULL, state TEXT NOT NULL, input TEXT, result TEXT, error TEXT,
            thread_id TEXT, turn_id TEXT, interrupt_reason TEXT, observed_at REAL, pending_request TEXT,
            created_at REAL NOT NULL, updated_at REAL NOT NULL);
INSERT INTO "autopilot_actions" VALUES('00000000-0000-4000-8000-0000000000d1','00000000-0000-4000-8000-0000000000d0',0,0,'completed',NULL,NULL,NULL,'thread-d1','turn-d1',NULL,NULL,NULL,0.0,0.0);
CREATE TABLE autopilot_allocations(
            intent_id TEXT PRIMARY KEY REFERENCES jobs(intent_id), project_id TEXT NOT NULL,
            run_id TEXT NOT NULL REFERENCES autopilot_runs(id), requested_gpus INTEGER NOT NULL,
            reserved_seconds INTEGER NOT NULL, state TEXT NOT NULL DEFAULT 'reserved',
            cluster TEXT, job_id TEXT, started_at REAL, accounted_until REAL, allocated_gpus INTEGER,
            source TEXT, observed_at REAL, anomaly TEXT);
CREATE TABLE autopilot_backlog(
            run_id TEXT NOT NULL REFERENCES autopilot_runs(id), id TEXT NOT NULL, revision INTEGER NOT NULL,
            document TEXT NOT NULL, PRIMARY KEY(run_id,id));
CREATE TABLE autopilot_decisions(
            id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES autopilot_runs(id), author TEXT NOT NULL,
            action_id TEXT, policy_revision INTEGER NOT NULL, document TEXT NOT NULL, created_at REAL NOT NULL);
CREATE TABLE autopilot_deliveries(identity TEXT PRIMARY KEY, delivered_at REAL NOT NULL);
CREATE TABLE autopilot_documents(
            run_id TEXT NOT NULL REFERENCES autopilot_runs(id), name TEXT NOT NULL, version INTEGER NOT NULL,
            request_id TEXT NOT NULL, author TEXT NOT NULL, action_id TEXT, policy_revision INTEGER NOT NULL,
            content TEXT NOT NULL, sources TEXT NOT NULL, created_at REAL NOT NULL,
            PRIMARY KEY(run_id,name,version), UNIQUE(request_id,name));
CREATE TABLE autopilot_events(
            id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES autopilot_runs(id), revision INTEGER NOT NULL,
            kind TEXT NOT NULL, payload TEXT NOT NULL, created_at REAL NOT NULL, delivered_at REAL,
            UNIQUE(run_id,revision));
CREATE TABLE autopilot_files(id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES autopilot_runs(id), request_id TEXT NOT NULL UNIQUE, name TEXT NOT NULL, path TEXT NOT NULL, size INTEGER NOT NULL, created_at REAL NOT NULL);
CREATE TABLE autopilot_inputs(
            run_id TEXT NOT NULL REFERENCES autopilot_runs(id), seq INTEGER NOT NULL, identity TEXT NOT NULL,
            kind TEXT NOT NULL, document TEXT NOT NULL, created_at REAL NOT NULL,
            PRIMARY KEY(run_id,seq), UNIQUE(run_id,identity));
CREATE TABLE autopilot_members(
            experiment_id TEXT PRIMARY KEY REFERENCES experiments(id), run_id TEXT NOT NULL REFERENCES autopilot_runs(id),
            project_id TEXT NOT NULL, backlog_id TEXT NOT NULL, purpose TEXT NOT NULL, rationale TEXT NOT NULL,
            priority INTEGER NOT NULL, policy_revision INTEGER NOT NULL, deferred INTEGER NOT NULL DEFAULT 0,
            defer_reason TEXT, deferred_stages TEXT NOT NULL DEFAULT '[]', created_at REAL NOT NULL);
CREATE TABLE autopilot_notices(
            id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES autopilot_runs(id), kind TEXT NOT NULL,
            text TEXT NOT NULL, resolved_at REAL, created_at REAL NOT NULL);
CREATE TABLE autopilot_policies(
            run_id TEXT NOT NULL REFERENCES autopilot_runs(id), revision INTEGER NOT NULL,
            policy TEXT NOT NULL, author TEXT NOT NULL, created_at REAL NOT NULL,
            PRIMARY KEY(run_id,revision));
CREATE TABLE autopilot_requests(
            request_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, run_id TEXT,
            payload TEXT NOT NULL, author TEXT NOT NULL, state TEXT NOT NULL,
            result TEXT, error TEXT, created_at REAL NOT NULL);
CREATE TABLE autopilot_runs(
            id TEXT PRIMARY KEY, project_id TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'draft',
            revision INTEGER NOT NULL DEFAULT 0, policy_revision INTEGER NOT NULL DEFAULT 1,
            control_revision INTEGER NOT NULL DEFAULT 0, phase TEXT NOT NULL DEFAULT 'foundation',
            foundation TEXT NOT NULL DEFAULT 'pending', foundation_refs TEXT NOT NULL DEFAULT '{}',
            borrowed_percent REAL NOT NULL DEFAULT 0, input_cursor INTEGER NOT NULL DEFAULT 0,
            consumed_cursor INTEGER NOT NULL DEFAULT 0, next_report_at REAL, next_review_at REAL,
            hold_reason TEXT, central_argument TEXT, uncertainty TEXT, handoff TEXT,
            created_at REAL NOT NULL, updated_at REAL NOT NULL);
INSERT INTO "autopilot_runs" VALUES('00000000-0000-4000-8000-0000000000d0','test-project','stopped',0,1,0,'foundation','pending','{}',0.0,0,0,NULL,NULL,NULL,NULL,NULL,NULL,0.0,0.0);
CREATE TABLE autopilot_stage_holds(
            experiment_id TEXT NOT NULL REFERENCES experiments(id), stage_id TEXT NOT NULL,
            attempt INTEGER NOT NULL, reason TEXT NOT NULL, detail TEXT NOT NULL,
            updated_at REAL NOT NULL, PRIMARY KEY(experiment_id,stage_id,attempt));
CREATE TABLE autopilot_transitions(
            run_id TEXT NOT NULL REFERENCES autopilot_runs(id), policy_revision INTEGER NOT NULL,
            boundary TEXT NOT NULL, nominal_at REAL NOT NULL, observed_at REAL NOT NULL, gaps TEXT NOT NULL,
            PRIMARY KEY(run_id,policy_revision,boundary));
CREATE TABLE commands(
          request_id TEXT PRIMARY KEY, experiment_id TEXT REFERENCES experiments(id), kind TEXT NOT NULL,
          origin TEXT NOT NULL, payload TEXT NOT NULL, state TEXT NOT NULL, result TEXT NOT NULL,
          received_at REAL NOT NULL);
CREATE TABLE discussion_threads(
                    experiment_id TEXT PRIMARY KEY REFERENCES experiments(id), thread_id TEXT NOT NULL);
CREATE TABLE discussions(
                    id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL REFERENCES experiments(id),
                    seq INTEGER NOT NULL, text TEXT NOT NULL, model TEXT, reasoning_effort TEXT,
                    state TEXT NOT NULL DEFAULT 'queued', hold_reason TEXT, error TEXT, reply TEXT,
                    evidence_ids TEXT NOT NULL DEFAULT '[]', report_version INTEGER,
                    action_id TEXT UNIQUE REFERENCES agent_actions(id),
                    created_at REAL NOT NULL, updated_at REAL NOT NULL, artifact_ids TEXT NOT NULL DEFAULT '[]', followup_experiment_id TEXT REFERENCES experiments(id),
                    UNIQUE(experiment_id,seq));
CREATE TABLE events(
          seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE,
          experiment_id TEXT NOT NULL REFERENCES experiments(id), kind TEXT NOT NULL,
          revision INTEGER NOT NULL, payload TEXT NOT NULL, created_at REAL NOT NULL);
CREATE TABLE execution_transfers(
                    id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL REFERENCES experiments(id),
                    direction TEXT NOT NULL, payload TEXT NOT NULL, state TEXT NOT NULL,
                    result TEXT, updated_at REAL NOT NULL);
CREATE TABLE experiment_branches(
                    experiment_id TEXT PRIMARY KEY REFERENCES experiments(id),
                    repo TEXT NOT NULL, branch TEXT NOT NULL, worktree TEXT NOT NULL,
                    base_commit TEXT NOT NULL, head_commit TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('active','integrating','merged','abandoned')),
                    merged_commit TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL);
CREATE TABLE experiment_summaries(
        experiment_id TEXT PRIMARY KEY REFERENCES experiments(id), project_id TEXT NOT NULL,
        fingerprint TEXT NOT NULL, payload TEXT NOT NULL);
CREATE TABLE experiment_workspaces(
                    experiment_id TEXT PRIMARY KEY REFERENCES experiments(id),
                    folder TEXT NOT NULL, device INTEGER NOT NULL, inode INTEGER NOT NULL,
                    root TEXT NOT NULL UNIQUE);
CREATE TABLE experiments(
          id TEXT PRIMARY KEY, brief TEXT NOT NULL, recipe_id TEXT NOT NULL, contract TEXT NOT NULL,
          phase TEXT NOT NULL DEFAULT 'preparing', hold_reason TEXT, revision INTEGER NOT NULL DEFAULT 0,
          evidence_revision INTEGER NOT NULL DEFAULT 1, created_at REAL NOT NULL, updated_at REAL NOT NULL,
          observed_at REAL, thread_id TEXT, model TEXT, snapshot TEXT,
          pause_agent INTEGER NOT NULL DEFAULT 0, pause_compute INTEGER NOT NULL DEFAULT 0,
          pending_request TEXT, analysis TEXT, error TEXT, scientific_outcome TEXT, reference_experiment_id TEXT REFERENCES experiments(id), deleted_at REAL, title TEXT, plan_review TEXT, replaces_experiment_id TEXT REFERENCES experiments(id), partial_results TEXT);
INSERT INTO "experiments" VALUES('00000000-0000-4000-8000-000000000001','Synthetic schema fixture','agent_experiment_v1','{"execution_contract":4}','preparing',NULL,0,1,0.0,0.0,NULL,NULL,NULL,NULL,1,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "experiments" VALUES('00000000-0000-4000-8000-000000000002','Synthetic schema fixture','agent_experiment_v1','{"execution_contract":4}','preparing',NULL,0,1,0.0,0.0,NULL,NULL,NULL,NULL,0,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
CREATE TABLE inbox(
          id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL REFERENCES experiments(id),
          kind TEXT NOT NULL, text TEXT NOT NULL, created_at REAL NOT NULL,
          UNIQUE(experiment_id,kind,text));
CREATE TABLE isolated_workspaces(
                    experiment_id TEXT PRIMARY KEY REFERENCES experiments(id),
                    source TEXT NOT NULL, folder TEXT NOT NULL UNIQUE, head TEXT NOT NULL,
                    baseline TEXT NOT NULL, common_dir TEXT NOT NULL, git_device INTEGER NOT NULL,
                    git_inode INTEGER NOT NULL, state TEXT NOT NULL, device INTEGER, inode INTEGER,
                    created_at REAL NOT NULL);
CREATE TABLE jobs(
          intent_id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL REFERENCES experiments(id),
          stage_id TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 1, state TEXT NOT NULL,
          job_id TEXT, script_hash TEXT NOT NULL, resources TEXT NOT NULL, snapshot_hash TEXT NOT NULL,
          created_at REAL NOT NULL, observed_at REAL, scheduler_state TEXT, exit_code TEXT, error TEXT, started_at REAL, ended_at REAL, estimated_start_at REAL, pending_reason TEXT, progress TEXT, execution_root TEXT, execution_manifest TEXT, remote_polled_at REAL, tensorboard TEXT,
          UNIQUE(experiment_id,stage_id,attempt),
          FOREIGN KEY(experiment_id,stage_id) REFERENCES stages(experiment_id,id));
INSERT INTO "jobs" VALUES('00000000-0000-4000-8000-0000000000a1','00000000-0000-4000-8000-000000000001','train',1,'submitted','424242','source','{}','snapshot',0.0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
CREATE TABLE maintenance(
                    request_id TEXT PRIMARY KEY, release_id TEXT NOT NULL REFERENCES releases(commit_id),
                    state TEXT NOT NULL, error TEXT, detail TEXT NOT NULL DEFAULT '{}',
                    created_at REAL NOT NULL, updated_at REAL NOT NULL);
CREATE TABLE notes(
          id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL REFERENCES experiments(id),
          author TEXT NOT NULL, text TEXT NOT NULL, created_at REAL NOT NULL);
CREATE TABLE obligations(
                    id TEXT PRIMARY KEY, experiment_id TEXT REFERENCES experiments(id), project_id TEXT,
                    kind TEXT NOT NULL, target TEXT,
                    owner TEXT NOT NULL CHECK(owner IN ('backend','agent','owner','external')),
                    wait TEXT,
                    state TEXT NOT NULL CHECK(state IN ('open','satisfied','abandoned')),
                    detail TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL, due_at REAL);
CREATE TABLE outbox(
          id TEXT PRIMARY KEY REFERENCES events(id), payload TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
          delivered_at REAL, next_attempt_at REAL NOT NULL DEFAULT 0, error TEXT);
CREATE TABLE output_bindings(
                    consumer_intent TEXT NOT NULL REFERENCES jobs(intent_id),
                    output_id TEXT NOT NULL REFERENCES output_refs(id), rationale TEXT NOT NULL,
                    PRIMARY KEY(consumer_intent,output_id));
CREATE TABLE output_refs(
                    id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL REFERENCES experiments(id),
                    intent_id TEXT NOT NULL REFERENCES jobs(intent_id), name TEXT NOT NULL,
                    context TEXT NOT NULL, host TEXT NOT NULL, path TEXT NOT NULL, kind TEXT NOT NULL,
                    created_at REAL NOT NULL, UNIQUE(intent_id,name));
CREATE TABLE project_knowledge(
        project_id TEXT PRIMARY KEY, revision INTEGER NOT NULL DEFAULT 0,
        weekly INTEGER NOT NULL DEFAULT 1, next_due REAL NOT NULL,
        updated_at REAL NOT NULL);
CREATE TABLE project_memory_events(
        project_id TEXT NOT NULL, revision INTEGER NOT NULL, kind TEXT NOT NULL,
        payload TEXT NOT NULL, created_at REAL NOT NULL, delivered_at REAL,
        PRIMARY KEY(project_id,revision));
CREATE TABLE project_memory_rejections(
        request_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, payload TEXT NOT NULL,
        error TEXT NOT NULL);
CREATE TABLE project_preferences(
                    project_id TEXT PRIMARY KEY, instructions TEXT NOT NULL DEFAULT '',
                    revision INTEGER NOT NULL DEFAULT 0, updated_at REAL NOT NULL, execution TEXT NOT NULL DEFAULT '{}');
CREATE TABLE project_reports(
        project_id TEXT NOT NULL, version INTEGER NOT NULL, request_id TEXT NOT NULL UNIQUE,
        document TEXT NOT NULL, sources TEXT NOT NULL, created_at REAL NOT NULL,
        PRIMARY KEY(project_id,version));
CREATE TABLE project_requests(
                    request_id TEXT PRIMARY KEY, project_id TEXT NOT NULL UNIQUE, payload TEXT NOT NULL,
                    folder TEXT, state TEXT NOT NULL DEFAULT 'pending', error TEXT, result TEXT,
                    created_at REAL NOT NULL, delivered_at REAL, origin TEXT NOT NULL DEFAULT 'local');
CREATE TABLE project_settings_requests(
                    request_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, payload TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'pending', error TEXT, result TEXT,
                    created_at REAL NOT NULL, delivered_at REAL, origin TEXT NOT NULL DEFAULT 'local');
CREATE TABLE project_turns(
        id TEXT PRIMARY KEY, project_id TEXT NOT NULL, payload TEXT NOT NULL, origin TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'queued', hold_reason TEXT, error TEXT,
        input TEXT, thread_id TEXT, turn_id TEXT, result TEXT, reply TEXT,
        report_version INTEGER, created_at REAL NOT NULL, updated_at REAL NOT NULL);
INSERT INTO "project_turns" VALUES('00000000-0000-4000-8000-0000000000c1','test-project','{}','owner','completed',NULL,NULL,NULL,'thread-c1','turn-c1',NULL,NULL,NULL,0.0,0.0);
CREATE TABLE projects(
                    id TEXT PRIMARY KEY, name TEXT NOT NULL, folder TEXT NOT NULL UNIQUE,
                    device INTEGER NOT NULL, inode INTEGER NOT NULL, non_empty INTEGER NOT NULL,
                    created_at REAL NOT NULL);
CREATE TABLE quota_settings(
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    five_hour_percent INTEGER NOT NULL, weekly_percent INTEGER NOT NULL,
                    request_id TEXT NOT NULL UNIQUE, updated_at REAL NOT NULL);
CREATE TABLE quota_snapshots(
          id INTEGER PRIMARY KEY AUTOINCREMENT, observed_at REAL NOT NULL, payload TEXT NOT NULL,
          admitted INTEGER NOT NULL, reason TEXT);
CREATE TABLE rejected_gateway_commands(
                    request_id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL, kind TEXT NOT NULL,
                    payload TEXT NOT NULL, error TEXT NOT NULL, received_at REAL NOT NULL);
CREATE TABLE releases(
                    commit_id TEXT PRIMARY KEY, summary TEXT NOT NULL, state TEXT NOT NULL,
                    receipt TEXT, error TEXT, updated_at REAL NOT NULL);
CREATE TABLE report_versions(
                    experiment_id TEXT NOT NULL REFERENCES experiments(id), version INTEGER NOT NULL,
                    state TEXT NOT NULL, document TEXT NOT NULL, request_id TEXT NOT NULL UNIQUE,
                    artifact_ids TEXT NOT NULL DEFAULT '[]', report_artifact_id TEXT, error TEXT,
                    created_at REAL NOT NULL, updated_at REAL NOT NULL,
                    PRIMARY KEY(experiment_id,version));
CREATE TABLE schema_version(version INTEGER PRIMARY KEY);
INSERT INTO "schema_version" VALUES(1);
INSERT INTO "schema_version" VALUES(2);
INSERT INTO "schema_version" VALUES(3);
INSERT INTO "schema_version" VALUES(4);
INSERT INTO "schema_version" VALUES(5);
INSERT INTO "schema_version" VALUES(6);
INSERT INTO "schema_version" VALUES(7);
INSERT INTO "schema_version" VALUES(8);
INSERT INTO "schema_version" VALUES(9);
INSERT INTO "schema_version" VALUES(10);
INSERT INTO "schema_version" VALUES(11);
INSERT INTO "schema_version" VALUES(12);
INSERT INTO "schema_version" VALUES(13);
INSERT INTO "schema_version" VALUES(14);
INSERT INTO "schema_version" VALUES(15);
INSERT INTO "schema_version" VALUES(16);
INSERT INTO "schema_version" VALUES(17);
INSERT INTO "schema_version" VALUES(18);
INSERT INTO "schema_version" VALUES(19);
INSERT INTO "schema_version" VALUES(20);
INSERT INTO "schema_version" VALUES(21);
INSERT INTO "schema_version" VALUES(22);
INSERT INTO "schema_version" VALUES(23);
INSERT INTO "schema_version" VALUES(24);
INSERT INTO "schema_version" VALUES(25);
CREATE TABLE stage_activity(
                    action_id TEXT NOT NULL REFERENCES agent_actions(id),
                    item_id TEXT NOT NULL, experiment_id TEXT NOT NULL REFERENCES experiments(id),
                    stage_id TEXT NOT NULL, kind TEXT NOT NULL, summary TEXT NOT NULL, state TEXT NOT NULL,
                    created_at REAL NOT NULL, updated_at REAL NOT NULL,
                    PRIMARY KEY(action_id,item_id));
CREATE TABLE stage_producers(
                    experiment_id TEXT NOT NULL REFERENCES experiments(id), stage_id TEXT NOT NULL,
                    intent_id TEXT NOT NULL REFERENCES jobs(intent_id), rationale TEXT NOT NULL,
                    PRIMARY KEY(experiment_id,stage_id));
CREATE TABLE stages(
          experiment_id TEXT NOT NULL REFERENCES experiments(id), id TEXT NOT NULL,
          state TEXT NOT NULL DEFAULT 'pending', attempt INTEGER NOT NULL DEFAULT 1,
          output_manifest TEXT, error TEXT, updated_at REAL NOT NULL, ordinal INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY(experiment_id,id));
INSERT INTO "stages" VALUES('00000000-0000-4000-8000-000000000001','train','complete',1,NULL,NULL,0.0,1);
CREATE TABLE steering(
                    request_id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL REFERENCES experiments(id),
                    text TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
                    created_at REAL NOT NULL, applied_at REAL, mode TEXT NOT NULL DEFAULT 'later', cancel_job_id TEXT, replacement_experiment_id TEXT REFERENCES experiments(id));
CREATE INDEX events_experiment_revision ON events(experiment_id,revision);
CREATE INDEX idx_experiments_reference ON experiments(reference_experiment_id);
CREATE UNIQUE INDEX one_active_agent ON agent_actions(experiment_id) WHERE state='running';
CREATE INDEX agent_phase_revision ON agent_actions(experiment_id,phase,evidence_revision);
CREATE UNIQUE INDEX one_maintenance ON maintenance((1))
                    WHERE state NOT IN ('complete','failed','cancelled');
CREATE UNIQUE INDEX one_autopilot_run ON autopilot_runs(project_id) WHERE status NOT IN ('completed','stopped');
CREATE UNIQUE INDEX one_autopilot_action ON autopilot_actions(run_id) WHERE state IN ('pending','dispatching','running','uncertain','received');
CREATE INDEX obligations_open ON obligations(experiment_id) WHERE state='open';
DELETE FROM "sqlite_sequence";
COMMIT;
