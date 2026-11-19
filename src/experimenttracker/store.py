import contextlib
import json
import os
import shutil
import sqlite3
import tempfile
import threading
from pathlib import Path
from uuid import NAMESPACE_URL, uuid4, uuid5

from . import clock
from .config import ProjectConfig
from .core import (
    action_obligations,
    advancement_decisions,
    report_obligations,
    request_obligations,
    review_obligations,
    transfer_obligations,
)
from .engine import integration_queue
from .legacy_archive import (
    SUPPORTED_EXECUTION_CONTRACTS,
    archive_active,
    execution_frozen,
)
from .models import STAGES, request_answer
from .title import title_prompt


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


# Named experiment branches start from the project's single main base branch.
_INTEGRATION_BASE = "main"
_INTEGRATION_VERSION = 1

# Flip only at the owner-authorized hosted-create cutover; local and explicit creates are unaffected.
_REFUSE_UNBOUND_HOSTED_CREATES = False


def _integration_request(exp, branch, kind, *, merge_commit=None, reason=None):
    """Build the bounded queue request from the saved branch, never from client input."""
    request = {
        "schema_version": _INTEGRATION_VERSION,
        "kind": kind,
        "experiment_id": exp["id"],
        "branch": branch,
        "base": _INTEGRATION_BASE,
    }
    if reason:
        request["reason"] = reason
    if merge_commit is not None:
        request["merge_commit"] = merge_commit
    return request


def _exact_commit(value):
    return (
        isinstance(value, str)
        and len(value) in (40, 64)
        and all(char in "0123456789abcdef" for char in value)
    )


def public_projection(value):
    """Omit executor repair evidence from views, including already queued views."""
    result = dict(value)
    contract = dict(result["contract"])
    for key in ("repair_project", "working_directory"):
        contract.pop(key, None)
    if contract.get("early_repair"):
        contract["early_repair"] = {
            key: value
            for key, value in contract["early_repair"].items()
            if key in ("state", "revision", "failed_intents", "decisions")
        }
    if contract.get("job_cancellations"):
        contract["job_cancellations"] = {
            request: {
                key: value
                for key, value in record.items()
                if key in ("intent_id", "reason", "state", "job_id", "scheduler_state")
            }
            for request, record in contract["job_cancellations"].items()
        }
    for key in ("auto_repair", "repair_history"):
        if key not in contract:
            continue
        repairs = contract[key] if key == "repair_history" else [contract[key]]
        summaries = [
            {
                k: repair[k]
                for k in ("revision", "previous_revision", "kind", "reason", "error", "completed_stages")
                if k in repair
            }
            for repair in repairs
        ]
        contract[key] = summaries if key == "repair_history" else summaries[0]
    result["contract"] = contract
    # D1 projections and gateway requests are bounded. Prefer active/recent job
    # previews when a large graph contains more telemetry than one view can hold.
    if "jobs" in result:
        jobs = [dict(job) for job in result["jobs"]]
        remaining = 256 * 1024
        for job in sorted(
            jobs,
            key=lambda j: (j.get("state") in ("submitted", "submitting"), j.get("created_at", 0)),
            reverse=True,
        ):
            preview = job.get("tensorboard")
            if preview:
                size = len(encode(preview).encode())
                if size > remaining:
                    job["tensorboard"] = None
                else:
                    remaining -= size
        result["jobs"] = jobs
    events = []
    for event in result.get("events", []):
        omitted = {
            "automatic_repair_started": {"snapshot"},
            "owner_question_asked": {"question", "options", "objective"},
            "owner_approval_requested": {"brief", "objective"},
            "selective_repair_queued": {"original_work", "error"},
            "selective_cancellation_uncertain": {"error"},
            "research_checkpoint_saved": {"markdown"},
        }.get(event["kind"], set())
        if event["kind"].startswith("selective_cancellation_"):
            omitted.add("error")
        if omitted:
            event = {
                **event,
                "payload": encode(
                    {key: value for key, value in json.loads(event["payload"]).items() if key not in omitted}
                ),
            }
        events.append(event)
    result["events"] = events
    return result


class Conflict(ValueError):
    pass


class Store:
    """One process owns the database. Every domain mutation emits a durable outbox reference."""

    @classmethod
    def read_only(cls, path: Path):
        reader = cls.__new__(cls)
        reader.path = path
        reader.lock = threading.RLock()
        reader.db = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True, check_same_thread=False)
        reader.db.row_factory = sqlite3.Row
        version = reader.db.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]
        if version not in (12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25):
            reader.close()
            raise RuntimeError("start the owner to migrate this database before detailed inspection")
        return reader

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path = path
        self.projects = {"test-project": ProjectConfig(id="test-project", name="Test project")}
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        path.chmod(0o600)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA journal_mode=DELETE")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS schema_version(version INTEGER PRIMARY KEY);
        INSERT OR IGNORE INTO schema_version VALUES(1);
        CREATE TABLE IF NOT EXISTS experiments(
          id TEXT PRIMARY KEY, brief TEXT NOT NULL, recipe_id TEXT NOT NULL, contract TEXT NOT NULL,
          phase TEXT NOT NULL DEFAULT 'preparing', hold_reason TEXT, revision INTEGER NOT NULL DEFAULT 0,
          evidence_revision INTEGER NOT NULL DEFAULT 1, created_at REAL NOT NULL, updated_at REAL NOT NULL,
          observed_at REAL, thread_id TEXT, model TEXT, snapshot TEXT,
          pause_agent INTEGER NOT NULL DEFAULT 0, pause_compute INTEGER NOT NULL DEFAULT 0,
          pending_request TEXT, analysis TEXT, error TEXT, scientific_outcome TEXT);
        CREATE TABLE IF NOT EXISTS stages(
          experiment_id TEXT NOT NULL REFERENCES experiments(id), id TEXT NOT NULL,
          state TEXT NOT NULL DEFAULT 'pending', attempt INTEGER NOT NULL DEFAULT 1,
          output_manifest TEXT, error TEXT, updated_at REAL NOT NULL,
          PRIMARY KEY(experiment_id,id));
        CREATE TABLE IF NOT EXISTS jobs(
          intent_id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL REFERENCES experiments(id),
          stage_id TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 1, state TEXT NOT NULL,
          job_id TEXT, script_hash TEXT NOT NULL, resources TEXT NOT NULL, snapshot_hash TEXT NOT NULL,
          created_at REAL NOT NULL, observed_at REAL, scheduler_state TEXT, exit_code TEXT, error TEXT,
          UNIQUE(experiment_id,stage_id,attempt),
          FOREIGN KEY(experiment_id,stage_id) REFERENCES stages(experiment_id,id));
        CREATE TABLE IF NOT EXISTS agent_actions(
          id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL REFERENCES experiments(id), phase TEXT NOT NULL,
          evidence_revision INTEGER NOT NULL, state TEXT NOT NULL, thread_id TEXT, turn_id TEXT,
          input TEXT NOT NULL, result TEXT, error TEXT, attempts INTEGER NOT NULL DEFAULT 0,
          created_at REAL NOT NULL, updated_at REAL NOT NULL,
          UNIQUE(experiment_id,phase,evidence_revision));
        CREATE UNIQUE INDEX IF NOT EXISTS one_active_agent ON agent_actions((1))
          WHERE state='running';
        CREATE TABLE IF NOT EXISTS artifacts(
          id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL REFERENCES experiments(id), stage_id TEXT NOT NULL,
          path TEXT NOT NULL, name TEXT NOT NULL, sha256 TEXT NOT NULL, size INTEGER NOT NULL,
          media_type TEXT NOT NULL, evidence_revision INTEGER NOT NULL, local_valid INTEGER NOT NULL,
          remote_published INTEGER NOT NULL DEFAULT 0, UNIQUE(experiment_id,name,evidence_revision));
        CREATE TABLE IF NOT EXISTS commands(
          request_id TEXT PRIMARY KEY, experiment_id TEXT REFERENCES experiments(id), kind TEXT NOT NULL,
          origin TEXT NOT NULL, payload TEXT NOT NULL, state TEXT NOT NULL, result TEXT NOT NULL,
          received_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS notes(
          id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL REFERENCES experiments(id),
          author TEXT NOT NULL, text TEXT NOT NULL, created_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS events(
          seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE,
          experiment_id TEXT NOT NULL REFERENCES experiments(id), kind TEXT NOT NULL,
          revision INTEGER NOT NULL, payload TEXT NOT NULL, created_at REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS events_experiment_revision ON events(experiment_id,revision);
        CREATE TABLE IF NOT EXISTS outbox(
          id TEXT PRIMARY KEY REFERENCES events(id), payload TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
          delivered_at REAL, next_attempt_at REAL NOT NULL DEFAULT 0, error TEXT);
        CREATE TABLE IF NOT EXISTS inbox(
          id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL REFERENCES experiments(id),
          kind TEXT NOT NULL, text TEXT NOT NULL, created_at REAL NOT NULL,
          UNIQUE(experiment_id,kind,text));
        CREATE TABLE IF NOT EXISTS quota_snapshots(
          id INTEGER PRIMARY KEY AUTOINCREMENT, observed_at REAL NOT NULL, payload TEXT NOT NULL,
          admitted INTEGER NOT NULL, reason TEXT);
        """)
        version = self.db.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]
        if version == 1:
            with self.tx() as db:
                db.execute("ALTER TABLE stages ADD COLUMN ordinal INTEGER NOT NULL DEFAULT 0")
                db.executemany(
                    "UPDATE stages SET ordinal=? WHERE id=?", [(i, stage) for i, stage in enumerate(STAGES)]
                )
                db.execute("INSERT INTO schema_version VALUES(2)")
            version = 2
        if version == 2:
            with self.tx() as db:
                db.execute("""CREATE TABLE rejected_gateway_commands(
                    request_id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL, kind TEXT NOT NULL,
                    payload TEXT NOT NULL, error TEXT NOT NULL, received_at REAL NOT NULL)""")
                db.execute("INSERT INTO schema_version VALUES(3)")
            version = 3
        if version == 3:
            with self.tx() as db:
                db.execute(
                    "ALTER TABLE experiments ADD COLUMN reference_experiment_id TEXT REFERENCES experiments(id)"
                )
                db.execute("CREATE INDEX idx_experiments_reference ON experiments(reference_experiment_id)")
                db.execute("INSERT INTO schema_version VALUES(4)")
            version = 4
        if version == 4:
            with self.tx() as db:
                db.execute("ALTER TABLE experiments ADD COLUMN deleted_at REAL")
                for column in (
                    "started_at REAL",
                    "ended_at REAL",
                    "estimated_start_at REAL",
                    "pending_reason TEXT",
                ):
                    db.execute("ALTER TABLE jobs ADD COLUMN " + column)
                db.execute("""CREATE TABLE stage_activity(
                    action_id TEXT NOT NULL REFERENCES agent_actions(id),
                    item_id TEXT NOT NULL, experiment_id TEXT NOT NULL REFERENCES experiments(id),
                    stage_id TEXT NOT NULL, kind TEXT NOT NULL, summary TEXT NOT NULL, state TEXT NOT NULL,
                    created_at REAL NOT NULL, updated_at REAL NOT NULL,
                    PRIMARY KEY(action_id,item_id))""")
                db.execute("INSERT INTO schema_version VALUES(5)")
            version = 5
        if version == 5:
            with self.tx() as db:
                db.execute("""CREATE TABLE report_versions(
                    experiment_id TEXT NOT NULL REFERENCES experiments(id), version INTEGER NOT NULL,
                    state TEXT NOT NULL, document TEXT NOT NULL, request_id TEXT NOT NULL UNIQUE,
                    artifact_ids TEXT NOT NULL DEFAULT '[]', report_artifact_id TEXT, error TEXT,
                    created_at REAL NOT NULL, updated_at REAL NOT NULL,
                    PRIMARY KEY(experiment_id,version))""")
                db.execute("INSERT INTO schema_version VALUES(6)")
            version = 6
        if version == 6:
            with self.tx() as db:
                db.execute("""CREATE TABLE discussions(
                    id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL REFERENCES experiments(id),
                    seq INTEGER NOT NULL, text TEXT NOT NULL, model TEXT, reasoning_effort TEXT,
                    state TEXT NOT NULL DEFAULT 'queued', hold_reason TEXT, error TEXT, reply TEXT,
                    evidence_ids TEXT NOT NULL DEFAULT '[]', report_version INTEGER,
                    action_id TEXT UNIQUE REFERENCES agent_actions(id),
                    created_at REAL NOT NULL, updated_at REAL NOT NULL,
                    UNIQUE(experiment_id,seq))""")
                db.execute("""CREATE TABLE discussion_threads(
                    experiment_id TEXT PRIMARY KEY REFERENCES experiments(id), thread_id TEXT NOT NULL)""")
                db.execute("INSERT INTO schema_version VALUES(7)")
            version = 7
        if version == 7:
            with self.tx() as db:
                db.execute("ALTER TABLE discussions ADD COLUMN artifact_ids TEXT NOT NULL DEFAULT '[]'")
                db.execute(
                    "ALTER TABLE discussions ADD COLUMN followup_experiment_id TEXT REFERENCES experiments(id)"
                )
                db.execute("INSERT INTO schema_version VALUES(8)")
            version = 8
        if version == 8:
            with self.tx() as db:
                db.execute("""CREATE TABLE projects(
                    id TEXT PRIMARY KEY, name TEXT NOT NULL, folder TEXT NOT NULL UNIQUE,
                    device INTEGER NOT NULL, inode INTEGER NOT NULL, non_empty INTEGER NOT NULL,
                    created_at REAL NOT NULL)""")
                db.execute("""CREATE TABLE project_requests(
                    request_id TEXT PRIMARY KEY, project_id TEXT NOT NULL UNIQUE, payload TEXT NOT NULL,
                    folder TEXT, state TEXT NOT NULL DEFAULT 'pending', error TEXT, result TEXT,
                    created_at REAL NOT NULL, delivered_at REAL, origin TEXT NOT NULL DEFAULT 'local')""")
                db.execute("""CREATE TABLE experiment_workspaces(
                    experiment_id TEXT PRIMARY KEY REFERENCES experiments(id),
                    folder TEXT NOT NULL, device INTEGER NOT NULL, inode INTEGER NOT NULL,
                    root TEXT NOT NULL UNIQUE)""")
                db.execute("INSERT INTO schema_version VALUES(9)")
            version = 9
        if version == 9:
            with self.tx() as db:
                db.execute("""CREATE TABLE project_preferences(
                    project_id TEXT PRIMARY KEY, instructions TEXT NOT NULL DEFAULT '',
                    revision INTEGER NOT NULL DEFAULT 0, updated_at REAL NOT NULL)""")
                db.execute("""CREATE TABLE project_settings_requests(
                    request_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, payload TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'pending', error TEXT, result TEXT,
                    created_at REAL NOT NULL, delivered_at REAL, origin TEXT NOT NULL DEFAULT 'local')""")
                db.execute("INSERT INTO schema_version VALUES(10)")
            version = 10
        if version == 10:
            with self.tx() as db:
                db.execute("ALTER TABLE experiments ADD COLUMN title TEXT")
                db.execute("ALTER TABLE experiments ADD COLUMN plan_review TEXT")
                db.execute("""CREATE TABLE steering(
                    request_id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL REFERENCES experiments(id),
                    text TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
                    created_at REAL NOT NULL, applied_at REAL)""")
                db.execute("""CREATE TABLE quota_settings(
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    five_hour_percent INTEGER NOT NULL, weekly_percent INTEGER NOT NULL,
                    request_id TEXT NOT NULL UNIQUE, updated_at REAL NOT NULL)""")
                db.execute("INSERT INTO schema_version VALUES(11)")
            version = 11
        if version == 11:
            with self.tx() as db:
                db.execute("ALTER TABLE steering ADD COLUMN mode TEXT NOT NULL DEFAULT 'later'")
                db.execute("ALTER TABLE steering ADD COLUMN cancel_job_id TEXT")
                db.execute(
                    "ALTER TABLE steering ADD COLUMN replacement_experiment_id TEXT REFERENCES experiments(id)"
                )
                db.execute(
                    "ALTER TABLE experiments ADD COLUMN replaces_experiment_id TEXT REFERENCES experiments(id)"
                )
                db.execute("INSERT INTO schema_version VALUES(12)")
            version = 12
        if version == 12:
            with self.tx() as db:
                db.execute("ALTER TABLE jobs ADD COLUMN progress TEXT")
                db.execute("INSERT INTO schema_version VALUES(13)")
            version = 13
        if version == 13:
            with self.tx() as db:
                db.execute("""CREATE TABLE isolated_workspaces(
                    experiment_id TEXT PRIMARY KEY REFERENCES experiments(id),
                    source TEXT NOT NULL, folder TEXT NOT NULL UNIQUE, head TEXT NOT NULL,
                    baseline TEXT NOT NULL, common_dir TEXT NOT NULL, git_device INTEGER NOT NULL,
                    git_inode INTEGER NOT NULL, state TEXT NOT NULL, device INTEGER, inode INTEGER,
                    created_at REAL NOT NULL)""")
                db.execute("INSERT INTO schema_version VALUES(14)")
            version = 14
        if version == 14:
            # Keep all existing identities and foreign-key targets. A phase may
            # now have several append-only continuations at the same revision.
            self.db.execute("PRAGMA foreign_keys=OFF")
            try:
                with self.tx() as db:
                    db.execute("""CREATE TABLE agent_actions_next(
                        id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL REFERENCES experiments(id),
                        phase TEXT NOT NULL, evidence_revision INTEGER NOT NULL, state TEXT NOT NULL,
                        thread_id TEXT, turn_id TEXT, input TEXT NOT NULL, result TEXT, error TEXT,
                        attempts INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL, updated_at REAL NOT NULL,
                        previous_action_id TEXT UNIQUE REFERENCES agent_actions(id), interrupt_reason TEXT)""")
                    db.execute("INSERT INTO agent_actions_next SELECT *,NULL,NULL FROM agent_actions")
                    db.execute("DROP TABLE agent_actions")
                    db.execute("ALTER TABLE agent_actions_next RENAME TO agent_actions")
                    db.execute(
                        "CREATE UNIQUE INDEX one_active_agent ON agent_actions(experiment_id) WHERE state='running'"
                    )
                    db.execute(
                        "CREATE INDEX agent_phase_revision ON agent_actions(experiment_id,phase,evidence_revision)"
                    )
                    db.execute("ALTER TABLE jobs ADD COLUMN execution_root TEXT")
                    db.execute("ALTER TABLE jobs ADD COLUMN execution_manifest TEXT")
                    db.execute("""CREATE TABLE agent_wakeups(
                        id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL REFERENCES experiments(id),
                        reason TEXT NOT NULL, due_at REAL NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
                        action_id TEXT REFERENCES agent_actions(id), created_at REAL NOT NULL)""")
                    db.execute("INSERT INTO schema_version VALUES(15)")
            finally:
                self.db.execute("PRAGMA foreign_keys=ON")
            if self.rows("PRAGMA foreign_key_check"):
                raise RuntimeError("agent continuation migration failed foreign-key validation")
            version = 15
        if version == 15:
            with self.tx() as db:
                db.execute("""CREATE TABLE agent_usage(
                    action_id TEXT PRIMARY KEY REFERENCES agent_actions(id),
                    thread_id TEXT NOT NULL, turn_id TEXT NOT NULL,
                    payload TEXT NOT NULL, observed_at REAL NOT NULL)""")
                db.execute("INSERT INTO schema_version VALUES(16)")
            version = 16
        if version == 16:
            from .knowledge import migrate

            with self.tx() as db:
                migrate(db)
                db.execute("INSERT INTO schema_version VALUES(17)")
            version = 17
        if version == 17:
            with self.tx() as db:
                db.execute("ALTER TABLE project_preferences ADD COLUMN execution TEXT NOT NULL DEFAULT '{}'")
                db.execute("ALTER TABLE jobs ADD COLUMN remote_polled_at REAL")
                db.execute("""CREATE TABLE execution_transfers(
                    id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL REFERENCES experiments(id),
                    direction TEXT NOT NULL, payload TEXT NOT NULL, state TEXT NOT NULL,
                    result TEXT, updated_at REAL NOT NULL)""")
                db.execute("INSERT INTO schema_version VALUES(18)")
            version = 18
        if version == 18:
            with self.tx() as db:
                db.execute("ALTER TABLE jobs ADD COLUMN tensorboard TEXT")
                db.execute("INSERT INTO schema_version VALUES(19)")
            version = 19
        if version == 19:
            with self.tx() as db:
                db.execute("ALTER TABLE experiments ADD COLUMN partial_results TEXT")
                db.execute("""CREATE TABLE output_refs(
                    id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL REFERENCES experiments(id),
                    intent_id TEXT NOT NULL REFERENCES jobs(intent_id), name TEXT NOT NULL,
                    context TEXT NOT NULL, host TEXT NOT NULL, path TEXT NOT NULL, kind TEXT NOT NULL,
                    created_at REAL NOT NULL, UNIQUE(intent_id,name))""")
                db.execute("""CREATE TABLE output_bindings(
                    consumer_intent TEXT NOT NULL REFERENCES jobs(intent_id),
                    output_id TEXT NOT NULL REFERENCES output_refs(id), rationale TEXT NOT NULL,
                    PRIMARY KEY(consumer_intent,output_id))""")
                db.execute("""CREATE TABLE stage_producers(
                    experiment_id TEXT NOT NULL REFERENCES experiments(id), stage_id TEXT NOT NULL,
                    intent_id TEXT NOT NULL REFERENCES jobs(intent_id), rationale TEXT NOT NULL,
                    PRIMARY KEY(experiment_id,stage_id))""")
                db.execute("INSERT INTO schema_version VALUES(20)")
            version = 20
        if version == 20:
            with self.tx() as db:
                db.execute("""CREATE TABLE releases(
                    commit_id TEXT PRIMARY KEY, summary TEXT NOT NULL, state TEXT NOT NULL,
                    receipt TEXT, error TEXT, updated_at REAL NOT NULL)""")
                db.execute("""CREATE TABLE maintenance(
                    request_id TEXT PRIMARY KEY, release_id TEXT NOT NULL REFERENCES releases(commit_id),
                    state TEXT NOT NULL, error TEXT, detail TEXT NOT NULL DEFAULT '{}',
                    created_at REAL NOT NULL, updated_at REAL NOT NULL)""")
                db.execute("""CREATE UNIQUE INDEX one_maintenance ON maintenance((1))
                    WHERE state NOT IN ('complete','failed','cancelled')""")
                db.execute("INSERT INTO schema_version VALUES(21)")
            version = 21
        if version == 21:
            from .autopilot import migrate

            with self.tx() as db:
                migrate(db)
                db.execute("INSERT INTO schema_version VALUES(22)")
            version = 22
        if version == 22:
            with self.tx() as db:
                db.execute("""CREATE TABLE artifact_registrations(
                    artifact_id TEXT PRIMARY KEY REFERENCES artifacts(id),
                    event_id TEXT NOT NULL REFERENCES events(id),
                    sha256 TEXT NOT NULL, size INTEGER NOT NULL)""")
                db.execute("INSERT INTO schema_version VALUES(23)")
            version = 23
        if version == 23:
            with self.tx() as db:
                db.execute("""CREATE TABLE obligations(
                    id TEXT PRIMARY KEY, experiment_id TEXT REFERENCES experiments(id), project_id TEXT,
                    kind TEXT NOT NULL, target TEXT,
                    owner TEXT NOT NULL CHECK(owner IN ('backend','agent','owner','external')),
                    wait TEXT,
                    state TEXT NOT NULL CHECK(state IN ('open','satisfied','abandoned')),
                    detail TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL, due_at REAL)""")
                db.execute("CREATE INDEX obligations_open ON obligations(experiment_id) WHERE state='open'")
                db.execute("INSERT INTO schema_version VALUES(24)")
            version = 24
        if version == 24:
            with self.tx() as db:
                db.execute("""CREATE TABLE experiment_branches(
                    experiment_id TEXT PRIMARY KEY REFERENCES experiments(id),
                    repo TEXT NOT NULL, branch TEXT NOT NULL, worktree TEXT NOT NULL,
                    base_commit TEXT NOT NULL, head_commit TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('active','integrating','merged','abandoned')),
                    merged_commit TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL)""")
                db.execute("INSERT INTO schema_version VALUES(25)")
            version = 25
        if version != 25:
            raise RuntimeError("unsupported database schema version")

    def continue_action(self, action, *, request_id=None, feedback=None, _db=None):
        """Append once after a saved terminal outcome; never replace its identity/result."""
        blocked = (
            action["state"] == "completed"
            and (action["phase"].startswith("agent:") or action["phase"] == "prepare")
            and json.loads(action["result"] or "{}").get("outcome") in ("blocked", "failed")
        )
        if action["state"] != "failed" and not blocked:
            raise Conflict("continuation requires a known failed action")
        with contextlib.nullcontext(_db) if _db is not None else self.tx() as db:
            prior = db.execute(
                "SELECT * FROM agent_actions WHERE previous_action_id=?", (action["id"],)
            ).fetchone()
            if prior:
                return dict(prior)
            identity, now = str(request_id or uuid4()), clock.now()
            db.execute(
                "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,input,error,"
                "created_at,updated_at,previous_action_id) VALUES(?,?,?,?,'pending',?,?,?,?,?,?)",
                (
                    identity,
                    action["experiment_id"],
                    action["phase"],
                    action["evidence_revision"],
                    action["thread_id"],
                    action["input"],
                    feedback or action["error"],
                    now,
                    now,
                    action["id"],
                ),
            )
            saved = dict(db.execute("SELECT * FROM agent_actions WHERE id=?", (identity,)).fetchone())
            action_obligations.sync_action(db, identity)
            self.emit(
                db,
                action["experiment_id"],
                "agent_continuation_queued",
                {"action_id": identity, "previous_action_id": action["id"], "phase": action["phase"]},
            )
            return saved

    def _consume_steering(self, db, eid, *, request_id=None):
        """Attach saved later guidance to an admitted recovery, in its transaction."""
        rows = db.execute(
            "SELECT request_id,text FROM steering WHERE experiment_id=? AND state='pending' "
            "AND (mode='later' OR request_id=?) ORDER BY created_at,rowid",
            (eid, request_id),
        ).fetchall()
        if not rows:
            return None
        db.executemany(
            "UPDATE steering SET state='applied',applied_at=? WHERE request_id=?",
            [(clock.now(), row["request_id"]) for row in rows],
        )
        self.emit(db, eid, "steering_applied", {"request_ids": [row["request_id"] for row in rows]})
        return "\n\n".join(row["text"] for row in rows)

    def _resume_blocked(self, db, exp, request_id, feedback, *, resolve_request=False):
        """Continue only a settled blocked phase; retain the original turn and attempt."""
        eid = exp["id"]
        if (
            exp["phase"] in ("complete", "cancelled")
            or (exp["pending_request"] and not resolve_request)
            or db.execute(
                "SELECT 1 FROM agent_actions WHERE experiment_id=? "
                "AND state IN ('dispatching','running','uncertain')",
                (eid,),
            ).fetchone()
            or db.execute(
                "SELECT 1 FROM steering WHERE experiment_id=? AND mode='restart' "
                "AND state IN ('pending','cancelling')",
                (eid,),
            ).fetchone()
        ):
            raise Conflict("resolve pending or uncertain work before resuming the blocked phase")
        implementation = exp["hold_reason"] == "implementation_blocked"
        saved = db.execute(
            "SELECT a.* FROM agent_actions a JOIN stages s ON s.experiment_id=a.experiment_id "
            "AND s.id=CASE WHEN a.phase='prepare' THEN 'prepare' ELSE substr(a.phase,7) END "
            "WHERE a.experiment_id=? "
            "AND (a.state='failed' OR (a.state='completed' "
            "AND json_extract(COALESCE(a.result,'{}'),'$.outcome') IN ('blocked','failed'))) "
            "AND NOT EXISTS (SELECT 1 FROM agent_actions child WHERE child.previous_action_id=a.id) AND "
            + (
                "a.phase='prepare' "
                if implementation
                else "a.phase LIKE 'agent:%' AND a.evidence_revision=? AND s.state IN ('pending','failed') "
            )
            + "ORDER BY a.rowid DESC LIMIT 1",
            (eid,) if implementation else (eid, exp["evidence_revision"]),
        ).fetchone()
        if (
            not saved
            or saved["state"] not in ("completed", "failed")
            or (saved["state"] == "failed" and saved["attempts"] and not saved["turn_id"])
        ):
            raise Conflict("resumption requires a known settled blocked action")
        action = self.continue_action(dict(saved), request_id=request_id, feedback=feedback, _db=db)
        db.execute(
            "UPDATE experiments SET phase=?,hold_reason=NULL,error=NULL,pending_request=NULL WHERE id=?",
            ("preparing" if implementation else "training", eid),
        )
        if exp["pending_request"]:
            request_obligations.sync_request(db, eid, before=exp["pending_request"])
        db.execute(
            "UPDATE stages SET state=?,error=NULL,updated_at=? WHERE experiment_id=? AND id=?",
            (
                "pending" if implementation else "running",
                clock.now(),
                eid,
                "prepare" if implementation else saved["phase"].removeprefix("agent:"),
            ),
        )
        return action

    @contextlib.contextmanager
    def tx(self):
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                yield self.db
                self.db.execute("COMMIT")
            except BaseException:
                self.db.execute("ROLLBACK")
                raise

    def rows(self, sql, args=()):
        with self.lock:
            return [dict(row) for row in self.db.execute(sql, args)]

    def one(self, sql, args=()):
        rows = self.rows(sql, args)
        return rows[0] if rows else None

    def experiment(self, eid):
        result = self.one("SELECT * FROM experiments WHERE id=?", (eid,))
        if not result:
            raise KeyError(eid)
        for field in ("contract", "snapshot", "analysis", "pending_request", "plan_review"):
            result[field] = json.loads(result[field]) if result[field] else None
        for field in ("pause_agent", "pause_compute"):
            result[field] = bool(result[field])
        return result

    def projection(self, eid):
        result = self.experiment(eid)
        result["project_id"] = result["contract"].get("project", {}).get("id", "test-project")
        live = not execution_frozen(result)
        if self.one("SELECT 1 FROM sqlite_master WHERE name='autopilot_runs'"):
            result["autopilot_stage_holds"] = self.rows(
                "SELECT stage_id,attempt,reason,detail FROM autopilot_stage_holds WHERE experiment_id=?",
                (eid,),
            )
            result["autopilot"] = self.one(
                "SELECT run_id,project_id,purpose,deferred FROM autopilot_members WHERE experiment_id=?",
                (eid,),
            )
        else:
            result["autopilot_stage_holds"], result["autopilot"] = [], None
        result["steering"] = self.rows(
            "SELECT request_id,text,mode,state,created_at,applied_at,replacement_experiment_id FROM steering WHERE experiment_id=? ORDER BY created_at",
            (eid,),
        )
        for name, table in (
            ("stages", "stages"),
            ("jobs", "jobs"),
            ("artifacts", "artifacts"),
            ("notes", "notes"),
            ("agent_actions", "agent_actions"),
        ):
            ordering = (
                " ORDER BY ordinal"
                if table == "stages"
                else " ORDER BY created_at"
                if table == "agent_actions"
                else ""
            )
            condition = " AND phase NOT LIKE 'discuss:%'" if table == "agent_actions" else ""
            if table == "artifacts" and not live:
                condition = f" AND evidence_revision={int(result['evidence_revision'])}"
            elif table == "jobs" and result["contract"].get("auto_repair") and not live:
                reused = result["contract"].get("reused_job_intents", [])
                condition = f" AND (attempt={int(result['evidence_revision'])}"
                if reused:
                    from uuid import UUID

                    ids = ",".join("'" + str(UUID(i)) + "'" for i in reused)
                    condition += f" OR intent_id IN ({ids})"
                condition += ")"
            result[name] = self.rows(
                f"SELECT * FROM {table} WHERE experiment_id=?" + condition + ordering, (eid,)
            )
        # Agent attempts share producer bookkeeping, but are not scheduler jobs.
        result["jobs"] = [
            j
            for j in result["jobs"]
            if json.loads(j.get("execution_manifest") or "{}").get("executor") != "agent"
        ]
        if (
            result["snapshot"]
            and result["snapshot"].get("expected_outputs")
            and result["recipe_id"] == "agent_experiment_v1"
        ):
            current_stages = {"plan", "prepare", "validate", "interpret", "publish"} | set(
                result["snapshot"]["expected_outputs"]
            )
            if result["contract"].get("execution_contract") == 3:
                current_stages.discard("validate")
            result["stages"] = [stage for stage in result["stages"] if stage["id"] in current_stages]
        result["conversation"] = self.conversation(eid)
        result["conversation_total"] = self.one(
            "SELECT COUNT(*) AS n FROM discussions WHERE experiment_id=?", (eid,)
        )["n"]
        result["report_versions"] = self.rows(
            "SELECT version,state,artifact_ids,report_artifact_id,error,created_at,updated_at "
            "FROM report_versions WHERE experiment_id=? ORDER BY version",
            (eid,),
        )
        for report in result["report_versions"]:
            report["artifact_ids"] = json.loads(report["artifact_ids"])
        ready = [r for r in result["report_versions"] if r["state"] == "ready"]
        result["active_report_id"] = ready[-1]["report_artifact_id"] if ready else None
        result["stage_activity"] = self.rows(
            "SELECT stage_id,kind,summary,state,created_at,updated_at FROM stage_activity "
            "WHERE experiment_id=? AND stage_id NOT LIKE 'discuss:%' ORDER BY created_at",
            (eid,),
        )
        for artifact in result["artifacts"]:
            # Absolute paths are executor-only metadata, never exposed to a browser or gateway.
            artifact.pop("path")
        for action in result["agent_actions"]:
            action.pop("input")
            action.pop("result")
        for job in result["jobs"]:
            job.pop("execution_root", None)
            manifest = job.pop("execution_manifest", None)
            if live:
                commit = (json.loads(manifest) if manifest else {}).get("commit")
                if _exact_commit(commit):
                    job["commit"] = commit
            job["resources"] = json.loads(job["resources"])
            job["progress"] = json.loads(job["progress"]) if job.get("progress") else None
            job["tensorboard"] = json.loads(job["tensorboard"]) if job.get("tensorboard") else None
        result["events"] = self.rows(
            "SELECT * FROM (SELECT seq,id,kind,revision,payload,created_at FROM events WHERE experiment_id=? ORDER BY seq DESC LIMIT 100) ORDER BY seq",
            (eid,),
        )
        if result["contract"].get("execution_contract") == 4:
            # Expose only validated turn-end fields and checkpoint metadata; never the
            # saved action result or private checkpoint Markdown.
            result["research_turns"] = []
            for action in self.rows(
                "SELECT id,state,result,created_at,updated_at FROM ("
                "SELECT id,state,result,created_at,updated_at,rowid FROM agent_actions "
                "WHERE experiment_id=? AND phase='research' ORDER BY created_at DESC,rowid DESC LIMIT 50) "
                "ORDER BY created_at,rowid",
                (eid,),
            ):
                turn = {key: action[key] for key in ("id", "state", "created_at", "updated_at")}
                # Only completed turns have a validated C7 turn-end. Failed/uncertain raw
                # results may contain arbitrary agent output and are never projected.
                if action["state"] == "completed":
                    try:
                        end = json.loads(action["result"] or "null")
                    except (TypeError, ValueError):
                        end = None
                    if (
                        isinstance(end, dict)
                        and end.get("status") in ("continue", "wait", "needs_owner", "done")
                        and isinstance(end.get("summary"), str)
                        and isinstance(end.get("next"), str)
                    ):
                        turn.update(
                            status=end["status"],
                            summary=end["summary"][:4000],
                            next=end["next"][:2000],
                        )
                result["research_turns"].append(turn)
            checkpoint = self.one(
                "SELECT payload,created_at FROM events WHERE experiment_id=? "
                "AND kind='research_checkpoint_saved' ORDER BY seq DESC LIMIT 1",
                (eid,),
            )
            if checkpoint:
                payload = json.loads(checkpoint["payload"])
                version = payload.get("version")
                size = payload.get("bytes")
                result["research_checkpoint"] = {
                    "version": version if isinstance(version, int) and version > 0 else None,
                    "bytes": size if isinstance(size, int) and size >= 0 else None,
                    "created_at": checkpoint["created_at"],
                }
            else:
                result["research_checkpoint"] = None
        result["partial_results"] = (
            json.loads(result["partial_results"]) if result.get("partial_results") else None
        )
        if result["partial_results"]:
            remaining = 200
            for task in result["partial_results"]["tasks"].values():
                metrics = task["result"]["metrics"]
                task["metric_count"] = len(metrics)
                task["result"]["metrics"] = metrics[:remaining]
                remaining -= len(task["result"]["metrics"])
                task["metrics_truncated"] = len(task["result"]["metrics"]) < len(metrics)
                task["result"].pop("protocol", None)
        result["output_refs"] = (
            self.rows(
                "SELECT id,intent_id,name,kind,created_at FROM output_refs WHERE experiment_id=? ORDER BY created_at DESC LIMIT 1000",
                (eid,),
            )
            if self.one("SELECT name FROM sqlite_master WHERE name='output_refs'")
            else []
        )
        result["schema_version"] = 1
        if live:
            branch = self._projected_branch(eid)
            if branch:
                result["branch"] = branch["branch"]
                result["head_commit"] = branch["head_commit"]
                result["branch_state"] = branch["state"]
                integration = self._projected_integration(eid)
                if integration:
                    result["integration"] = integration
        return public_projection(result)

    def _projected_branch(self, eid):
        """Return only the branch identity and state safe to publish, never checkout paths."""
        if not self.one("SELECT 1 FROM sqlite_master WHERE name='experiment_branches'"):
            return None
        return self.one(
            "SELECT branch,head_commit,state FROM experiment_branches WHERE experiment_id=?",
            (eid,),
        )

    def _projected_integration(self, eid):
        """Return the saved integration stage and exact approval identity, not its detail."""
        if not self.one("SELECT 1 FROM sqlite_master WHERE name='obligations'"):
            return None
        row = self.one(
            "SELECT * FROM obligations WHERE kind='integration' AND experiment_id=? "
            "ORDER BY created_at DESC,id DESC LIMIT 1",
            (eid,),
        )
        if row is None:
            return None
        try:
            return integration_queue.public_record(row)
        except ValueError:
            return None

    def conversation(self, eid, before=None):
        rows = self.rows(
            "SELECT id,seq,text,model,reasoning_effort,state,hold_reason,error,reply,evidence_ids,"
            "report_version,artifact_ids,followup_experiment_id,created_at,updated_at,action_id FROM discussions "
            "WHERE experiment_id=? AND seq<? ORDER BY seq DESC LIMIT 20",
            (eid, before if before is not None else 9223372036854775807),
        )
        for row in rows:
            row["evidence_ids"] = json.loads(row["evidence_ids"])
            row["artifact_ids"] = json.loads(row["artifact_ids"])
            row["activity"] = self.rows(
                "SELECT summary,state FROM (SELECT summary,state,created_at FROM stage_activity "
                "WHERE action_id=? ORDER BY created_at DESC LIMIT 20) ORDER BY created_at",
                (row.pop("action_id"),),
            )
        return list(reversed(rows))

    def discussion_state(self, identity, state, *, hold_reason=None, error=None):
        with self.tx() as db:
            row = db.execute(
                "SELECT experiment_id,state,hold_reason,error FROM discussions WHERE id=?", (identity,)
            ).fetchone()
            if (row["state"], row["hold_reason"], row["error"]) == (state, hold_reason, error):
                return
            db.execute(
                "UPDATE discussions SET state=?,hold_reason=?,error=?,updated_at=? WHERE id=?",
                (state, hold_reason, error[:2000] if error else None, clock.now(), identity),
            )
            self.emit(
                db, row["experiment_id"], "discussion_updated", {"discussion_id": identity, "state": state}
            )

    def emit(self, db, eid, kind, payload=None):
        now = clock.now()
        db.execute("UPDATE experiments SET revision=revision+1,updated_at=? WHERE id=?", (now, eid))
        revision = db.execute("SELECT revision FROM experiments WHERE id=?", (eid,)).fetchone()[0]
        event_id = str(uuid4())
        db.execute(
            "INSERT INTO events(id,experiment_id,kind,revision,payload,created_at) VALUES(?,?,?,?,?,?)",
            (event_id, eid, kind, revision, encode(payload or {}), now),
        )
        db.execute("INSERT INTO outbox(id,payload) VALUES(?,'{}')", (event_id,))

    def archive_legacy_experiments(self):
        """Put unfinished historical contracts on a read-only owner decision hold."""
        text = (
            "This legacy execution is archived and read-only. You may cancel it; "
            "saved in-flight work can only be settled against its exact identity."
        )
        with self.tx() as db:
            supported = ",".join("?" for _ in SUPPORTED_EXECUTION_CONTRACTS)
            rows = db.execute(
                "SELECT id FROM experiments WHERE deleted_at IS NULL "
                "AND phase NOT IN ('complete','failed','cancelled') "
                f"AND COALESCE(json_extract(contract,'$.execution_contract'),0) NOT IN ({supported}) "
                "AND hold_reason IS NOT 'legacy_contract_archived'",
                SUPPORTED_EXECUTION_CONTRACTS,
            ).fetchall()
            for row in rows:
                eid = row["id"]
                db.execute("UPDATE experiments SET hold_reason='legacy_contract_archived' WHERE id=?", (eid,))
                self.emit(db, eid, "state_changed", {"hold_reason": "legacy_contract_archived"})
                cursor = db.execute(
                    "INSERT OR IGNORE INTO inbox VALUES(?,?,?,?,?)",
                    (str(uuid4()), eid, "required_decision", text, clock.now()),
                )
                if cursor.rowcount:
                    self.emit(db, eid, "notification", {"kind": "required_decision", "text": text})

    def update(self, eid, *, kind="state_changed", **fields):
        allowed = {
            "phase",
            "hold_reason",
            "thread_id",
            "model",
            "snapshot",
            "pending_request",
            "analysis",
            "error",
            "scientific_outcome",
            "title",
            "plan_review",
            "observed_at",
            "pause_agent",
            "pause_compute",
        }
        if not fields.keys() <= allowed:
            raise ValueError("unknown experiment field")
        for key in ("snapshot", "analysis", "pending_request", "plan_review"):
            if key in fields and fields[key] is not None:
                fields[key] = encode(fields[key])
        previous = self.one("SELECT * FROM experiments WHERE id=?", (eid,))
        if not previous:
            raise KeyError(eid)
        archived = archive_active({**previous, "contract": json.loads(previous["contract"])}) and (
            previous["hold_reason"] == "legacy_contract_archived"
        )
        if archived:
            fields.pop("phase", None)
            if "hold_reason" in fields:
                fields["hold_reason"] = "legacy_contract_archived"
        # Async work may finish after a cancellation command committed.
        if previous["phase"] == "cancelled" and kind != "cancellation_status":
            return
        if all(previous[k] == v for k, v in fields.items()):
            return
        with self.tx() as db:
            before = request_obligations.saved_pending(db, eid) if "pending_request" in fields else None
            before_review = review_obligations.saved_review(db, eid) if "plan_review" in fields else None
            db.execute(
                f"UPDATE experiments SET {','.join(k + '=?' for k in fields)} WHERE id=?",
                (*fields.values(), eid),
            )
            if "pending_request" in fields:
                request_obligations.sync_request(db, eid, before=before)
            if "plan_review" in fields:
                review_obligations.sync_review(db, eid, before=before_review)
            if "phase" in fields:
                advancement_decisions.sync_lifetime(db, eid, previous["phase"], fields["phase"])
            if kind == "cancellation_status" and previous["phase"] == "cancelled" and "hold_reason" in fields:
                if fields["hold_reason"] is None:
                    advancement_decisions.settle_cancellation(db, eid)
                else:
                    advancement_decisions.open_cancellation(db, eid, unconfirmed=True)
            self.emit(db, eid, kind, fields)

    def resolve_implementation(self, eid, manifest):
        with self.tx() as db:
            row = db.execute("SELECT contract,snapshot FROM experiments WHERE id=?", (eid,)).fetchone()
            if row["snapshot"]:
                if json.loads(row["snapshot"]) != manifest:
                    raise ValueError("resolved implementation cannot change")
                return
            contract = json.loads(row["contract"])
            contract.update(
                parameters=manifest["parameters"],
                synthetic_demo=manifest["plan"]["synthetic_demo"],
                plan=manifest["plan"],
            )
            if contract.get("execution_contract") == 3:
                db.execute(
                    "DELETE FROM stages WHERE experiment_id=? AND id='validate' AND state='pending'", (eid,)
                )
            graph = [
                *(["plan"] if contract.get("planning_version") else []),
                "prepare",
                *[stage["id"] for stage in manifest["plan"]["stages"]],
                "interpret",
                "publish",
            ]
            for ordinal, stage in enumerate(graph):
                db.execute(
                    "INSERT INTO stages(experiment_id,id,ordinal,updated_at) VALUES(?,?,?,?) "
                    "ON CONFLICT(experiment_id,id) DO UPDATE SET ordinal=excluded.ordinal",
                    (eid, stage, ordinal, clock.now()),
                )
            db.execute(
                "UPDATE experiments SET contract=?,snapshot=? WHERE id=?",
                (encode(contract), encode(manifest), eid),
            )
            self.emit(db, eid, "implementation_frozen", {"snapshot_hash": manifest["snapshot_hash"]})

    def stage(self, eid, stage_id, state, *, error=None, outputs=None):
        if self.experiment(eid)["phase"] == "cancelled":
            return
        with self.tx() as db:
            prior = db.execute(
                "SELECT state FROM stages WHERE experiment_id=? AND id=?", (eid, stage_id)
            ).fetchone()
            if not prior:
                raise KeyError(stage_id)
            if prior[0] == state:
                return
            db.execute(
                "UPDATE stages SET state=?,error=?,output_manifest=?,updated_at=? WHERE experiment_id=? AND id=?",
                (state, error, encode(outputs) if outputs else None, clock.now(), eid, stage_id),
            )
            self.emit(db, eid, "stage_changed", {"stage": stage_id, "state": state, "error": error})

    def cancel_experiment(self, db, eid, now):
        """Retire queued work atomically; dispatched identities remain for reconciliation."""
        exp = self.experiment(eid)
        if exp["phase"] == "complete":
            raise Conflict("a completed experiment cannot be cancelled")
        if exp["phase"] == "cancelled":
            return
        review = exp["plan_review"]
        before_review = dict(review) if review else None
        if review:
            review["state"] = "cancelled"
        db.execute(
            "UPDATE experiments SET phase='cancelled',pause_agent=1,pause_compute=1,"
            "pending_request=NULL,plan_review=?,hold_reason='cancellation_pending',error=NULL WHERE id=?",
            (encode(review) if review else None, eid),
        )
        advancement_decisions.close_lifetime(db, eid, "cancelled")
        advancement_decisions.open_cancellation(db, eid)
        if exp["pending_request"]:
            request_obligations.sync_request(db, eid, before=exp["pending_request"])
        review_obligations.sync_review(db, eid, before=before_review)
        transfers = db.execute(
            "SELECT * FROM execution_transfers "
            "WHERE experiment_id=? AND state NOT IN ('complete','failed','cancelled')",
            (eid,),
        ).fetchall()
        db.execute(
            "UPDATE execution_transfers SET state='cancelled',result='Experiment cancelled',updated_at=? "
            "WHERE experiment_id=? AND state NOT IN ('complete','failed','cancelled')",
            (now, eid),
        )
        for transfer in transfers:
            transfer_obligations.sync_transfer(db, transfer["id"], before=transfer)
        db.execute(
            "UPDATE stages SET state='cancelled',error=NULL,updated_at=? "
            "WHERE experiment_id=? AND state NOT IN ('complete','failed','cancelled')",
            (now, eid),
        )
        pending_actions = db.execute(
            "SELECT * FROM agent_actions WHERE experiment_id=? AND state='pending'", (eid,)
        ).fetchall()
        db.execute(
            "UPDATE agent_actions SET state='superseded',updated_at=? WHERE experiment_id=? AND state='pending'",
            (now, eid),
        )
        for action in pending_actions:
            action_obligations.sync_action(db, action["id"], before=action)
        db.execute(
            "UPDATE steering SET state='cancelled',applied_at=? WHERE experiment_id=? AND state IN ('pending','cancelling')",
            (now, eid),
        )
        cancelled_reports = db.execute(
            "SELECT version FROM report_versions WHERE experiment_id=? "
            "AND state IN ('queued','building','publishing')",
            (eid,),
        ).fetchall()
        db.execute(
            "UPDATE report_versions SET state='failed',error='Experiment cancelled',updated_at=? "
            "WHERE experiment_id=? AND state IN ('queued','building','publishing')",
            (now, eid),
        )
        for report in cancelled_reports:
            report_obligations.sync_report(db, eid, report["version"])
        self.emit(db, eid, "experiment_cancel_requested")

    def branch_workspace_for(self, payload):
        """Decide once, at creation, whether this experiment edits a named branch worktree.

        The answer is saved in the contract, so a later project or busy state cannot move
        an existing run. Only a writable Git project with the main base branch takes this
        path; a non-Git folder, an explicit shared workspace and the synthetic demo
        project keep their old checkout.
        """
        from .engine.gitworkspace import branch_eligible

        project = self.projects.get(payload.get("project_id") or "test-project")
        if not project or not project.writable or not project.checkout:
            return False
        if payload.get("workspace_mode", "auto") == "shared":
            return False
        return branch_eligible(project.checkout)

    def research_loop_for(self, payload, *, branch_workspace):
        """Decide once, at creation, whether this experiment runs the contract-4 loop.

        The owner's project flag enables the loop, and eligibility comes from the
        create-time ``branch_workspace`` derived from the configured checkout's verified
        normal path, never from client input. A flagged project that cannot host the
        named branch loop is refused; it never silently falls back to a contract-3
        experiment.
        """
        project = self.projects.get(payload.get("project_id") or "test-project")
        if not project or not project.research_loop:
            return False
        if not project.writable or not project.checkout:
            raise Conflict("the research loop needs a writable Git project checkout")
        if payload.get("reference_experiment_id"):
            raise Conflict("the research loop does not copy a reference experiment snapshot")
        if payload.get("workspace_mode", "auto") == "shared":
            raise Conflict("the research loop needs its own named branch workspace")
        if not branch_workspace:
            raise Conflict("the research loop needs the project's main Git checkout and base branch")
        return True

    def _integration_branch(self, exp):
        """Return this experiment's exact saved named branch, or refuse.

        A frozen legacy record or a run without a saved branch has no scope the
        integration queue could bind safely.
        """
        if execution_frozen(exp):
            raise Conflict("integration requires a live contract-3 experiment")
        branch = self.one("SELECT * FROM experiment_branches WHERE experiment_id=?", (exp["id"],))
        if not branch:
            raise Conflict("integration requires a named experiment branch")
        return branch

    def _refuse_queued_integration(self, db, exp, repo, kind):
        """Refuse a duplicate request while this experiment's own request is live."""
        views = integration_queue.pending(db, repo)
        active = integration_queue.active(db, repo)
        if active is not None:
            views.append(active)
        if any(view["experiment_id"] == exp["id"] and view["request"].get("kind") == kind for view in views):
            raise Conflict("this experiment's integration is already queued")

    def _enqueue_integration(self, db, exp, branch, request_id, request):
        """Queue one saved request in this command's transaction; no Git work here."""
        project_id = exp["contract"].get("project", {}).get("id", "test-project")
        try:
            return integration_queue.enqueue(
                db,
                branch["repo"],
                request_id,
                request,
                experiment_id=exp["id"],
                project_id=project_id,
            )
        except ValueError as exc:
            raise Conflict(str(exc)) from None

    def _approve_integration(self, db, exp, payload):
        """Move this experiment's exact awaiting-approval candidate to ready; no Git here.

        The command binds the saved experiment, request and candidate commit, so a
        stale or foreign approval cannot become ``ready``. The runner still applies
        only through ``integration.apply``, which revalidates the exact checkout.
        """
        if payload.get("action") != "approve":
            raise Conflict("only the owner approval action is supported")
        candidate_commit = payload.get("candidate_commit")
        if (
            not isinstance(candidate_commit, str)
            or len(candidate_commit) not in (40, 64)
            or any(char not in "0123456789abcdef" for char in candidate_commit)
        ):
            raise Conflict("the integration candidate commit is not exact")
        branch = self._integration_branch(exp)
        try:
            view = integration_queue.saved(db, payload.get("integration_request_id"))
        except ValueError as exc:
            raise Conflict(str(exc)) from None
        candidate = view["candidate"] if view else None
        if (
            view is None
            or view["experiment_id"] != exp["id"]
            or view["repo"] != branch["repo"]
            or view["state"] != "open"
            or view["stage"] != "awaiting_approval"
            or not isinstance(candidate, dict)
            or candidate.get("state") != "ready"
            or candidate.get("candidate_commit") != candidate_commit
        ):
            raise Conflict("only this experiment's exact awaiting approval candidate can be approved")
        try:
            return integration_queue.checkpoint(db, view["request_id"], "ready", status="owner approved")
        except ValueError as exc:
            raise Conflict(str(exc)) from None

    def request_completion_integration(self, eid, *, _db=None):
        """Queue a saved completion decision through the same command as owner integration.

        The worker still owns the project check, approval setting and merge. The
        completion action determines the request ID, so replay cannot queue it again.
        """
        with contextlib.nullcontext(_db) if _db is not None else self.tx() as db:
            exp = self.experiment(eid)
            if (
                exp["phase"] != "complete"
                or exp["deleted_at"] is not None
                or execution_frozen(exp)
                or exp["contract"].get("dont_merge")
            ):
                return None
            research = exp["contract"].get("execution_contract") == 4
            action = self.one(
                "SELECT id,result FROM agent_actions WHERE experiment_id=? AND phase=? "
                "AND state='completed' ORDER BY rowid DESC LIMIT 1",
                (eid, "research" if research else "interpret"),
            )
            if action is None:
                return None
            if research:
                # A checkpoint, pre-completion decision or C3 analysis cannot authorize this merge.
                try:
                    result = json.loads(action["result"] or "null")
                except ValueError:
                    return None
                if not isinstance(result, dict) or result.get("status") != "done":
                    return None
                decision = result.get("integrate")
                if not isinstance(decision, dict) or decision.get("value") is not True:
                    return None
                reason = decision.get("reason")
                if not isinstance(reason, str) or not reason.strip() or len(reason) > 2000:
                    return None
            else:
                analysis = exp["analysis"] or {}
                if not analysis.get("integrate"):
                    return None
                reason = analysis.get("integrate_reason")
            branch = self.one("SELECT * FROM experiment_branches WHERE experiment_id=?", (eid,))
            if branch is None or branch["state"] != "active":
                return None
            views = integration_queue.pending(db, branch["repo"])
            if len(views) >= integration_queue.MAX_QUEUED:
                self.emit(db, eid, "integration_not_queued", {"reason": "repository queue full"})
                return None
            active = integration_queue.active(db, branch["repo"])
            if any(
                view["experiment_id"] == eid and view["request"].get("kind") == "merge"
                for view in views + ([active] if active else [])
            ):
                return None
            payload = {"reason": reason} if reason else {}
            request_id = str(
                uuid5(NAMESPACE_URL, f"experimenttracker:completion-integration:{eid}:{action['id']}")
            )
            return self.command(request_id, "integrate", payload, eid=eid, origin="agent", _db=db)

    def command(self, request_id, kind, payload, *, eid=None, origin="local", experiment_id=None, _db=None):
        """Apply a validated user command atomically. Stable request IDs include a payload conflict check."""
        request_id = str(request_id)
        wire = encode(payload)
        with contextlib.nullcontext(_db) if _db is not None else self.tx() as db:
            prior = db.execute("SELECT * FROM commands WHERE request_id=?", (request_id,)).fetchone()
            if prior:
                if (
                    prior["kind"] != kind
                    or prior["payload"] != wire
                    or (eid and prior["experiment_id"] != eid)
                    or (experiment_id and prior["experiment_id"] != experiment_id)
                ):
                    raise Conflict("request_id was already used for a different command")
                return json.loads(prior["result"])
            if kind == "create" and payload.get("recipe_id") != "agent_experiment_v1":
                raise Conflict("recipe is retired; create a brief-driven experiment")
            if kind == "create" and "execution_contract" in payload:
                requested_contract = payload["execution_contract"]
                if type(requested_contract) is not int or requested_contract != 4:
                    raise Conflict("only explicit execution contract 4 is supported")
            if (
                kind == "create"
                and origin == "gateway"
                and "execution_contract" not in payload
                and _REFUSE_UNBOUND_HOSTED_CREATES
            ):
                raise Conflict(
                    "unbound hosted creates are refused at the contract cutover; resubmit explicitly"
                )
            if db.execute("SELECT 1 FROM autopilot_requests WHERE request_id=?", (request_id,)).fetchone():
                raise Conflict("request_id was already used for an Autopilot command")
            if (
                db.execute("SELECT 1 FROM project_requests WHERE request_id=?", (request_id,)).fetchone()
                or db.execute(
                    "SELECT 1 FROM project_settings_requests WHERE request_id=?", (request_id,)
                ).fetchone()
            ):
                raise Conflict("request_id was already used for another command")
            if db.execute("SELECT 1 FROM project_turns WHERE id=?", (request_id,)).fetchone():
                raise Conflict("request_id was already used for a project knowledge command")
            now = clock.now()
            if kind == "reserve":
                five = payload["five_hour_percent"]
                week = payload["weekly_percent"]
                if not all(
                    isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= 90
                    for value in (five, week)
                ):
                    raise Conflict("subscription reserves must be whole percentages from 0 to 90")
                db.execute(
                    "INSERT INTO quota_settings VALUES(1,?,?,?,?) ON CONFLICT(singleton) DO UPDATE SET "
                    "five_hour_percent=excluded.five_hour_percent,weekly_percent=excluded.weekly_percent,"
                    "request_id=excluded.request_id,updated_at=excluded.updated_at",
                    (five, week, request_id, now),
                )
            elif kind == "create":
                eid = experiment_id or str(uuid4())
                project_id = payload.get("project_id") or "test-project"
                project = self.projects.get(project_id)
                if not project or not project.public()["available"]:
                    raise Conflict("project is unknown or its checkout is unavailable")
                registered = self.one("SELECT * FROM projects WHERE id=?", (project_id,))
                if registered:
                    info = project.checkout.stat()
                    if project.checkout.is_symlink() or (info.st_dev, info.st_ino) != (
                        registered["device"],
                        registered["inode"],
                    ):
                        raise Conflict("project folder identity has changed")
                reference_id = payload.get("reference_experiment_id")
                try:
                    reference = self.experiment(reference_id) if reference_id else None
                except KeyError:
                    raise Conflict("unknown reference experiment") from None
                if reference and (
                    reference["deleted_at"] or reference["phase"] != "complete" or not reference["snapshot"]
                ):
                    raise Conflict("reference must be a completed experiment with a frozen protocol")
                if (
                    reference
                    and reference["contract"].get("project", {}).get("id", "test-project") != project_id
                ):
                    raise Conflict("reference must belong to the selected project")
                # The checkout is decided once, at creation, and saved in the contract:
                # an existing run keeps its own path and a busy project never decides it.
                branch_workspace = self.branch_workspace_for(payload)
                research_loop = self.research_loop_for(payload, branch_workspace=branch_workspace)
                if payload.get("execution_contract") == 4 and not research_loop:
                    raise Conflict(
                        "explicit execution contract 4 is not enabled and eligible for this project"
                    )
                contract = {
                    "schema_version": 1,
                    "recipe_id": "agent_experiment_v1",
                    "resource_policy_version": 2,
                    "recovery_turns": 1,
                    "brief_driven": True,
                    "reporting_version": 1,
                    "planning_version": 1,
                    "execution_contract": 4 if research_loop else 3,
                    "workspace_mode": payload.get("workspace_mode", "auto"),
                }
                if research_loop:
                    # The plan schema and its stage graph belong to contract 3 only.
                    contract.pop("planning_version")
                if branch_workspace:
                    contract["branch_workspace"] = True
                from .execution_settings import ExecutionSettings

                preferences = db.execute(
                    "SELECT execution FROM project_preferences WHERE project_id=?", (project_id,)
                ).fetchone()
                execution = (
                    ExecutionSettings.model_validate(payload["execution"])
                    if payload.get("execution")
                    else ExecutionSettings.model_validate_json(
                        preferences["execution"] if preferences else "{}"
                    )
                )
                contract["execution"] = execution.model_dump()
                contract["project"] = {"id": project.id, "name": project.name}
                if project.checkout and project.writable:
                    contract["project"]["writable"] = True
                if payload.get("model") or payload.get("reasoning_effort"):
                    contract["agent"] = {
                        "model": payload.get("model"),
                        "reasoning_effort": payload.get("reasoning_effort"),
                    }
                if reference:
                    contract["reference"] = {
                        "experiment_id": reference_id,
                        **{
                            field: reference["snapshot"].get(field)
                            for field in ("snapshot_hash", "parameters_hash", "data_hash", "test_hash")
                        },
                    }
                db.execute(
                    "INSERT INTO experiments(id,brief,recipe_id,contract,created_at,updated_at,reference_experiment_id) VALUES(?,?,?,?,?,?,?)",
                    (eid, payload["brief"], payload["recipe_id"], encode(contract), now, now, reference_id),
                )
                advancement_decisions.open_lifetime(db, eid)
                if project.checkout and project.writable:
                    folder = project.checkout.resolve(strict=True)
                    info = folder.stat()
                    db.execute(
                        "INSERT INTO experiment_workspaces VALUES(?,?,?,?,?)",
                        (
                            eid,
                            str(folder),
                            info.st_dev,
                            info.st_ino,
                            str(folder / ".experimenttracker" / "experiments" / eid),
                        ),
                    )
                stage_ids = () if research_loop else ("plan", "prepare", "interpret", "publish")
                db.executemany(
                    "INSERT INTO stages(experiment_id,id,updated_at,ordinal) VALUES(?,?,?,?)",
                    [(eid, stage, now, i) for i, stage in enumerate(stage_ids)],
                )
            else:
                exp = self.experiment(eid)
                if exp["deleted_at"] and kind != "delete":
                    raise Conflict("experiment has been deleted")
                if archive_active(exp) and (
                    kind in ("answer", "plan_review", "steer")
                    or (kind == "action" and payload["action"] != "cancel")
                ):
                    raise Conflict("legacy execution is archived; only cancellation is available")
                if execution_frozen(exp) and (
                    (kind == "action" and payload.get("action") in ("retry_agent", "summarize_title"))
                    or kind == "steer"
                    or (kind == "plan_review" and payload.get("decision") in ("approve", "revise"))
                ):
                    raise Conflict("legacy execution is archived; this mutation is unavailable")
                if execution_frozen(exp) and kind == "answer":
                    pending = exp["pending_request"]
                    saved = (
                        db.execute(
                            "SELECT * FROM agent_actions WHERE id=?", (pending.get("action_id"),)
                        ).fetchone()
                        if pending and pending.get("action_id")
                        else None
                    )
                    if not (
                        pending
                        and exp["phase"] == "complete"
                        and not pending.get("expired")
                        and saved
                        and saved["experiment_id"] == eid
                        and saved["phase"].startswith("discuss:")
                        and saved["state"] in ("dispatching", "running")
                    ):
                        raise Conflict(
                            "legacy execution is archived; only a live discussion reply is available"
                        )
                if (
                    exp["phase"] == "cancelled"
                    and kind not in ("note", "delete")
                    and not (kind == "action" and payload["action"] == "cancel")
                ):
                    raise Conflict("experiment has been cancelled")
                if kind == "discuss":
                    if exp["phase"] != "complete":
                        raise Conflict("discussion is available when the experiment is complete")
                    if not payload["text"].strip():
                        raise Conflict("message must contain text")
                    if (
                        db.execute(
                            "SELECT COUNT(*) FROM discussions WHERE experiment_id=? AND state IN ('queued','waiting','running')",
                            (eid,),
                        ).fetchone()[0]
                        >= 20
                    ):
                        raise Conflict("wait for the queued conversation messages before sending more")
                    seq = db.execute(
                        "SELECT COALESCE(MAX(seq),0)+1 FROM discussions WHERE experiment_id=?", (eid,)
                    ).fetchone()[0]
                    db.execute(
                        "INSERT INTO discussions(id,experiment_id,seq,text,model,reasoning_effort,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                        (
                            request_id,
                            eid,
                            seq,
                            payload["text"],
                            payload.get("model"),
                            payload.get("reasoning_effort"),
                            now,
                            now,
                        ),
                    )
                elif kind == "present":
                    if exp["phase"] != "complete":
                        raise Conflict("a new report version needs a completed experiment")
                    version = db.execute(
                        "SELECT MAX(version) FROM report_versions WHERE experiment_id=?", (eid,)
                    ).fetchone()[0]
                    self._insert_report(db, eid, max(version or 1, 1) + 1, payload["document"], request_id)
                elif kind == "delete":
                    if exp["hold_reason"] in ("cancellation_pending", "cancellation_unconfirmed"):
                        raise Conflict("reconcile cancellation before deleting the experiment")
                    if exp["phase"] not in ("complete", "failed", "cancelled"):
                        raise Conflict("finish or stop the experiment before deleting it")
                    if (
                        db.execute(
                            "SELECT 1 FROM jobs WHERE experiment_id=? AND state IN ('submitting','submitted')",
                            (eid,),
                        ).fetchone()
                        or db.execute(
                            "SELECT 1 FROM agent_actions WHERE experiment_id=? AND state IN ('dispatching','running','uncertain')",
                            (eid,),
                        ).fetchone()
                    ):
                        raise Conflict("reconcile active or uncertain work before deleting the experiment")
                    if db.execute(
                        "SELECT 1 FROM report_versions WHERE experiment_id=? AND state IN ('queued','building','publishing')",
                        (eid,),
                    ).fetchone():
                        raise Conflict("finish report publication before deleting the experiment")
                    db.execute(
                        "UPDATE experiments SET deleted_at=COALESCE(deleted_at,?),pending_request=NULL WHERE id=?",
                        (now, eid),
                    )
                    if exp["pending_request"]:
                        request_obligations.sync_request(db, eid, before=exp["pending_request"])
                    db.execute(
                        "UPDATE discussions SET state='failed',hold_reason=NULL,error='Experiment deleted',updated_at=? WHERE experiment_id=? AND state IN ('queued','waiting')",
                        (now, eid),
                    )
                elif kind == "note":
                    db.execute(
                        "INSERT INTO notes VALUES(?,?,?,?,?)",
                        (request_id, eid, "researcher", payload["text"], now),
                    )
                elif kind == "action":
                    action = payload["action"]
                    if action in ("retry_agent", "repair_implementation"):
                        guidance = self._consume_steering(db, eid)
                        feedback = "\n\n".join(filter(None, (exp["error"], guidance))) or None
                    if action == "cancel":
                        self.cancel_experiment(db, eid, now)
                    elif action == "repair_implementation":
                        if (
                            exp["hold_reason"] not in ("agent_stage_blocked", "agent_stage_failed")
                            or exp["contract"].get("execution_contract") != 3
                            or not exp["snapshot"]
                            or exp["pending_request"]
                            or db.execute(
                                "SELECT 1 FROM agent_actions WHERE experiment_id=? AND state IN ('pending','dispatching','running','uncertain')",
                                (eid,),
                            ).fetchone()
                            or db.execute(
                                "SELECT 1 FROM steering WHERE experiment_id=? AND state IN ('pending','cancelling')",
                                (eid,),
                            ).fetchone()
                        ):
                            raise Conflict(
                                "implementation repair requires a settled agent stage and no pending work"
                            )
                        saved = db.execute(
                            "SELECT * FROM agent_actions WHERE experiment_id=? AND phase LIKE 'agent:%' ORDER BY rowid DESC LIMIT 1",
                            (eid,),
                        ).fetchone()
                        if (
                            not saved
                            or saved["state"] != "completed"
                            or json.loads(saved["result"] or "{}").get("outcome") not in ("blocked", "failed")
                        ):
                            raise Conflict(
                                "implementation repair requires a known failed or blocked agent outcome"
                            )
                        job = db.execute(
                            "SELECT * FROM jobs WHERE experiment_id=? AND stage_id=? AND attempt=? AND state='agent' ORDER BY rowid DESC LIMIT 1",
                            (eid, saved["phase"].removeprefix("agent:"), saved["evidence_revision"]),
                        ).fetchone()
                        if not job:
                            raise Conflict("the saved agent producer is unavailable for repair")
                        db.execute(
                            "UPDATE jobs SET state='failed',error=?,ended_at=? WHERE intent_id=?",
                            (feedback, now, job["intent_id"]),
                        )
                        contract = exp["contract"]
                        contract["manual_repair_requests"] = contract.get("manual_repair_requests", 0) + 1
                        db.execute(
                            "UPDATE experiments SET contract=?,phase='preparing',hold_reason='automatic_repair_pending',error=? WHERE id=?",
                            (encode(contract), feedback, eid),
                        )
                    elif action == "retry_agent" and exp["hold_reason"] in (
                        "implementation_blocked",
                        "agent_stage_blocked",
                        "agent_stage_failed",
                    ):
                        self._resume_blocked(db, exp, request_id, feedback)
                    elif action == "retry_agent":
                        session_failure = db.execute(
                            "SELECT * FROM agent_actions WHERE experiment_id=? AND state='failed' "
                            "AND turn_id IS NULL AND error LIKE 'session resume rejected:%' "
                            "AND phase IN ('plan','prepare','interpret') ORDER BY updated_at DESC LIMIT 1",
                            (eid,),
                        ).fetchone()
                        if exp["hold_reason"] == "agent_session_unavailable" and session_failure:
                            if exp["phase"] in ("complete", "failed", "cancelled") or exp["pending_request"]:
                                raise Conflict(
                                    "session retry requires an active experiment without pending input"
                                )
                            if (
                                db.execute(
                                    "SELECT 1 FROM agent_actions WHERE experiment_id=? "
                                    "AND state IN ('dispatching','running','uncertain')",
                                    (eid,),
                                ).fetchone()
                                or db.execute(
                                    "SELECT 1 FROM steering WHERE experiment_id=? AND state IN ('pending','cancelling')",
                                    (eid,),
                                ).fetchone()
                            ):
                                raise Conflict(
                                    "resolve pending or uncertain work before retrying the session"
                                )
                            before = dict(session_failure)
                            db.execute(
                                "UPDATE agent_actions SET state='pending',error=?,updated_at=? WHERE id=?",
                                (guidance, now, session_failure["id"]),
                            )
                            action_obligations.sync_action(db, session_failure["id"], before=before)
                            db.execute(
                                "UPDATE experiments SET hold_reason=NULL,error=NULL WHERE id=?", (eid,)
                            )
                        elif (
                            exp["phase"] == "failed"
                            and exp["recipe_id"] == "agent_experiment_v1"
                            and exp["hold_reason"]
                            in (
                                "agent_recovery_exhausted",
                                "agent_correction_stalled",
                                "agent_budget_exhausted",
                            )
                        ):
                            failed = db.execute(
                                "SELECT * FROM agent_actions WHERE experiment_id=? AND (phase IN ('plan','prepare','interpret') OR phase LIKE 'agent:%') ORDER BY rowid DESC LIMIT 1",
                                (eid,),
                            ).fetchone()
                            if (
                                not failed
                                or failed["state"] != "failed"
                                or exp["pending_request"]
                                or db.execute(
                                    "SELECT 1 FROM agent_actions WHERE experiment_id=? AND state IN ('dispatching','running','uncertain')",
                                    (eid,),
                                ).fetchone()
                                or db.execute(
                                    "SELECT 1 FROM steering WHERE experiment_id=? AND state IN ('pending','cancelling')",
                                    (eid,),
                                ).fetchone()
                                or not (
                                    (not failed["attempts"] and not failed["turn_id"])
                                    or (
                                        failed["turn_id"]
                                        and str(failed["error"]).startswith(
                                            (
                                                "turn interrupted:",
                                                "turn failed:",
                                                "turn timeout; interrupted",
                                                "agent output validation:",
                                            )
                                        )
                                    )
                                )
                            ):
                                raise Conflict(
                                    "retry requires a known terminal agent outcome without unresolved work"
                                )
                            self.continue_action(
                                dict(failed),
                                request_id=request_id,
                                feedback="\n\n".join(filter(None, (failed["error"], guidance))) or None,
                                _db=db,
                            )
                            if exp["hold_reason"] == "agent_budget_exhausted":
                                contract = exp["contract"]
                                contract["manual_agent_turn_requests"] = (
                                    contract.get("manual_agent_turn_requests", 0) + 1
                                )
                                db.execute(
                                    "UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid)
                                )
                            db.execute(
                                "UPDATE experiments SET phase=?,hold_reason='agent_continuation',error=NULL WHERE id=?",
                                (
                                    "training"
                                    if failed["phase"].startswith("agent:")
                                    else "interpreting"
                                    if failed["phase"] == "interpret"
                                    else "preparing",
                                    eid,
                                ),
                            )
                            db.execute(
                                "UPDATE stages SET state='pending',error=NULL,updated_at=? WHERE experiment_id=? AND id=?",
                                (now, eid, failed["phase"].removeprefix("agent:")),
                            )
                        else:
                            if (
                                exp["phase"] != "failed"
                                or exp["recipe_id"] != "agent_experiment_v1"
                                or not exp["snapshot"]
                                or exp["hold_reason"] not in ("compute_failed", "validation_failed")
                            ):
                                raise Conflict("agent retry requires failed agent or compute work")
                            if (
                                exp["pending_request"]
                                or db.execute(
                                    "SELECT 1 FROM agent_actions WHERE experiment_id=? AND state IN ('dispatching','running','uncertain')",
                                    (eid,),
                                ).fetchone()
                                or db.execute(
                                    "SELECT 1 FROM steering WHERE experiment_id=? AND state IN ('pending','cancelling')",
                                    (eid,),
                                ).fetchone()
                            ):
                                raise Conflict("resolve pending or uncertain work before requesting repair")
                            contract = exp["contract"]
                            contract["manual_repair_requests"] = contract.get("manual_repair_requests", 0) + 1
                            db.execute(
                                "UPDATE experiments SET contract=?,phase='preparing',hold_reason='automatic_repair_pending',error=? WHERE id=?",
                                (encode(contract), feedback, eid),
                            )
                    elif action == "summarize_title":
                        if db.execute(
                            "SELECT 1 FROM agent_actions WHERE experiment_id=? AND phase IN ('title','summarize_title') "
                            "AND state IN ('pending','dispatching','running','uncertain')",
                            (eid,),
                        ).fetchone():
                            raise Conflict("a title summary is already pending or needs reconciliation")
                        revision = db.execute(
                            "SELECT COALESCE(MAX(evidence_revision),0)+1 FROM agent_actions "
                            "WHERE experiment_id=? AND phase='summarize_title'",
                            (eid,),
                        ).fetchone()[0]
                        prompt = title_prompt(
                            exp["brief"],
                            exp["contract"].get("project", {}).get("name", "Project"),
                            (exp["snapshot"] or {}).get("plan", {}).get("summary") or exp["title"] or "",
                        )
                        db.execute(
                            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at) "
                            "VALUES(?,?,'summarize_title',?,'pending',?,?,?)",
                            (request_id, eid, revision, encode({"prompt": prompt}), now, now),
                        )
                        action_obligations.sync_action(db, request_id)
                    else:
                        column = "pause_agent" if action.endswith("agent") else "pause_compute"
                        db.execute(
                            f"UPDATE experiments SET {column}=? WHERE id=?", (action.startswith("pause"), eid)
                        )
                elif kind == "answer":
                    pending = exp["pending_request"]
                    resumes_stage = exp["contract"].get("execution_contract") == 3 and exp["hold_reason"] in (
                        "implementation_blocked",
                        "agent_stage_blocked",
                        "agent_stage_failed",
                    )
                    if (
                        not pending
                        or pending["id"] != payload["pending_request_id"]
                        or (pending.get("expired") and not resumes_stage)
                    ):
                        raise Conflict("stale or mismatched pending request")
                    try:
                        request_answer(pending, payload["answer"])
                    except ValueError as exc:
                        raise Conflict(str(exc)) from None
                    if (
                        exp["contract"].get("execution_contract") == 4
                        and pending.get("kind") == "approval"
                        and (origin not in ("local", "gateway") or pending.get("answer"))
                    ):
                        raise Conflict("only the owner can decide a research approval once")
                    if resumes_stage:
                        # A settled turn has no live RPC to answer, even if its request survived restart.
                        # Resuming clears the request, so record the answer that fulfills it.
                        pending["answer"] = payload["answer"]
                        db.execute(
                            "INSERT INTO steering(request_id,experiment_id,text,created_at,mode) VALUES(?,?,?,?,'later')",
                            (request_id, eid, payload["answer"], now),
                        )
                        guidance = self._consume_steering(db, eid)
                        resumed = self._resume_blocked(
                            db,
                            exp,
                            request_id,
                            guidance,
                            resolve_request=True,
                        )
                        self.emit(
                            db,
                            eid,
                            "steering_resumed_blocked_work",
                            {
                                "request_id": request_id,
                                "action_id": resumed["id"],
                                "source": "answer",
                            },
                        )
                    else:
                        before = dict(pending)
                        pending["answer"] = payload["answer"]
                        db.execute(
                            "UPDATE experiments SET pending_request=? WHERE id=?", (encode(pending), eid)
                        )
                        request_obligations.sync_request(db, eid, before=before)
                        if exp["contract"].get("execution_contract") == 4 and pending.get("rpc_id") is None:
                            # A saved contract 4 answer is that experiment's first event
                            # wake: record it in the answer's own transaction so a restart
                            # keeps the window next to the answer it debounces.
                            from .research import wakeups

                            wakeups.record(
                                self, eid, wakeups.OWNER_REQUEST_ANSWERED, pending["id"], db=db, now=now
                            )
                elif kind == "plan_review":
                    review = exp["plan_review"]
                    if not review or review.get("state") != "awaiting_review":
                        raise Conflict("the plan is not awaiting review")
                    if payload["plan_revision"] != review["revision"]:
                        raise Conflict("plan revision is stale")
                    if db.execute(
                        "SELECT 1 FROM jobs WHERE experiment_id=? AND attempt=?",
                        (eid, exp["evidence_revision"]),
                    ).fetchone():
                        raise Conflict("a submitted experiment plan cannot be changed by plan review")
                    before_review = dict(review)
                    decision = payload["decision"]
                    if decision == "approve":
                        if review.get("questions"):
                            raise Conflict("answer the planning questions before approval")
                        review["state"] = "approved"
                    elif decision == "revise":
                        feedback = (payload.get("feedback") or "").strip()
                        if not feedback:
                            raise Conflict("revision feedback is required")
                        review.update(
                            state="drafting", scope="plan", revision=review["revision"] + 1, feedback=feedback
                        )
                        review.pop("preparation_revision", None)
                        db.execute(
                            "UPDATE stages SET state='pending',updated_at=? WHERE experiment_id=? AND id IN ('plan','prepare')",
                            (now, eid),
                        )
                    elif decision == "cancel":
                        self.cancel_experiment(db, eid, now)
                        review["state"] = "cancelled"
                    else:
                        raise Conflict("unknown plan decision")
                    db.execute(
                        "UPDATE experiments SET plan_review=?,hold_reason=? WHERE id=?",
                        (
                            encode(review),
                            "plan_review"
                            if decision == "approve"
                            else "cancellation_pending"
                            if decision == "cancel"
                            else None,
                            eid,
                        ),
                    )
                    review_obligations.sync_review(db, eid, before=before_review)
                elif kind == "steer":
                    text = payload["text"].strip()
                    if not text:
                        raise Conflict("steering text is required")
                    mode = payload.get("mode", "later")
                    contract4 = exp["contract"].get("execution_contract") == 4
                    if contract4 and mode == "restart":
                        raise Conflict("restart steering is not supported for contract 4 research sessions")
                    if contract4 and len(text.encode()) > 1000:
                        raise Conflict("research steering must be at most 1000 UTF-8 bytes")
                    blocked_work = not contract4 and exp["hold_reason"] in (
                        "implementation_blocked",
                        "agent_stage_blocked",
                        "agent_stage_failed",
                    )
                    if exp["phase"] in ("complete", "failed", "cancelled") and not (
                        exp["phase"] == "failed" and (mode == "restart" or blocked_work)
                    ):
                        raise Conflict("steering requires an active experiment or a restart of a failed run")
                    if db.execute(
                        "SELECT 1 FROM steering WHERE experiment_id=? AND mode='restart' AND state IN ('pending','cancelling')",
                        (eid,),
                    ).fetchone():
                        raise Conflict("a restart is already pending for this experiment")
                    if mode not in ("later", "restart"):
                        raise Conflict("unknown steering mode")
                    if (
                        db.execute(
                            "SELECT COUNT(*) FROM steering WHERE experiment_id=? AND state='pending'",
                            (eid,),
                        ).fetchone()[0]
                        >= 20
                    ):
                        raise Conflict("wait for the saved steering requests before sending more")
                    db.execute(
                        "INSERT INTO steering(request_id,experiment_id,text,created_at,mode) VALUES(?,?,?,?,?)",
                        (request_id, eid, text, now, mode),
                    )
                    if blocked_work:
                        guidance = self._consume_steering(db, eid, request_id=request_id)
                        resumed = self._resume_blocked(
                            db,
                            exp,
                            request_id,
                            guidance,
                            resolve_request=True,
                        )
                        self.emit(
                            db,
                            eid,
                            "steering_resumed_blocked_work",
                            {
                                "request_id": request_id,
                                "action_id": resumed["id"],
                                "source": "steer",
                            },
                        )
                    elif mode == "restart":
                        db.execute(
                            "UPDATE experiments SET pause_compute=1,hold_reason='steering_restart' WHERE id=?",
                            (eid,),
                        )
                elif kind == "integrate":
                    if exp["contract"].get("dont_merge"):
                        raise Conflict("this experiment is marked Don't merge")
                    branch = self._integration_branch(exp)
                    if branch["state"] != "active":
                        raise Conflict("only an active experiment branch can be integrated")
                    reason = payload.get("reason")
                    if reason is not None and (
                        not isinstance(reason, str) or not reason.strip() or len(reason) > 2000
                    ):
                        raise Conflict("integration reason must be bounded text")
                    self._refuse_queued_integration(db, exp, branch["repo"], "merge")
                    request = _integration_request(exp, branch["branch"], "merge", reason=reason)
                    self._enqueue_integration(db, exp, branch, request_id, request)
                elif kind == "revert_integration":
                    branch = self._integration_branch(exp)
                    merged = branch["merged_commit"]
                    if branch["state"] != "merged" or not merged:
                        raise Conflict("only an applied integration can be reverted")
                    if len(merged) not in (40, 64) or any(char not in "0123456789abcdef" for char in merged):
                        raise Conflict("the saved integration merge commit is not exact")
                    self._refuse_queued_integration(db, exp, branch["repo"], "revert")
                    request = _integration_request(exp, branch["branch"], "revert", merge_commit=merged)
                    self._enqueue_integration(db, exp, branch, request_id, request)
                elif kind == "approve_integration":
                    self._approve_integration(db, exp, payload)
                elif kind == "dont_merge":
                    value = payload.get("value")
                    if not isinstance(value, bool):
                        raise Conflict("the Don't merge flag requires a boolean value")
                    if execution_frozen(exp):
                        raise Conflict("integration requires a live contract-3 experiment")
                    branch = self.one("SELECT * FROM experiment_branches WHERE experiment_id=?", (eid,))
                    if value and branch:
                        active = integration_queue.active(db, branch["repo"])
                        if (
                            active is not None
                            and active["experiment_id"] == eid
                            and active["request"].get("kind") == "merge"
                        ):
                            raise Conflict(
                                "this experiment's integration is already being applied; "
                                "revert it after it lands"
                            )
                        for view in integration_queue.pending(db, branch["repo"]):
                            if view["experiment_id"] == eid and view["request"].get("kind") == "merge":
                                integration_queue.finish(db, view["request_id"], "abandoned")
                    contract = exp["contract"]
                    if value:
                        contract["dont_merge"] = True
                    else:
                        contract.pop("dont_merge", None)
                    db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
                else:
                    raise ValueError("unknown command")
                phase = db.execute("SELECT phase FROM experiments WHERE id=?", (eid,)).fetchone()[0]
                advancement_decisions.sync_lifetime(db, eid, exp["phase"], phase)
            result = {"schema_version": 1, "experiment_id": eid, "request_id": request_id, "state": "applied"}
            db.execute(
                "INSERT INTO commands VALUES(?,?,?,?,?,?,?,?)",
                (request_id, eid, kind, origin, wire, "applied", encode(result), now),
            )
            if eid:
                self.emit(db, eid, "command_applied", {"kind": kind, "request_id": request_id})
            return result

    def activity(self, action, item_id, kind, summary, state):
        now = clock.now()
        eid = action["experiment_id"]
        action = {**action, "phase": action["phase"].removeprefix("agent:")}
        with self.tx() as db:
            prior = db.execute(
                "SELECT summary,state FROM stage_activity WHERE action_id=? AND item_id=?",
                (action["id"], item_id),
            ).fetchone()
            if prior and (prior["summary"], prior["state"]) == (summary, state):
                return
            # Notifications can be delivered after completion. Never regress a completed item.
            if prior and prior["state"] != "running" and state == "running":
                return
            db.execute(
                """INSERT INTO stage_activity VALUES(?,?,?,?,?,?,?,?,?)
                ON CONFLICT(action_id,item_id) DO UPDATE SET summary=excluded.summary,state=excluded.state,updated_at=excluded.updated_at""",
                (action["id"], item_id, eid, action["phase"], kind, summary, state, now, now),
            )
            db.execute(
                """DELETE FROM stage_activity WHERE experiment_id=? AND stage_id=? AND rowid NOT IN
                (SELECT rowid FROM stage_activity WHERE experiment_id=? AND stage_id=? ORDER BY created_at DESC LIMIT 60)""",
                (eid, action["phase"], eid, action["phase"]),
            )
            self.emit(db, eid, "stage_activity", {"stage": action["phase"]})

    def notify(self, eid, kind, text):
        with self.tx() as db:
            cursor = db.execute(
                "INSERT OR IGNORE INTO inbox VALUES(?,?,?,?,?)", (str(uuid4()), eid, kind, text, clock.now())
            )
            if cursor.rowcount:
                self.emit(db, eid, "notification", {"kind": kind, "text": text})

    def backup(self, directory: Path):
        """Use SQLite's online backup API; atomically replace a self-contained backup on shared storage."""
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        if (directory / "workflow.sqlite").resolve() == self.path.resolve():
            raise ValueError("backup destination must differ from authoritative state")
        fd, name = tempfile.mkstemp(prefix="workflow-", suffix=".pending", dir=directory)
        os.close(fd)
        tmp = Path(name)
        try:
            with self.lock, contextlib.closing(sqlite3.connect(tmp)) as target:
                self.db.backup(target, pages=256)
            tmp.replace(directory / "workflow.sqlite")
        finally:
            tmp.unlink(missing_ok=True)

    def publish_backup(self, directory: Path):
        """Independent online reader; close the local snapshot before shared I/O."""
        if (directory / "workflow.sqlite").resolve() == self.path.resolve():
            raise ValueError("backup destination must differ from authoritative state")
        with tempfile.TemporaryDirectory(prefix="backup-", dir=self.path.parent) as local:
            reader = self.read_only(self.path)
            try:
                reader.backup(Path(local))
            finally:
                reader.close()
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            fd, name = tempfile.mkstemp(prefix="workflow-", suffix=".pending", dir=directory)
            os.close(fd)
            pending = Path(name)
            try:
                shutil.copyfile(Path(local) / "workflow.sqlite", pending)
                pending.replace(directory / "workflow.sqlite")
            finally:
                pending.unlink(missing_ok=True)

    def close(self):
        self.db.close()

    def _insert_report(self, db, eid, version, document, request_id):
        now = clock.now()
        db.execute(
            "INSERT INTO report_versions(experiment_id,version,state,document,request_id,created_at,updated_at) VALUES(?,?,'queued',?,?,?,?)",
            (eid, version, encode(document), request_id, now, now),
        )
        report_obligations.sync_report(db, eid, version)

    def working_report(self, eid, document, request_id, *, _db=None):
        """Idempotent report intent independent of requested compute completion."""
        with contextlib.nullcontext(_db) if _db is not None else self.tx() as db:
            existing = db.execute(
                "SELECT version,document FROM report_versions WHERE experiment_id=? AND request_id=?",
                (eid, request_id),
            ).fetchone()
            if existing:
                if existing["document"] != encode(document):
                    raise Conflict("report request identity already belongs to another document")
                return existing["version"]
            version = db.execute(
                "SELECT COALESCE(MAX(version),0)+1 FROM report_versions WHERE experiment_id=?", (eid,)
            ).fetchone()[0]
            self._insert_report(db, eid, version, document, request_id)
            self.emit(db, eid, "report_requested", {"report_version": version})
            return version

    def report_state(self, eid, version, state, error=None):
        if self.experiment(eid)["phase"] == "cancelled":
            return
        with self.tx() as db:
            changed = db.execute(
                "UPDATE report_versions SET state=?,error=?,updated_at=? WHERE experiment_id=? AND version=?",
                (state, error, clock.now(), eid, version),
            )
            if changed.rowcount:
                report_obligations.sync_report(db, eid, version)
            if state == "ready" and version > 1:
                db.execute(
                    "INSERT OR IGNORE INTO inbox VALUES(?,?,?,?,?)",
                    (str(uuid4()), eid, "report_ready", f"Report version {version} is ready.", clock.now()),
                )
            self.emit(db, eid, "report_updated", {"report_version": version, "state": state})
