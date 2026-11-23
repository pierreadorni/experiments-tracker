"""Project authority and research state, advanced by the existing SQLite owner."""

import json
import math
from uuid import NAMESPACE_URL, uuid4, uuid5

from . import autopilot_policy, clock
from .autopilot_budget import WEEK, Budget
from .autopilot_models import AutopilotCommand, AutopilotPolicy
from .autopilot_policy import calendar
from .store import Conflict, encode

DOCUMENTS = {
    "research_plan",
    "literature",
    "protocol",
    "claims",
    "backlog",
    "paper.tex",
    "references.bib",
    "weekly_report",
    "handoff",
    "guidelines",
}


def migrate(db):
    db.execute("ALTER TABLE agent_actions ADD COLUMN autopilot_control_revision INTEGER")
    statements = [
        """CREATE TABLE autopilot_runs(
            id TEXT PRIMARY KEY, project_id TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'draft',
            revision INTEGER NOT NULL DEFAULT 0, policy_revision INTEGER NOT NULL DEFAULT 1,
            control_revision INTEGER NOT NULL DEFAULT 0, phase TEXT NOT NULL DEFAULT 'foundation',
            foundation TEXT NOT NULL DEFAULT 'pending', foundation_refs TEXT NOT NULL DEFAULT '{}',
            borrowed_percent REAL NOT NULL DEFAULT 0, input_cursor INTEGER NOT NULL DEFAULT 0,
            consumed_cursor INTEGER NOT NULL DEFAULT 0, next_report_at REAL, next_review_at REAL,
            hold_reason TEXT, central_argument TEXT, uncertainty TEXT, handoff TEXT,
            created_at REAL NOT NULL, updated_at REAL NOT NULL)""",
        "CREATE UNIQUE INDEX one_autopilot_run ON autopilot_runs(project_id) WHERE status NOT IN ('completed','stopped')",
        """CREATE TABLE autopilot_policies(
            run_id TEXT NOT NULL REFERENCES autopilot_runs(id), revision INTEGER NOT NULL,
            policy TEXT NOT NULL, author TEXT NOT NULL, created_at REAL NOT NULL,
            PRIMARY KEY(run_id,revision))""",
        """CREATE TABLE autopilot_requests(
            request_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, run_id TEXT,
            payload TEXT NOT NULL, author TEXT NOT NULL, state TEXT NOT NULL,
            result TEXT, error TEXT, created_at REAL NOT NULL)""",
        """CREATE TABLE autopilot_members(
            experiment_id TEXT PRIMARY KEY REFERENCES experiments(id), run_id TEXT NOT NULL REFERENCES autopilot_runs(id),
            project_id TEXT NOT NULL, backlog_id TEXT NOT NULL, purpose TEXT NOT NULL, rationale TEXT NOT NULL,
            priority INTEGER NOT NULL, policy_revision INTEGER NOT NULL, deferred INTEGER NOT NULL DEFAULT 0,
            defer_reason TEXT, deferred_stages TEXT NOT NULL DEFAULT '[]', created_at REAL NOT NULL)""",
        """CREATE TABLE autopilot_backlog(
            run_id TEXT NOT NULL REFERENCES autopilot_runs(id), id TEXT NOT NULL, revision INTEGER NOT NULL,
            document TEXT NOT NULL, PRIMARY KEY(run_id,id))""",
        """CREATE TABLE autopilot_decisions(
            id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES autopilot_runs(id), author TEXT NOT NULL,
            action_id TEXT, policy_revision INTEGER NOT NULL, document TEXT NOT NULL, created_at REAL NOT NULL)""",
        """CREATE TABLE autopilot_events(
            id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES autopilot_runs(id), revision INTEGER NOT NULL,
            kind TEXT NOT NULL, payload TEXT NOT NULL, created_at REAL NOT NULL, delivered_at REAL,
            UNIQUE(run_id,revision))""",
        """CREATE TABLE autopilot_inputs(
            run_id TEXT NOT NULL REFERENCES autopilot_runs(id), seq INTEGER NOT NULL, identity TEXT NOT NULL,
            kind TEXT NOT NULL, document TEXT NOT NULL, created_at REAL NOT NULL,
            PRIMARY KEY(run_id,seq), UNIQUE(run_id,identity))""",
        """CREATE TABLE autopilot_actions(
            id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES autopilot_runs(id), cursor INTEGER NOT NULL,
            control_revision INTEGER NOT NULL, state TEXT NOT NULL, input TEXT, result TEXT, error TEXT,
            thread_id TEXT, turn_id TEXT, interrupt_reason TEXT, observed_at REAL, pending_request TEXT,
            created_at REAL NOT NULL, updated_at REAL NOT NULL)""",
        "CREATE UNIQUE INDEX one_autopilot_action ON autopilot_actions(run_id) WHERE state IN ('pending','dispatching','running','uncertain','received')",
        """CREATE TABLE autopilot_allocations(
            intent_id TEXT PRIMARY KEY REFERENCES jobs(intent_id), project_id TEXT NOT NULL,
            run_id TEXT NOT NULL REFERENCES autopilot_runs(id), requested_gpus INTEGER NOT NULL,
            reserved_seconds INTEGER NOT NULL, state TEXT NOT NULL DEFAULT 'reserved',
            cluster TEXT, job_id TEXT, started_at REAL, accounted_until REAL, allocated_gpus INTEGER,
            source TEXT, observed_at REAL, anomaly TEXT)""",
        """CREATE TABLE autopilot_accounting(
            intent_id TEXT NOT NULL REFERENCES autopilot_allocations(intent_id), observed_at REAL NOT NULL,
            cluster TEXT NOT NULL, job_id TEXT NOT NULL, document TEXT NOT NULL,
            PRIMARY KEY(intent_id,observed_at))""",
        """CREATE TABLE autopilot_stage_holds(
            experiment_id TEXT NOT NULL REFERENCES experiments(id), stage_id TEXT NOT NULL,
            attempt INTEGER NOT NULL, reason TEXT NOT NULL, detail TEXT NOT NULL,
            updated_at REAL NOT NULL, PRIMARY KEY(experiment_id,stage_id,attempt))""",
        """CREATE TABLE autopilot_documents(
            run_id TEXT NOT NULL REFERENCES autopilot_runs(id), name TEXT NOT NULL, version INTEGER NOT NULL,
            request_id TEXT NOT NULL, author TEXT NOT NULL, action_id TEXT, policy_revision INTEGER NOT NULL,
            content TEXT NOT NULL, sources TEXT NOT NULL, created_at REAL NOT NULL,
            PRIMARY KEY(run_id,name,version), UNIQUE(request_id,name))""",
        """CREATE TABLE autopilot_notices(
            id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES autopilot_runs(id), kind TEXT NOT NULL,
            text TEXT NOT NULL, resolved_at REAL, created_at REAL NOT NULL)""",
        """CREATE TABLE autopilot_transitions(
            run_id TEXT NOT NULL REFERENCES autopilot_runs(id), policy_revision INTEGER NOT NULL,
            boundary TEXT NOT NULL, nominal_at REAL NOT NULL, observed_at REAL NOT NULL, gaps TEXT NOT NULL,
            PRIMARY KEY(run_id,policy_revision,boundary))""",
    ]
    statements.extend(
        [
            "CREATE TABLE autopilot_deliveries(identity TEXT PRIMARY KEY, delivered_at REAL NOT NULL)",
            "CREATE TABLE autopilot_files(id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES autopilot_runs(id), request_id TEXT NOT NULL UNIQUE, name TEXT NOT NULL, path TEXT NOT NULL, size INTEGER NOT NULL, created_at REAL NOT NULL)",
        ]
    )
    for statement in statements:
        db.execute(statement)


class Autopilot:
    # The policy checks live in ``autopilot_policy``; these names keep their callers.
    authority = autopilot_policy.member_authority
    quota = autopilot_policy.reserves
    require_slot = autopilot_policy.require_slot

    def __init__(self, store, config=None):
        self.store, self.config = store, config
        self.budget = Budget(store)

    def run(self, project_id=None, run_id=None):
        row = (
            self.store.one("SELECT * FROM autopilot_runs WHERE id=?", (run_id,))
            if run_id
            else self.store.one(
                "SELECT * FROM autopilot_runs WHERE project_id=? ORDER BY created_at DESC,rowid DESC LIMIT 1",
                (project_id,),
            )
        )
        if not row:
            raise KeyError("No Autopilot run")
        return row

    def policy(self, run):
        return AutopilotPolicy.model_validate_json(
            self.store.one(
                "SELECT policy FROM autopilot_policies WHERE run_id=? AND revision=?",
                (run["id"], run["policy_revision"]),
            )["policy"]
        )

    def enabled(self, project_id):
        row = self.store.one(
            "SELECT status FROM autopilot_runs WHERE project_id=? AND status NOT IN ('completed','stopped')",
            (project_id,),
        )
        return bool(row and row["status"] != "draft")

    def emit(self, db, run_id, kind, detail=None):
        now = clock.now()
        db.execute("UPDATE autopilot_runs SET revision=revision+1,updated_at=? WHERE id=?", (now, run_id))
        run = self.run(run_id=run_id)
        db.execute(
            "INSERT INTO autopilot_events VALUES(?,?,?,?,?,?,NULL)",
            (str(uuid4()), run_id, run["revision"], kind, encode(detail or {}), now),
        )

    def wake(self, db, run_id, identity, kind, document=None):
        run = self.run(run_id=run_id)
        seq = run["input_cursor"] + 1
        added = db.execute(
            "INSERT OR IGNORE INTO autopilot_inputs VALUES(?,?,?,?,?,?)",
            (run_id, seq, identity, kind, encode(document or {}), clock.now()),
        ).rowcount
        if added:
            db.execute("UPDATE autopilot_runs SET input_cursor=? WHERE id=?", (seq, run_id))

    def view(self, project_id, now=None):
        now = clock.now() if now is None else now
        try:
            run = self.run(project_id)
        except KeyError:
            return {"schema_version": 1, "available": True, "run": None, "pending_commands": []}
        policy = self.policy(run)
        documents = self.store.rows(
            "SELECT name,MAX(version) AS version FROM autopilot_documents WHERE run_id=? GROUP BY name",
            (run["id"],),
        )
        members = self.store.rows(
            "SELECT m.*,e.phase,e.pause_agent,e.pause_compute,e.hold_reason FROM autopilot_members m JOIN experiments e ON e.id=m.experiment_id WHERE m.run_id=? ORDER BY m.priority DESC,m.created_at",
            (run["id"],),
        )
        return {
            "schema_version": 1,
            "available": True,
            "run": run,
            "policy": policy.model_dump(mode="json"),
            "calendar": calendar(policy, now, run["borrowed_percent"]),
            "budget": self.budget.view(project_id, policy, now),
            "documents": documents,
            "files": self.store.rows(
                "SELECT id,name,size,created_at FROM autopilot_files WHERE run_id=? ORDER BY created_at DESC LIMIT 20",
                (run["id"],),
            ),
            "backlog": [
                {**r, "document": json.loads(r["document"])}
                for r in self.store.rows("SELECT * FROM autopilot_backlog WHERE run_id=?", (run["id"],))
            ],
            "experiments": members,
            "stage_holds": self.store.rows(
                "SELECT h.* FROM autopilot_stage_holds h JOIN autopilot_members m ON m.experiment_id=h.experiment_id WHERE m.run_id=?",
                (run["id"],),
            ),
            "notices": self.store.rows(
                "SELECT * FROM autopilot_notices WHERE run_id=? ORDER BY created_at DESC LIMIT 50",
                (run["id"],),
            ),
            "decisions": [
                {**r, "document": json.loads(r["document"])}
                for r in self.store.rows(
                    "SELECT * FROM autopilot_decisions WHERE run_id=? ORDER BY created_at DESC LIMIT 30",
                    (run["id"],),
                )
            ],
            "responses": self.store.rows(
                "SELECT id,cursor,result,created_at FROM autopilot_actions WHERE run_id=? AND state='completed' AND result IS NOT NULL ORDER BY created_at DESC LIMIT 20",
                (run["id"],),
            ),
            "messages": self.store.rows(
                "SELECT * FROM autopilot_inputs WHERE run_id=? AND kind IN ('message','answer') ORDER BY seq DESC LIMIT 30",
                (run["id"],),
            ),
            "actions": self.store.rows(
                "SELECT id,state,error,interrupt_reason,created_at FROM autopilot_actions WHERE run_id=? ORDER BY created_at DESC LIMIT 20",
                (run["id"],),
            ),
            "observed_at": now,
            "pending_commands": [],
        }

    def receipt(self, request_id, project_id, payload, author):
        prior = self.store.one("SELECT * FROM autopilot_requests WHERE request_id=?", (request_id,))
        if prior and (
            prior["project_id"] != project_id
            or prior["payload"] != encode(payload)
            or prior["author"] != author
        ):
            raise Conflict("request identity already used with different payload or attribution")
        if not prior:
            for table, key in (
                ("commands", "request_id"),
                ("project_requests", "request_id"),
                ("project_settings_requests", "request_id"),
                ("project_turns", "id"),
                ("maintenance", "request_id"),
            ):
                if self.store.one(f"SELECT 1 FROM {table} WHERE {key}=?", (request_id,)):
                    raise Conflict("request identity already belongs to another domain command")
        return prior

    def command(self, project_id, body, origin="researcher"):
        body = AutopilotCommand.model_validate(body)
        payload = body.model_dump(mode="json", exclude_none=True)
        request_id = str(body.request_id)
        with self.store.tx() as db:
            prior = self.receipt(request_id, project_id, payload, origin)
            if prior:
                if prior["error"]:
                    raise Conflict(prior["error"])
                return json.loads(prior["result"])
            if (
                project_id not in self.store.projects
                or not self.store.projects[project_id].public()["available"]
            ):
                raise Conflict("project is unavailable")
            try:
                run = self.run(project_id)
            except KeyError:
                run = None
            if body.kind == "configure" and (not run or run["status"] in ("completed", "stopped")):
                if body.expected_revision != 0 or not body.policy:
                    raise Conflict("new draft requires policy and expected_revision 0")
                if (
                    self.store.one(
                        "SELECT 1 FROM autopilot_members m JOIN jobs j ON j.experiment_id=m.experiment_id WHERE m.project_id=? AND j.state IN ('submitting','submitted','agent')",
                        (project_id,),
                    )
                    or self.store.one(
                        "SELECT 1 FROM autopilot_actions WHERE run_id IN (SELECT id FROM autopilot_runs WHERE project_id=?) AND state IN ('dispatching','running','uncertain')",
                        (project_id,),
                    )
                    or self.store.one(
                        "SELECT 1 FROM autopilot_members m JOIN agent_actions a ON a.experiment_id=m.experiment_id WHERE m.project_id=? AND a.state IN ('dispatching','running','uncertain')",
                        (project_id,),
                    )
                ):
                    raise Conflict("reconcile previous run's submitted and uncertain work before a new run")
                if self.store.one(
                    "SELECT 1 FROM autopilot_allocations WHERE project_id=? AND state!='settled'",
                    (project_id,),
                ):
                    raise Conflict("reconcile outstanding charged work before starting another run")
                run_id = str(uuid5(NAMESPACE_URL, "autopilot:" + request_id))
                db.execute(
                    "INSERT INTO autopilot_runs(id,project_id,created_at,updated_at) VALUES(?,?,?,?)",
                    (run_id, project_id, clock.now(), clock.now()),
                )
                db.execute(
                    "INSERT INTO autopilot_policies VALUES(?,1,?,?,?)",
                    (run_id, encode(body.policy.model_dump()), origin, clock.now()),
                )
                run = self.run(run_id=run_id)
            else:
                if not run or body.expected_revision != run["revision"]:
                    raise Conflict("Autopilot revision is stale; refresh project")
                run_id = run["id"]
                policy = self.policy(run)
                if body.kind in ("configure", "revise"):
                    if run["status"] in ("stopped", "completed"):
                        raise Conflict("terminal runs retain their authority history; configure a new run")
                    if not body.policy or (body.kind == "configure" and run["status"] != "draft"):
                        raise Conflict("policy revision requires a complete policy")
                    if run["status"] != "draft" and body.policy.anchor_at != policy.anchor_at:
                        raise Conflict("weekly anchor is immutable after activation")
                    # The anchor belongs to the project, including subsequent runs.
                    first = self.store.one(
                        "SELECT p.policy FROM autopilot_policies p JOIN autopilot_runs r ON r.id=p.run_id WHERE r.project_id=? AND r.status!='draft' ORDER BY r.created_at,p.revision LIMIT 1",
                        (project_id,),
                    )
                    if first and body.policy.anchor_at != json.loads(first["policy"])["anchor_at"]:
                        raise Conflict("project allowance anchor cannot be reset")
                    revision = run["policy_revision"] + 1
                    db.execute(
                        "INSERT INTO autopilot_policies VALUES(?,?,?,?,?)",
                        (run_id, revision, encode(body.policy.model_dump()), origin, clock.now()),
                    )
                    reserve_only = body.policy.model_dump(
                        exclude={"five_hour_reserve", "weekly_reserve"}
                    ) == policy.model_dump(exclude={"five_hour_reserve", "weekly_reserve"})
                    db.execute(
                        "UPDATE autopilot_runs SET policy_revision=?,control_revision=control_revision+1,borrowed_percent=CASE WHEN ? THEN borrowed_percent ELSE 0 END WHERE id=?",
                        (revision, reserve_only, run_id),
                    )
                    if (
                        run["status"] in ("handoff_ready", "handoff_incomplete")
                        and body.policy.handoff_at > clock.now()
                        and body.policy.handoff_at != policy.handoff_at
                    ):
                        db.execute(
                            "UPDATE autopilot_runs SET status='active',handoff=NULL WHERE id=?", (run_id,)
                        )
                    self.wake(db, run_id, request_id, "policy_revision")
                elif body.kind == "activate":
                    if run["status"] != "draft":
                        raise Conflict("only a draft can be activated")
                    first = self.store.one(
                        "SELECT p.policy FROM autopilot_policies p JOIN autopilot_runs r ON r.id=p.run_id WHERE r.project_id=? AND r.id!=? AND r.status!='draft' ORDER BY r.created_at,p.revision LIMIT 1",
                        (project_id, run_id),
                    )
                    if first and policy.anchor_at != json.loads(first["policy"])["anchor_at"]:
                        raise Conflict("project allowance anchor cannot be reset")
                    db.execute(
                        "UPDATE autopilot_runs SET status='active',control_revision=control_revision+1,next_report_at=? WHERE id=?",
                        (policy.weekly_report_at or policy.start_at + WEEK, run_id),
                    )
                    # The lead is a contract 4 experiment created in this transaction;
                    # its id derives from the run, so a replay never creates another.
                    self.store.command(
                        autopilot_policy.lead_request_id(run_id),
                        "create",
                        {
                            "brief": autopilot_policy.lead_brief(policy),
                            "recipe_id": "agent_experiment_v1",
                            "project_id": project_id,
                            "execution_contract": 4,
                            "autopilot_lead": {"run_id": run_id},
                            "model": policy.model,
                            "reasoning_effort": policy.reasoning_effort,
                        },
                        origin=autopilot_policy.LEAD_ORIGIN,
                        experiment_id=autopilot_policy.lead_id(run_id),
                        _db=db,
                    )
                    self.wake(db, run_id, request_id, "activation")
                elif body.kind in ("pause", "resume", "stop", "complete"):
                    allowed = {
                        "pause": {"active", "handoff_ready", "handoff_incomplete"},
                        "resume": {"paused"},
                        "stop": {"active", "paused", "handoff_ready", "handoff_incomplete"},
                        "complete": {"handoff_ready", "handoff_incomplete"},
                    }
                    if run["status"] not in allowed[body.kind]:
                        raise Conflict("invalid Autopilot lifecycle transition")
                    state = {
                        "pause": "paused",
                        "resume": "active",
                        "stop": "stopped",
                        "complete": "completed",
                    }[body.kind]
                    if body.kind == "resume" and clock.now() >= policy.handoff_at:
                        state = "handoff_incomplete"
                    db.execute(
                        "UPDATE autopilot_runs SET status=?,control_revision=control_revision+1 WHERE id=?",
                        (state, run_id),
                    )
                    if body.kind == "resume":
                        self.wake(db, run_id, request_id, "resume")
                elif body.kind in ("message", "answer"):
                    if not body.text.strip():
                        raise Conflict("message is empty")
                    if body.kind == "answer":
                        if not db.execute(
                            "UPDATE autopilot_notices SET resolved_at=? WHERE id=? AND run_id=? AND resolved_at IS NULL",
                            (clock.now(), body.input_id, run_id),
                        ).rowcount:
                            raise Conflict("input request is not pending")
                    pending_action = (
                        self.store.one(
                            "SELECT * FROM autopilot_actions WHERE run_id=? AND pending_request IS NOT NULL ORDER BY created_at DESC LIMIT 1",
                            (run_id,),
                        )
                        if body.kind == "answer"
                        else None
                    )
                    pending = json.loads(pending_action["pending_request"]) if pending_action else None
                    if pending and pending["id"] == body.input_id and not pending.get("expired"):
                        from .models import request_answer

                        request_answer(pending, body.text)
                        pending["answer"] = body.text
                        db.execute(
                            "UPDATE autopilot_actions SET pending_request=? WHERE id=?",
                            (encode(pending), pending_action["id"]),
                        )
                    self.wake(db, run_id, request_id, body.kind, {"text": body.text, "author": origin})
                    if body.kind == "message":
                        db.execute(
                            "UPDATE autopilot_runs SET control_revision=control_revision+1 WHERE id=?",
                            (run_id,),
                        )
                elif body.kind == "control":
                    self.control(
                        db, run, str(body.experiment_id), body.control, body.text, request_id, origin
                    )
            self.emit(db, run_id, "researcher_" + body.kind, {"request_id": request_id})
            result = {
                "schema_version": 1,
                "request_id": request_id,
                "run_id": run_id,
                "state": "applied",
                "revision": self.run(run_id=run_id)["revision"],
            }
            db.execute(
                "INSERT INTO autopilot_requests VALUES(?,?,?,?,?,'applied',?,NULL,?)",
                (request_id, project_id, run_id, encode(payload), origin, encode(result), clock.now()),
            )
            return result

    def member(self, eid):
        return self.store.one("SELECT * FROM autopilot_members WHERE experiment_id=?", (eid,))

    def control_revision(self, eid):
        member = self.member(eid)
        return self.run(run_id=member["run_id"])["control_revision"] if member else None

    def control(self, db, run, eid, operation, reason, request_id, author):
        member = self.member(eid)
        if not member or member["run_id"] != run["id"]:
            raise Conflict("experiment is not owned by this Autopilot run")
        if not reason or not reason.strip():
            raise Conflict("control requires a reason")
        exp = self.store.experiment(eid)
        if exp["phase"] in ("cancelled", "complete") and operation != "defer":
            raise Conflict("terminal work cannot be resumed or redirected")
        if operation in ("defer", "resume"):
            if operation == "resume":
                self.require_slot(run)
            db.execute(
                "UPDATE autopilot_members SET deferred=?,defer_reason=? WHERE experiment_id=?",
                (operation == "defer", reason, eid),
            )
            self.store.emit(
                db,
                eid,
                "autopilot_" + operation,
                {"request_id": request_id, "author": author, "reason": reason},
            )
        elif operation == "abandon":
            if author.startswith("agent:") and not self.policy(run).may_abandon:
                raise Conflict("researcher has not granted scoped abandonment")
            self.store.command(request_id, "action", {"action": "cancel"}, eid=eid, origin=author, _db=db)
        elif operation == "steer":
            self.store.command(
                request_id, "steer", {"text": reason, "mode": "later"}, eid=eid, origin=author, _db=db
            )
        else:
            raise Conflict("unsupported experiment control")
        db.execute("UPDATE autopilot_runs SET control_revision=control_revision+1 WHERE id=?", (run["id"],))

    def document(self, run_id, name, version=None):
        row = self.store.one(
            "SELECT * FROM autopilot_documents WHERE run_id=? AND name=? "
            + ("AND version=?" if version else "ORDER BY version DESC LIMIT 1"),
            (run_id, name, version) if version else (run_id, name),
        )
        if not row:
            raise KeyError("document version is unavailable")
        return {**row, "sources": json.loads(row["sources"])}

    def context(self, run):
        view = self.view(run["project_id"])
        view["changes"] = self.store.rows(
            "SELECT * FROM autopilot_inputs WHERE run_id=? AND seq>? ORDER BY seq LIMIT 100",
            (run["id"], run["consumed_cursor"]),
        )
        view["instructions"] = self.policy(run).instructions
        view["project_guidance"] = self.store.one(
            "SELECT instructions,revision FROM project_preferences WHERE project_id=?", (run["project_id"],)
        )
        view["accepted_tool_results"] = self.store.rows(
            "SELECT request_id,author,result FROM autopilot_requests WHERE run_id=? AND author LIKE 'agent:%' AND state='applied' ORDER BY created_at DESC LIMIT 100",
            (run["id"],),
        )
        # Full documents and experiment details are fetched on demand.
        return view

    def validate_sources(self, run, sources):
        for source in sources:
            if source.kind == "literature":
                if (
                    not source.id.startswith("https://")
                    or not source.retrieved_at
                    or not source.access
                    or not source.title
                ):
                    raise Conflict(
                        "literature requires verified URL, title, retrieval date and explicit read depth"
                    )
                continue
            if source.kind == "experiment":
                exp = self.store.experiment(source.id)
            elif source.kind in ("output", "artifact"):
                row = self.store.one(
                    "SELECT experiment_id FROM "
                    + ("output_refs" if source.kind == "output" else "artifacts")
                    + " WHERE id=?",
                    (source.id,),
                )
                if not row:
                    raise Conflict("unknown evidence reference")
                exp = self.store.experiment(row["experiment_id"])
            else:
                if not source.version or not self.store.one(
                    "SELECT 1 FROM report_versions WHERE experiment_id=? AND version=? AND state='ready'",
                    (source.id, source.version),
                ):
                    raise Conflict("unknown ready report version")
                exp = self.store.experiment(source.id)
            if exp["contract"].get("project", {}).get("id", "test-project") != run["project_id"]:
                raise Conflict("evidence must belong to the project")
            if (
                exp["contract"].get("synthetic_demo")
                or (exp["snapshot"] or {}).get("plan", {}).get("synthetic_demo")
                or self.config is not None
                and self.config.mode == "demo"
            ):
                raise Conflict("synthetic demonstration outputs cannot support research claims")

    def update_document(self, db, run, request_id, document, author, action_id=None):
        """Save the next version of one logical document from its current base version."""
        if document.name not in DOCUMENTS:
            raise Conflict("unknown logical document name")
        current = (
            self.store.one(
                "SELECT MAX(version) AS version FROM autopilot_documents WHERE run_id=? AND name=?",
                (run["id"], document.name),
            )["version"]
            or 0
        )
        if document.base_version != current:
            raise Conflict("document base version is stale")
        self.validate_sources(run, document.sources)
        db.execute(
            "INSERT INTO autopilot_documents VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                run["id"],
                document.name,
                current + 1,
                request_id,
                author,
                action_id,
                run["policy_revision"],
                document.text,
                encode([s.model_dump() for s in document.sources]),
                clock.now(),
            ),
        )
        return {"versions": {document.name: current + 1}}

    def project_status(self, run, document=None):
        """The lead's bounded read: run status text, pending objectives and versions."""
        result = {
            "status": autopilot_policy.project_status(self.store.db, run["id"]),
            "pending_objectives": [],
            "documents": {
                row["name"]: row["version"]
                for row in self.store.rows(
                    "SELECT name,MAX(version) AS version FROM autopilot_documents WHERE run_id=? GROUP BY name",
                    (run["id"],),
                )
            },
            "reports": {
                row["experiment_id"]: row["version"]
                for row in self.store.rows(
                    "SELECT m.experiment_id,MAX(v.version) AS version FROM autopilot_members m "
                    "JOIN report_versions v ON v.experiment_id=m.experiment_id AND v.state='ready' "
                    "WHERE m.run_id=? GROUP BY m.experiment_id",
                    (run["id"],),
                )
            },
        }
        for row in self.store.rows(
            "SELECT e.id,e.pending_request FROM experiments e JOIN autopilot_members m "
            "ON m.experiment_id=e.id WHERE m.run_id=? AND e.pending_request IS NOT NULL LIMIT 20",
            (run["id"],),
        ):
            pending = json.loads(row["pending_request"])
            if pending.get("kind") == "approval" and not pending.get("answer"):
                result["pending_objectives"].append(
                    {
                        "experiment_id": row["id"],
                        "approval_request_id": pending["id"],
                        "objective": pending.get("params", {}).get("objective"),
                    }
                )
        if document is not None:
            result["document"] = self.document(run["id"], document)
        return result

    def owned_member(self, run, eid):
        member = self.member(eid)
        if not member or member["run_id"] != run["id"] or member["project_id"] != run["project_id"]:
            raise Conflict("experiment is not a member of this Autopilot run")
        return member

    def invoke(self, action_id, name, arguments):
        """Apply one lead project tool for the lead's saved research action."""
        from .autopilot_tools import MODELS

        if name not in MODELS:
            raise Conflict("tool is outside the lead's project allowlist")
        args = MODELS[name].model_validate(arguments)
        action = self.store.one(
            "SELECT a.experiment_id,e.contract FROM agent_actions a JOIN experiments e ON e.id=a.experiment_id "
            "WHERE a.id=?",
            (action_id,),
        )
        run_id = autopilot_policy.lead_run_id(json.loads(action["contract"] or "{}")) if action else None
        if not run_id or action["experiment_id"] != autopilot_policy.lead_id(run_id):
            raise Conflict("unknown Autopilot lead action")
        run = self.run(run_id=run_id)
        if name == "project_status":
            return self.project_status(run, args.document)
        request_id = str(args.request_id)
        payload = {"name": name, "arguments": args.model_dump(mode="json")}
        author = "agent:" + action_id
        with self.store.tx() as db:
            prior = self.receipt(request_id, run["project_id"], payload, author)
            if prior:
                return json.loads(prior["result"])
            run = self.run(run_id=run_id)
            saved = db.execute(
                "SELECT state,autopilot_control_revision FROM agent_actions WHERE id=?", (action_id,)
            ).fetchone()
            if (
                saved["state"] not in ("dispatching", "running")
                or saved["autopilot_control_revision"] != run["control_revision"]
            ):
                raise Conflict("project turn authority changed; refresh at a safe continuation")
            if run["status"] not in autopilot_policy.ADMITTING:
                raise Conflict("Autopilot is held")
            policy = self.policy(run)
            now = clock.now()
            result = {}
            if name == "create_experiment":
                autopilot_policy.create_checks(self, run, policy, now, args.estimated_finish_at, args.purpose)
                eid = str(uuid5(NAMESPACE_URL, "autopilot-experiment:" + request_id))
                result = self.store.command(
                    request_id,
                    "create",
                    {
                        "brief": args.brief + "\n\nAutopilot purpose: " + args.purpose + ".",
                        "recipe_id": "agent_experiment_v1",
                        "project_id": run["project_id"],
                        "execution_contract": 4,
                        "model": policy.model,
                        "reasoning_effort": policy.reasoning_effort,
                    },
                    origin=author,
                    experiment_id=eid,
                    _db=db,
                )
                db.execute(
                    "INSERT INTO autopilot_members VALUES(?,?,?,'',?,'',0,?,0,NULL,'[]',?)",
                    (eid, run["id"], run["project_id"], args.purpose, run["policy_revision"], now),
                )
                self.store.emit(
                    db,
                    eid,
                    "autopilot_linked",
                    {"run_id": run["id"], "policy_revision": run["policy_revision"]},
                )
            elif name == "message_experiment":
                eid = str(args.experiment_id)
                self.owned_member(run, eid)
                self.store.command(request_id, "steer", {"text": args.text}, eid=eid, origin=author, _db=db)
            elif name == "request_integration":
                eid = str(args.experiment_id)
                self.owned_member(run, eid)
                self.store.command(
                    request_id, "integrate", {"reason": args.reason}, eid=eid, origin=author, _db=db
                )
            elif name == "update_document":
                result = self.update_document(db, run, request_id, args, author, action_id)
            elif name == "approve_experiment_objective":
                result = self.approve(db, run, args, author, action_id)
            elif name == "control_experiment":
                self.control(
                    db, run, str(args.experiment_id), args.operation, args.reason, request_id, author
                )
                # The lead's own exact control is accepted; externally changed authority stays stale.
                db.execute(
                    "UPDATE agent_actions SET autopilot_control_revision=(SELECT control_revision "
                    "FROM autopilot_runs WHERE id=?) WHERE id=?",
                    (run["id"], action_id),
                )
            if name in ("create_experiment", "control_experiment"):
                db.execute(
                    "INSERT INTO autopilot_decisions VALUES(?,?,?,?,?,?,?)",
                    (request_id, run["id"], author, action_id, run["policy_revision"], encode(payload), now),
                )
            self.emit(db, run["id"], name, {"request_id": request_id})
            result = {**result, "request_id": request_id, "revision": self.run(run_id=run["id"])["revision"]}
            db.execute(
                "INSERT INTO autopilot_requests VALUES(?,?,?,?,?,'applied',?,NULL,?)",
                (request_id, run["project_id"], run["id"], encode(payload), author, encode(result), now),
            )
            if name == "approve_experiment_objective":
                self.hand_off(db, run, result["approval"], action_id)
            return result

    def approve(self, db, run, args, author, action_id):
        eid = str(args.experiment_id)
        self.owned_member(run, eid)
        exp = self.store.experiment(eid)
        if exp["contract"].get("execution_contract") != 4:
            raise Conflict("C4 approval requires a contract-4 experiment")
        pending = exp["pending_request"] or None
        if (
            not pending
            or pending.get("id") != str(args.approval_request_id)
            or pending.get("kind") != "approval"
            or pending.get("params", {}).get("objective") != args.objective
            or pending.get("answer")
        ):
            raise Conflict("C4 approval request is not currently pending")
        requests = db.execute(
            "SELECT payload FROM events WHERE experiment_id=? AND kind='owner_approval_requested' "
            "AND json_extract(payload,'$.request_id')=? LIMIT 2",
            (eid, str(args.approval_request_id)),
        ).fetchall()
        if len(requests) != 1:
            raise Conflict("approval request is not one exact saved C4 request")
        requested = json.loads(requests[0]["payload"])
        if (
            requested.get("experiment_id") != eid
            or requested.get("project_id") != run["project_id"]
            or requested.get("brief") != exp["brief"]
            or requested.get("objective") != args.objective
        ):
            raise Conflict("approval does not match the saved request and objective")
        if db.execute(
            "SELECT 1 FROM events WHERE experiment_id=? AND kind='autopilot_objective_approved' "
            "AND json_extract(payload,'$.request_id')=? LIMIT 1",
            (eid, str(args.approval_request_id)),
        ).fetchone():
            raise Conflict("this C4 request already has an orchestrator decision")
        approval = {
            "request_id": str(args.approval_request_id),
            "experiment_id": eid,
            "run_id": run["id"],
            "policy_revision": run["policy_revision"],
            "control_revision": run["control_revision"],
            "objective": args.objective,
        }
        self.store.emit(
            db, eid, "autopilot_objective_approved", {**approval, "author": author, "action_id": action_id}
        )
        return {"approval": approval}

    def hand_off(self, db, run, approval, action_id):
        """Answer the member's pending request from the exact saved proof and wake the member."""
        from .core import request_obligations
        from .research import wakeups
        from .research.autopilot_approval import approved

        eid, request_id = approval["experiment_id"], approval["request_id"]
        if approved(self.store, eid, request_id, approval["objective"], db=db) is None:
            return
        pending = self.store.experiment(eid)["pending_request"]
        before = dict(pending)
        pending["answer"] = "Approved by the project orchestrator."
        pending["answer_source"] = {"kind": "orchestrator", "run_id": run["id"], "action_id": action_id}
        db.execute("UPDATE experiments SET pending_request=? WHERE id=?", (encode(pending), eid))
        request_obligations.sync_request(db, eid, before=before)
        wakeups.record(self.store, eid, wakeups.ORCHESTRATOR_APPROVAL, request_id, db=db)

    def tick(self, now=None):
        """Calendar, current reports and result wakeups never require inference."""
        now = clock.now() if now is None else now
        for run in self.store.rows(
            "SELECT * FROM autopilot_runs WHERE status IN ('active','paused','handoff_ready','handoff_incomplete')"
        ):
            policy = self.policy(run)
            schedule = calendar(policy, now, run["borrowed_percent"])
            with self.store.tx() as db:
                for phase, boundary in schedule["boundaries"].items():
                    if now < boundary:
                        continue
                    if db.execute(
                        "INSERT OR IGNORE INTO autopilot_transitions VALUES(?,?,?,?,?,?)",
                        (
                            run["id"],
                            run["policy_revision"],
                            phase,
                            boundary,
                            now,
                            encode({"foundation": run["foundation"], "uncertainty": run["uncertainty"]}),
                        ),
                    ).rowcount:
                        self.wake(
                            db,
                            run["id"],
                            f"phase:{run['policy_revision']}:{phase}",
                            "phase_boundary",
                            {"phase": phase},
                        )
                        self.emit(db, run["id"], "phase_boundary", {"phase": phase, "nominal_at": boundary})
                db.execute("UPDATE autopilot_runs SET phase=? WHERE id=?", (schedule["phase"], run["id"]))
                if run["next_report_at"] and now >= run["next_report_at"]:
                    # One current report covers missed intervals, never a backlog of model turns.
                    interval = math.floor((now - run["next_report_at"]) / WEEK)
                    due = run["next_report_at"] + interval * WEEK
                    self.wake(
                        db,
                        run["id"],
                        f"report:{due}",
                        "weekly_report",
                        {"covers_since": run["next_report_at"], "due_at": due},
                    )
                    db.execute(
                        "UPDATE autopilot_runs SET next_report_at=? WHERE id=?", (due + WEEK, run["id"])
                    )
                    self.emit(db, run["id"], "weekly_report_due")
                if run["next_review_at"] and now >= run["next_review_at"]:
                    self.wake(db, run["id"], f"review:{run['next_review_at']}", "scheduled_review")
                    db.execute("UPDATE autopilot_runs SET next_review_at=NULL WHERE id=?", (run["id"],))
                if schedule["handoff_due"] and run["handoff"] is None:
                    docs = self.store.rows(
                        "SELECT name,MAX(version) AS version FROM autopilot_documents WHERE run_id=? GROUP BY name",
                        (run["id"],),
                    )
                    handoff = {
                        "published_at": now,
                        "deadline_at": policy.handoff_at,
                        "incomplete": True,
                        "summary": "Latest available draft; automatic research deadline reached. Researcher review required.",
                        "documents": docs,
                        "foundation": run["foundation"],
                        "holds": run["hold_reason"],
                        "late_work": self.store.rows(
                            "SELECT j.intent_id,j.job_id,j.state FROM jobs j JOIN autopilot_members m ON m.experiment_id=j.experiment_id WHERE m.run_id=? AND j.state IN ('submitting','submitted')",
                            (run["id"],),
                        ),
                    }
                    db.execute(
                        "UPDATE autopilot_runs SET status=?,handoff=?,control_revision=control_revision+1 WHERE id=?",
                        (
                            "paused" if run["status"] == "paused" else "handoff_incomplete",
                            encode(handoff),
                            run["id"],
                        ),
                    )
                    db.execute(
                        "INSERT OR IGNORE INTO autopilot_notices VALUES(?,?,'handoff',?,NULL,?)",
                        ("handoff:" + run["id"], run["id"], handoff["summary"], now),
                    )
                    self.emit(db, run["id"], "deadline_handoff", handoff)
