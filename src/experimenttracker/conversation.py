"""Durable, evidence-grounded discussion turns sharing the workflow's single agent slot."""

import asyncio
import hashlib
import json
import sys
import time
from pathlib import Path
from uuid import NAMESPACE_URL, uuid4, uuid5

from pydantic import ValidationError

from . import artifacts, discussion_outputs, iteration, reporting
from .codex_client import NeedsAttention
from .models import DiscussionReply
from .store import encode


class CorrectableReply(ValueError):
    """A terminal model reply can be corrected without changing saved scientific evidence."""


class Conversation:
    def __init__(self, workflow):
        self.workflow = workflow
        self.store = workflow.store

    def hold(self, identity, reason, error=None):
        self.store.discussion_state(identity, "waiting", hold_reason=reason, error=error)

    def prompt(self, exp, message):
        root = self.workflow.root(exp["id"])
        artifacts.verify_snapshot(root, exp["snapshot"])
        records = self.store.rows(
            "SELECT * FROM artifacts WHERE experiment_id=?"
            + ("" if iteration.enabled(exp) else " AND evidence_revision=?"),
            (exp["id"],) if iteration.enabled(exp) else (exp["id"], exp["evidence_revision"]),
        )
        evidence = reporting.Evidence(exp, root, records)
        if not iteration.enabled(exp):
            for record in records:
                evidence.read(record["id"])
        current = self.store.one(
            "SELECT version,document FROM report_versions WHERE experiment_id=? AND state='ready' ORDER BY version DESC LIMIT 1",
            (exp["id"],),
        )
        history = self.store.rows(
            "SELECT text,reply,error,report_version FROM discussions WHERE experiment_id=? AND seq<? ORDER BY seq DESC LIMIT 20",
            (exp["id"], message["seq"]),
        )
        payload = {
            "phase": "discuss",
            "project_knowledge": self.workflow.supervisor.context(
                exp["contract"].get("project", {}).get("id", "test-project")
            ),
            "project_context": self.workflow.project_context(exp),
            "execution_context": self.workflow.execution_context(exp),
            "project_instructions": self.workflow.projects.instructions(
                exp["contract"].get("project", {}).get("id", "test-project")
            ),
            "brief": exp["brief"],
            "message": message["text"],
            "history": list(reversed(history)),
            "manifest": exp["snapshot"],
            "evidence_revision": exp["evidence_revision"],
            "results": artifacts.read_json(root / "results.json", root),
            "analysis": exp["analysis"],
            "artifacts": [{k: a[k] for k in ("id", "name", "path", "size", "sha256")} for a in records],
            "base_report_version": current["version"] if current else 1,
            "current_report_document": json.loads(current["document"]) if current else None,
            "analysis_workspace": str(discussion_outputs.workspace(root, message["id"])),
            "python": sys.executable,
            "experiment_python": str(self.workflow.experiment_python(exp)),
        }
        instruction = (
            "You are discussing this completed experiment with its researcher. The latest message is your instruction. "
            "When project_context.access is read-write, the researcher authorizes edits in the selected "
            "project folder. Work from it and implement requested project changes, preserving unrelated edits. "
            "Read-only project context must remain unchanged. "
            "Follow project_instructions as researcher guidance for this project. "
            "Answer it directly, using the saved evidence and conversation context. You have normal Codex execution tools: "
            "run small scripts, inspect files and figures, transform arbitrary data, fit small models or perform other "
            "requested analyses in analysis_workspace. Use python (with Matplotlib) or experiment_python if its environment "
            "exists; you may install needed dependencies into an environment inside analysis_workspace. "
            "Distinguish measured results, interpretations, assumptions and suggested next steps. Cite registered evidence_ids "
            "supporting factual claims; do not invent results, uncertainty or completed work. A synthetic demo remains synthetic. "
            "For an explicit request to add/change a plot or revise the report, return the full revised presentation document "
            "with the requested change, keeping useful existing content. The executor renders and publishes it as a new version "
            "after validation; explain the change in reply without claiming it is already published. For discussion alone, leave "
            "presentation null. The built-in plot blocks are conveniences, not a limit on what you can visualize. "
            "For arbitrary plots use scripts (e.g. Matplotlib), export SVG or PNG, inspect the image for readability, and "
            "insert a figure block with artifact_id 'generated:FILENAME'. Return generated_artifacts containing each file's "
            "basename, exact SHA-256 and registered source evidence_ids. Save the script and any new measurements as outputs "
            "too, so the analysis can be reproduced. All declared outputs must be regular files at analysis_workspace's top "
            "level, at most 10 MiB each; provenance.json is reserved. You can also cite 'generated:FILENAME' in evidence_ids. "
            "The executor registers and publishes these exact bytes, preserving old evidence and reports. Keep generated "
            "analysis outputs in analysis_workspace; project source edits may also use the authorized project folder. "
            "Never change old artifacts or unrelated projects. Label new local measurements and their methods honestly. "
            "Estimate compute before running it: for work likely to take hours, suggest a separate linked experiment so "
            "its jobs and results have their own progress. When the user asks you to start a follow-up, set new_experiment "
            "with a self-contained brief incorporating the relevant context; this is your create_experiment action. "
            "The executor queues it once with this experiment as reference and the selected model/effort, using the normal "
            "resource envelope and fresh quota checks. A suggestion alone should leave new_experiment null. Scheduler "
            "submissions go through that workflow, not direct sbatch calls. No billed APIs or credential access. "
            "Return DiscussionReply JSON with a natural concise reply, evidence_ids, optional presentation, "
            "generated_artifacts and optional new_experiment. Avoid JSON/provenance dumps in the reply.\n\n"
            + reporting.skill_text()
        )
        return instruction + "\nPAYLOAD:\n" + encode(payload)

    def turn_prompt(self, action, thread_id):
        """Keep validation inputs local; send only bounded context and changes to this thread."""
        instruction, raw = action["input"].split("\nPAYLOAD:\n", 1)
        payload = json.loads(raw)
        exp = self.store.experiment(action["experiment_id"])
        context = {}
        # Each large section has its own immutable file so reading metrics does not also
        # load every report block, source manifest and project experiment into the turn.
        detailed = {
            "manifest",
            "results",
            "analysis",
            "artifacts",
            "current_report_document",
            "project_knowledge",
            "execution_context",
            "brief",
            "project_instructions",
            "project_context",
        }
        for key in detailed:
            context[key + "_detail"] = self.workflow.context_pointer(exp, "discussion-" + key, payload[key])
        for key in ("project_context", "project_instructions", "evidence_revision", "base_report_version"):
            value = payload[key]
            if len(encode(value)) <= 2000:
                context[key] = value
        if "project_context" not in context:
            context["project_context"] = {
                key: payload["project_context"][key] for key in ("access", "checkout")
            }
        context["brief_summary"] = payload["brief"][:1200]
        context["summary"] = str(payload["analysis"].get("summary", ""))[:1600]
        metrics = payload["results"].get("metrics", [])
        if isinstance(metrics, dict):
            metrics = [
                {"variant": variant, "name": name, "value": value, "unit": ""}
                for variant, row in metrics.items()
                for name, value in row.items()
            ]
        context["metric_preview"] = {
            "columns": ["variant", "name", "value", "unit"],
            "rows": [[m.get(k) for k in ("variant", "name", "value", "unit")] for m in metrics[:24]],
            "total": len(metrics),
        }
        # Bound unusual metric labels/values too. The complete measurements stay in results_detail.
        if len(encode(context["metric_preview"])) > 6000:
            context["metric_preview"] = {"total": len(metrics), "rows": []}
        fingerprints = {
            key: hashlib.sha256(encode(value).encode()).hexdigest() for key, value in context.items()
        }
        prior = self.store.one(
            "SELECT input FROM agent_actions WHERE experiment_id=? AND phase LIKE 'discuss:%' "
            "AND thread_id=? AND turn_id IS NOT NULL AND state IN ('completed','failed') "
            "AND id<>? ORDER BY created_at DESC LIMIT 1",
            (exp["id"], thread_id, action["id"]),
        )
        previous = (
            json.loads(prior["input"].split("\nPAYLOAD:\n", 1)[1]).get("_sent_context", {}) if prior else {}
        )
        changes = {key: value for key, value in context.items() if fingerprints[key] != previous.get(key)}
        changes.update({key: None for key in previous if key not in fingerprints})
        turn = {
            "phase": "discuss",
            "message": payload["message"],
            "analysis_workspace": payload["analysis_workspace"],
            "python": payload["python"],
            "experiment_python": payload["experiment_python"],
            "context_mode": "update" if previous else "initial",
            "context": changes,
            "context_index": self.workflow.context_pointer(exp, "discussion-index", context),
        }
        if not previous:
            turn["history_detail"] = self.workflow.context_pointer(
                exp, "discussion-history", payload["history"]
            )
        if action.get("previous_action_id"):
            prior_action = self.store.one(
                "SELECT error FROM agent_actions WHERE id=?", (action["previous_action_id"],)
            )
            turn["publication_feedback"] = prior_action["error"]
        guidance = (
            "Context detail files contain exact saved inputs, accessible with normal file tools. "
            "Read only relevant sections/metrics; avoid dumping entire files. The metric_preview is a bounded "
            "preview, not the full evidence. Read artifacts_detail to find registered evidence IDs and paths. "
            "Read current_report_document_detail before revising a report. Prior messages are already in this "
            "thread; history_detail is for recovery only. Context updates replace prior values; null removes them. "
            "Use context_index to recover current detail pointers if earlier context was compacted. "
            "Latest detail files supersede previews; follow the latest project_instructions_detail. "
            "Preserve the experiment's synthetic/research distinction and project access rules. "
        )
        prefix = (
            instruction
            if not previous
            else (
                "Continue this experiment discussion using the existing thread and instructions. "
                "Answer the latest message. Return DiscussionReply JSON. "
            )
        )
        # The full payload remains the authoritative validation/replay input in SQLite.
        wire = prefix + "\n" + guidance + "\nPAYLOAD:\n" + encode(turn)
        payload["_sent_context"] = fingerprints
        payload["_sent_prompt"] = wire
        saved = instruction + "\nPAYLOAD:\n" + encode(payload)
        return wire, saved

    def validate(self, action, result):
        try:
            reply = DiscussionReply.model_validate(result)
        except ValidationError as exc:
            errors = [
                {"location": ".".join(map(str, e["loc"])), "message": e["msg"]}
                for e in exc.errors(include_input=False, include_url=False)[:20]
            ]
            raise CorrectableReply("DiscussionReply validation: " + encode(errors)) from exc
        payload = json.loads(action["input"].split("\nPAYLOAD:\n", 1)[1])
        exp = self.store.experiment(action["experiment_id"])
        if exp["snapshot"] != payload["manifest"] or exp["evidence_revision"] != payload["evidence_revision"]:
            raise ValueError("discussion's saved scientific context changed")
        root = self.workflow.root(exp["id"])
        artifacts.verify_snapshot(root, exp["snapshot"])
        records = self.store.rows(
            "SELECT * FROM artifacts WHERE experiment_id=?"
            + ("" if iteration.enabled(exp) else " AND evidence_revision=?"),
            (exp["id"],) if iteration.enabled(exp) else (exp["id"], exp["evidence_revision"]),
        )
        generated, files = discussion_outputs.prepare(
            exp, root, action["phase"].split(":", 1)[1], action, reply, payload
        )
        evidence = reporting.Evidence(exp, root, records + generated)
        for saved in payload["artifacts"]:
            current = evidence.records.get(saved["id"])
            if not current or any(current[k] != saved[k] for k in ("name", "sha256", "size")):
                raise ValueError("discussion evidence identity changed")
            if not iteration.enabled(exp):
                evidence.read(saved["id"])
        if not set(reply.evidence_ids) <= {a["id"] for a in payload["artifacts"] + generated}:
            raise CorrectableReply("discussion cites evidence outside its saved context")
        if reply.presentation:
            try:
                reporting.validate_document(evidence, reply.presentation)
            except ValueError as exc:
                raise CorrectableReply("Report validation: " + str(exc)) from exc
        for identity in reply.evidence_ids:
            evidence.read(identity)
        return reply, payload, files

    def complete(self, message, action):
        reply, payload, files = self.validate(action, json.loads(action["result"]))
        eid = message["experiment_id"]
        root = self.workflow.root(eid)
        for name, data in files.items():
            discussion_outputs.write_once(root / name, data, root)
        with self.store.tx() as db:
            # Crash/reconciliation replays the saved turn, never a second answer or report intent.
            prior = db.execute("SELECT state FROM discussions WHERE id=?", (message["id"],)).fetchone()
            if prior["state"] == "completed":
                return
            if files:
                artifacts.register(self.store, eid, "discuss:" + message["id"], root, list(files), _db=db)
            version = None
            if reply.presentation:
                current = db.execute(
                    "SELECT COALESCE(MAX(version),1) FROM report_versions WHERE experiment_id=? AND state='ready'",
                    (eid,),
                ).fetchone()[0]
                pending = db.execute(
                    "SELECT 1 FROM report_versions WHERE experiment_id=? AND state IN ('queued','building','publishing')",
                    (eid,),
                ).fetchone()
                if current != payload["base_report_version"] or pending:
                    raise ValueError(
                        "The report changed during this reply. Ask again using the current version."
                    )
                version = (
                    max(
                        db.execute(
                            "SELECT MAX(version) FROM report_versions WHERE experiment_id=?", (eid,)
                        ).fetchone()[0]
                        or 1,
                        1,
                    )
                    + 1
                )
                self.store._insert_report(
                    db,
                    eid,
                    version,
                    reply.presentation.model_dump(mode="json"),
                    "discussion:" + message["id"],
                )
            followup = None
            if reply.new_experiment:
                followup = str(uuid5(NAMESPACE_URL, "discussion-followup:" + message["id"]))
                exp = self.store.experiment(eid)
                request = {
                    "brief": reply.new_experiment.brief,
                    "project_id": exp["contract"].get("project", {}).get("id", "test-project"),
                    "recipe_id": exp["recipe_id"],
                    "reference_experiment_id": eid,
                    "model": message["model"],
                    "reasoning_effort": message["reasoning_effort"],
                }
                self.store.command(
                    followup, "create", request, origin="discussion", experiment_id=followup, _db=db
                )
            exp = self.store.experiment(eid)
            db.execute(
                "UPDATE discussions SET state='completed',hold_reason=NULL,error=NULL,reply=?,evidence_ids=?,report_version=?,artifact_ids=?,followup_experiment_id=?,updated_at=? WHERE id=?",
                (
                    reply.reply,
                    encode(reply.evidence_ids),
                    version,
                    encode([reporting.artifact_id(exp, n) for n in files]),
                    followup,
                    time.time(),
                    message["id"],
                ),
            )
            self.store.emit(
                db, eid, "discussion_completed", {"discussion_id": message["id"], "report_version": version}
            )

    async def advance(self):
        rows = self.store.rows(
            "SELECT d.* FROM discussions d JOIN experiments e ON e.id=d.experiment_id "
            "WHERE e.deleted_at IS NULL AND d.state IN ('queued','waiting','running','uncertain') ORDER BY d.created_at,d.seq"
        )
        for message in rows:
            eid = message["experiment_id"]
            # An unresolved earlier message keeps later messages in this experiment in order.
            if self.store.one(
                "SELECT 1 FROM discussions WHERE experiment_id=? AND seq<? AND state NOT IN ('completed','failed')",
                (eid, message["seq"]),
            ):
                continue
            action = (
                self.store.one("SELECT * FROM agent_actions WHERE id=?", (message["action_id"],))
                if message["action_id"]
                else None
            )
            if action and action["state"] == "completed":
                try:
                    self.complete(message, action)
                except (discussion_outputs.UndeclaredOutput, CorrectableReply) as exc:
                    self.correct_outputs(message, action, str(exc))
                except (ValueError, KeyError, OSError) as exc:
                    self.store.discussion_state(message["id"], "failed", error=str(exc))
                continue
            if action and action["state"] in ("dispatching", "running", "uncertain"):
                if self.workflow.agent_task and self.workflow.active_action == action["id"]:
                    if self.store.experiment(eid)["pause_agent"] and action["turn_id"]:
                        await self.workflow.agent.interrupt(action["thread_id"], action["turn_id"])
                    continue
                if action["state"] != "uncertain":
                    if not self.workflow.agent_ready:
                        self.hold(message["id"], "agent_unavailable", self.workflow.agent_error)
                        continue
                    try:
                        if not action["thread_id"] or not action["turn_id"]:
                            raise NeedsAttention("dispatch response lost; saved turn identity is required")
                        result = await self.workflow.agent.read_turn(action["thread_id"], action["turn_id"])
                        self.workflow._action_state(action["id"], "completed", result=result)
                        continue
                    except Exception as exc:
                        self.workflow._action_state(action["id"], "uncertain", error=str(exc))
                        action["error"] = str(exc)
                self.store.discussion_state(
                    message["id"], "uncertain", hold_reason="agent_dispatch_uncertain", error=action["error"]
                )
                continue
            if action and action["state"] == "failed":
                self.store.discussion_state(message["id"], "failed", error=action["error"])
                continue
            exp = self.store.experiment(eid)
            reason = (
                "agent_paused"
                if exp["pause_agent"]
                else "needs_attention"
                if exp["pending_request"]
                else "agent_unavailable"
                if not self.workflow.agent_ready
                else "agent_busy"
                if self.workflow.agent_task
                or self.store.one(
                    "SELECT 1 FROM agent_actions WHERE state IN ('dispatching','running','uncertain')"
                )
                else "report_publication"
                if self.store.one(
                    "SELECT 1 FROM report_versions WHERE experiment_id=? AND state IN ('queued','building','publishing')",
                    (eid,),
                )
                else "quota_hold"
                if not self.workflow.gate.decide().admitted
                else None
            )
            if reason:
                self.hold(message["id"], reason)
                continue
            if not action:
                try:
                    prompt = self.prompt(exp, message)
                except (ValueError, KeyError, OSError) as exc:
                    self.store.discussion_state(message["id"], "failed", error=str(exc))
                    continue
                identity, now = str(uuid4()), time.time()
                with self.store.tx() as db:
                    db.execute(
                        "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at) VALUES(?,?,?,?,'pending',?,?,?)",
                        (
                            identity,
                            eid,
                            "discuss:" + message["id"],
                            exp["evidence_revision"],
                            prompt,
                            now,
                            now,
                        ),
                    )
                    db.execute(
                        "UPDATE discussions SET action_id=?,state='queued',hold_reason=NULL,error=NULL,updated_at=? WHERE id=?",
                        (identity, now, message["id"]),
                    )
                    self.store.emit(
                        db,
                        eid,
                        "discussion_action_queued",
                        {"discussion_id": message["id"], "action_id": identity},
                    )
                action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (identity,))
            self.workflow.active_action = action["id"]
            self.workflow.agent_task = asyncio.create_task(self.dispatch(message, action))
            return

    def correct_outputs(self, message, action, error):
        """Continue only a saved terminal reply; preserve its result and exact turn identity."""
        error = str(error)[:1800]
        repeats, corrections, prior = 1, 0, action
        while prior.get("previous_action_id"):
            prior = self.store.one("SELECT * FROM agent_actions WHERE id=?", (prior["previous_action_id"],))
            corrections += 1
            if prior["error"] == error:
                repeats += 1
        if repeats >= self.workflow.config.codex.identical_error_limit or corrections >= 3:
            self.store.discussion_state(message["id"], "failed", error=error)
            return
        exp = self.store.experiment(message["experiment_id"])
        cap = self.workflow.agent_turn_ceiling(exp)
        count = self.store.one(
            "SELECT COUNT(*) AS n FROM agent_actions WHERE experiment_id=? AND attempts>0",
            (exp["id"],),
        )["n"]
        if cap is not None and count >= cap:
            self.store.discussion_state(
                message["id"], "failed", error="The configured agent turn budget is exhausted"
            )
            return
        with self.store.tx() as db:
            db.execute(
                "UPDATE agent_actions SET state='failed',error=?,updated_at=? WHERE id=?",
                (error, time.time(), action["id"]),
            )
            failed = dict(action, state="failed", error=error)
            continuation = self.store.continue_action(failed, feedback=error, _db=db)
            instruction, payload = action["input"].split("\nPAYLOAD:\n", 1)
            prompt = instruction + "\nPublication validation feedback: " + error + "\nPAYLOAD:\n" + payload
            db.execute("UPDATE agent_actions SET input=? WHERE id=?", (prompt, continuation["id"]))
            db.execute(
                "UPDATE discussions SET action_id=?,state='queued',hold_reason=NULL,error=?,updated_at=? WHERE id=?",
                (continuation["id"], error, time.time(), message["id"]),
            )
            self.store.emit(
                db,
                exp["id"],
                "discussion_correction_queued",
                {"discussion_id": message["id"], "action_id": continuation["id"]},
            )

    async def dispatch(self, message, action):
        agent, eid = self.workflow.agent, message["experiment_id"]
        try:
            decision = await self.workflow.refresh_quota()
            if not decision.admitted:
                self.hold(message["id"], "quota_hold", decision.reason)
                return
            if self.store.experiment(eid)["pause_agent"]:
                self.hold(message["id"], "agent_paused")
                return
            selection = self.store.experiment(eid)["contract"].get("agent", {})
            model = message["model"] or selection.get("model") or getattr(agent, "resolved_model", None)
            option = next((m for m in getattr(agent, "models", []) if m["id"] == model), None)
            effort = (
                message["reasoning_effort"]
                or selection.get("reasoning_effort")
                or (option or {}).get("default_effort", "medium")
            )
            if not option or effort not in option["reasoning_efforts"]:
                raise ValueError("Selected model or effort is unavailable; no fallback was used")
            thread = self.store.one("SELECT thread_id FROM discussion_threads WHERE experiment_id=?", (eid,))
            workspace = discussion_outputs.workspace(self.workflow.root(eid), message["id"])
            checkout = self.workflow.workspaces.checkout(eid)
            project_workspace = Path(checkout) if checkout else None
            thread_id, resolved = await agent.session(
                thread["thread_id"] if thread else None,
                project_workspace or workspace,
                writable=True,
                model=model,
            )
            wire, saved_input = self.turn_prompt(action, thread_id)
            with self.store.tx() as db:
                db.execute("UPDATE agent_actions SET input=? WHERE id=?", (saved_input, action["id"]))
                db.execute(
                    "INSERT INTO discussion_threads VALUES(?,?) ON CONFLICT(experiment_id) DO UPDATE SET thread_id=excluded.thread_id",
                    (eid, thread_id),
                )
                db.execute(
                    "UPDATE agent_actions SET thread_id=?,state='dispatching',attempts=attempts+1,updated_at=? WHERE id=?",
                    (thread_id, time.time(), action["id"]),
                )
                db.execute(
                    "UPDATE discussions SET state='running',model=?,reasoning_effort=?,hold_reason=NULL,error=NULL,updated_at=? WHERE id=?",
                    (resolved, effort, time.time(), message["id"]),
                )
                self.store.emit(
                    db,
                    eid,
                    "discussion_dispatch_intent",
                    {"discussion_id": message["id"], "action_id": action["id"]},
                )
            turn_id = await agent.begin(
                thread_id,
                wire,
                DiscussionReply.model_json_schema(),
                effort=effort,
                workspace=workspace,
                **({"project_workspace": project_workspace} if project_workspace else {}),
            )
            with self.store.tx() as db:
                db.execute(
                    "UPDATE agent_actions SET turn_id=?,state='running',updated_at=? WHERE id=?",
                    (turn_id, time.time(), action["id"]),
                )
                self.store.emit(db, eid, "discussion_turn_started", {"discussion_id": message["id"]})
            result = await agent.finish(thread_id, turn_id)
            self.workflow._action_state(action["id"], "completed", result=result)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            current = self.store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))
            state = (
                "uncertain"
                if current["state"] == "dispatching" or isinstance(exc, NeedsAttention)
                else "failed"
            )
            self.workflow._action_state(action["id"], state, error=str(exc)[:2000])
            self.store.discussion_state(
                message["id"],
                state,
                hold_reason="agent_dispatch_uncertain" if state == "uncertain" else None,
                error=str(exc),
            )
        finally:
            pending = self.store.experiment(eid)["pending_request"]
            if pending:
                pending["expired"] = True
                self.store.update(eid, pending_request=pending)
