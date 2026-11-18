"""Persistent experiment wakeups and a local MCP tool bridge owned by the executor."""

import argparse
import asyncio
import json
import socket
import sys
from pathlib import Path
from uuid import UUID, uuid4

from pydantic import Field

from . import artifacts, clock, early_repair, iteration, reporting
from .agent_pool import EXPERIMENT_PRIORITY, experiment_owner
from .codex_client import TerminalTurnError
from .config import StrictModel
from .core import action_obligations, wakeup_obligations
from .legacy_archive import execution_frozen
from .models import Analysis, Implementation, ReportDocument, ResearchPlan
from .research.admission import SubmitJobRequest
from .research.cancel_tool import CancelJob, cancel_job
from .research.checkpoints import SaveCheckpoint, save_checkpoint
from .research.figure_tool import RegisterFigure, register_figure
from .research.integration_tool import RequestIntegration, request_integration
from .research.job_dispatch import dispatch as dispatch_research_job
from .research.owner_tool import AskOwnerQuestion, ask_owner
from .research.report_tool import UpdateReport
from .research.report_tool import update_report as update_research_report
from .research.result_tool import RegisterResult
from .research.result_tool import register_result as register_research_result
from .research.tools import (
    ClusterInfo,
    JobLogs,
    JobStatus,
    checkpoint_receipt,
    cluster_info,
    job_logs,
    job_status,
    log_tail,
    require_live_research,
    require_research_write,
)
from .store import Conflict, encode


class ControllerDecision(StrictModel):
    summary: str = Field(min_length=1, max_length=2000)
    next_check_seconds: int = Field(default=900, ge=60, le=86400)


def tool(name, description, properties, required=()):
    return {
        "name": name,
        "description": description,
        "inputSchema": {
            "type": "object",
            "properties": properties,
            "required": list(required),
            "additionalProperties": False,
        },
    }


def tool_from_model(name, description, model):
    """Advertise exactly the schema the tool validates before dispatch."""
    return {"name": name, "description": description, "inputSchema": model.model_json_schema()}


# Tools that read or write the contract-3 stage graph, its manifests or its execution
# settings. Contract 4 replaces them with research-loop tools, so a saved contract 4
# turn cannot dispatch stage work; shared reads and owner actions are not listed.
STAGE_TOOLS = frozenset(
    (
        "check_agent_stage",
        "check_manifest",
        "register_analysis_file",
        "register_result",
        "set_project_execution",
        "submit_stage",
    )
)

TOOLS = [
    tool(
        "cancel_job",
        "During selective repair, cancel only this experiment's exact tracked Slurm intent. Saves a durable decision; terminal success is adopted, and uncertain outcomes need exact accounting.",
        {
            "intent_id": {"type": "string"},
            "request_id": {"type": "string"},
            "reason": {"type": "string", "maxLength": 2000},
        },
        ["intent_id", "request_id", "reason"],
    ),
    tool(
        "check_agent_stage",
        "Check this active agent stage's declared outputs without completing it; fix errors in this turn.",
        {},
        [],
    ),
    tool(
        "update_report",
        "Publish a new working report version from available metrics/figures without waiting for all tasks.",
        {"request_id": {"type": "string"}, "document": {"type": "object"}},
        ["request_id", "document"],
    ),
    tool(
        "check_manifest",
        "Check a plan, implementation or analysis now; returns concrete validation errors without dispatch or mutation.",
        {
            "kind": {"type": "string", "enum": ["plan", "prepare", "interpret"]},
            "manifest": {"type": "object"},
        },
        ["kind", "manifest"],
    ),
    tool(
        "job_status",
        "Read this experiment's saved jobs. Use summary_only for a compact list; select intent_ids to inspect bounded exact logs and details.",
        {
            "intent_ids": {"type": "array", "items": {"type": "string"}, "maxItems": 32},
            "summary_only": {"type": "boolean"},
        },
        [],
    ),
    tool(
        "schedule_wakeup",
        "Save a future backend compute check; healthy jobs need no inference. Repeating a request_id adopts the same wakeup; a new request replaces pending checks.",
        {
            "request_id": {"type": "string"},
            "delay_seconds": {"type": "integer", "minimum": 60, "maximum": 86400},
            "reason": {"type": "string", "maxLength": 1000},
        },
        ["request_id", "delay_seconds", "reason"],
    ),
    tool(
        "submit_stage",
        "Submit a prepared ready stage through the executor. Exact saved outcomes are adopted, never resubmitted. Requires approved objectives, compute admission and validated dependencies.",
        {"stage_id": {"type": "string"}, "request_id": {"type": "string"}},
        ["stage_id", "request_id"],
    ),
    tool(
        "register_result",
        "Validate and register an exact successfully completed stage's evidence without launching work.",
        {"stage_id": {"type": "string"}},
        ["stage_id"],
    ),
    tool(
        "register_analysis_file",
        "Register a new analysis figure/data file and return its evidence ID. Raw measurements remain unchanged; use a new filename for revisions.",
        {"filename": {"type": "string"}},
        ["filename"],
    ),
    tool(
        "set_project_execution",
        "Save this project's rsync up/down commands and remote folder. Also configures this experiment before its first submission. No transfer or job is launched. Templates: {local_project}, {remote_project}, {local_run}, {remote_run}, {host}; no shell evaluation.",
        {
            "request_id": {"type": "string"},
            "remote_project": {"type": "string"},
            "rsync_up": {"type": "string"},
            "rsync_down": {"type": "string"},
        },
        ["request_id", "remote_project", "rsync_up", "rsync_down"],
    ),
]

# Contract 4 advertises only research tools, never the stage-engine tools.
RESEARCH_TOOLS = [
    tool_from_model(
        "submit_job",
        "Submit one locally scoped research job after exact owner approval. Repeating a request_id adopts the saved outcome and never resubmits an uncertain job.",
        SubmitJobRequest,
    ),
    tool_from_model(
        "cancel_job",
        "Save a cancellation request for this experiment's exact tracked Slurm intent. This records the decision only; the existing backend reconciler handles scheduler cancellation later. Repeating a request_id with the same decision adopts the saved request.",
        CancelJob,
    ),
    tool(
        "cluster_info",
        "Return this executor's cached cluster capabilities and owner resource caps. Read-only; repeating a request_id is the same read and never refreshes discovery.",
        {"request_id": {"type": "string", "format": "uuid"}},
        ["request_id"],
    ),
    tool_from_model(
        "job_status",
        "Read this experiment's saved jobs. Without intent_ids, return the latest at most 32 jobs in stable order and report any omitted count. Repeating a request_id is the same read.",
        JobStatus,
    ),
    tool_from_model(
        "job_logs",
        "Read one of this experiment's saved jobs' bounded log tails from the executor's local attempt directory. Remote logs are available only after the existing transfer; repeating a request_id is the same read.",
        JobLogs,
    ),
    tool_from_model(
        "save_checkpoint",
        "Save one private, versioned research checkpoint for this turn. Returns only the version receipt; the Markdown is never published. Repeating a request_id with identical Markdown adopts the saved checkpoint.",
        SaveCheckpoint,
    ),
    tool_from_model(
        "update_report",
        "Queue one new working report version from saved evidence. Returns the version and state; the existing report runner builds and publishes it. Repeating a request_id with the same document adopts the saved version.",
        UpdateReport,
    ),
    tool_from_model(
        "ask_owner",
        "Ask the owner a durable question or request approval for an explicit proposed objective. Only an owner can decide approval. Repeating a request_id with the same input returns the saved state.",
        AskOwnerQuestion,
    ),
    tool_from_model(
        "request_integration",
        "Queue this experiment's exact saved named branch for the existing integration worker, with a bounded reason. Returns queued and never merged; the worker still owns approval, Git and the terminal stage. Repeating a request_id with the same reason adopts the saved queue entry.",
        RequestIntegration,
    ),
    tool_from_model(
        "register_result",
        'Register one validated result file from this experiment\'s exact successful job as a stable artifact receipt. Write the file as JSON before calling: {"schema_version":1,"protocol":"Generated 64-point held-out regression comparison","sample_count":64,"metrics":[{"name":"mse","variant":"linear","value":0.0123,"unit":"squared target units"}]}. Replace the example protocol with a nonempty description of the measurement protocol actually used. Give each metric a nonempty name and variant, finite numeric value and unit (use an empty string for dimensionless values). sample_count is optional; if provided, use a positive integer. Keep values numeric, not strings. The backend validates this file and its exact successful job; do not alter producer files or rely on metadata being inferred. Repeating a request_id with the same intent and path adopts the saved receipt.',
        RegisterResult,
    ),
    tool_from_model(
        "register_figure",
        "Register one existing image from the saved worktree or output root as a stable copy for this live research turn. Returns the saved artifact receipt; the mutable source is never referenced afterwards. Repeating a request_id with the same source and caption adopts the saved copy.",
        RegisterFigure,
    ),
]
RESEARCH_TOOL_NAMES = frozenset(item["name"] for item in RESEARCH_TOOLS)


def served_tools(research=False):
    """The tools an MCP process advertises for one saved execution contract."""
    return RESEARCH_TOOLS if research else TOOLS


class Controller:
    def __init__(self, workflow):
        self.workflow = workflow
        self.socket_path = workflow.config.state_dir / "controller.sock"
        self.server = None

    async def start(self):
        if (
            self.server
            or self.workflow.config.mode != "live"
            or getattr(self.workflow.agent, "is_demo", False)
        ):
            return
        # The owning process lock has already been acquired. The socket is local,
        # protected by the state directory and OS permissions, never browser auth.
        if self.socket_path.exists():
            if not self.socket_path.is_socket():
                raise ValueError("controller socket path is not a socket")
            self.socket_path.unlink()
        self.server = await asyncio.start_unix_server(
            self.connection, str(self.socket_path), limit=1024 * 1024
        )
        self.socket_path.chmod(0o600)

    def session_config(self, eid):
        if not self.server:
            return None
        row = self.workflow.store.one("SELECT contract FROM experiments WHERE id=?", (eid,))
        research = bool(row and (json.loads(row["contract"]) or {}).get("execution_contract") == 4)
        args = [
            "-m",
            "experimenttracker.controller",
            "--socket",
            str(self.socket_path),
            "--experiment",
            eid,
        ]
        if research:
            args.append("--research")
        return {
            "mcp_servers.experimenttracker": {
                "command": sys.executable,
                "args": args,
                "startup_timeout_sec": 15,
                "tool_timeout_sec": 120,
            }
        }

    async def connection(self, reader, writer):
        try:
            request = json.loads(await asyncio.wait_for(reader.readline(), 10))
            if request.get("project_action_id"):
                result = await self.workflow.research.call(
                    request["project_action_id"], request["name"], request.get("arguments", {})
                )
            else:
                result = await self.call(
                    request["experiment_id"], request["name"], request.get("arguments", {})
                )
            response = {"ok": True, "result": result}
        except Exception as exc:
            response = {"ok": False, "error": self.workflow.validation_feedback(exc)}
        try:
            writer.write((encode(response) + "\n").encode())
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    async def check(self, exp, kind, value):
        wf = self.workflow
        model = {"plan": ResearchPlan, "prepare": Implementation, "interpret": Analysis}[kind]
        parsed = model.model_validate(value)
        if kind == "prepare" and parsed.outcome == "blocked":
            return {"valid": True, "ready": False, "manifest": parsed.model_dump()}
        if kind in ("plan", "prepare"):
            wf.validate_resource_plan(exp, parsed)
        if kind == "prepare":
            runtime = await asyncio.to_thread(wf.workspaces.shared_runtime, exp["id"])
            python = wf.prepared_python(exp, parsed, runtime)
            for name in parsed.files:
                source = artifacts.allowed_path(wf.working_directory(exp) / name, wf.working_directory(exp))
                if name.endswith(".py"):
                    from .slurm import command

                    await command(
                        str(python),
                        "-c",
                        "import pathlib,sys; p=pathlib.Path(sys.argv[1]); compile(p.read_text(),str(p),'exec')",
                        str(source),
                    )
                elif name.endswith((".sh", ".sbatch")):
                    from .slurm import command

                    await command("/bin/bash", "-n", str(source))
        elif kind == "interpret":
            records = wf.store.rows(
                "SELECT * FROM artifacts WHERE experiment_id=? AND evidence_revision=?",
                (exp["id"], exp["evidence_revision"]),
            )
            artifacts.validate_analysis(value, [record["id"] for record in records])
            if exp["contract"].get("reporting_version") and not parsed.presentation:
                raise ValueError("analysis needs a presentation document")
            if parsed.presentation:
                reporting.validate_document(
                    reporting.Evidence(exp, wf.root(exp["id"]), records), parsed.presentation
                )
        return {"valid": True, "manifest": parsed.model_dump()}

    def status(self, eid, *, intent_ids=None, summary_only=False, limit=None):
        wf = self.workflow
        if intent_ids is not None and not intent_ids:
            return []
        query = "SELECT * FROM jobs WHERE experiment_id=?"
        params = [eid]
        if limit is None:
            query += " ORDER BY created_at"
        else:
            if intent_ids is not None:
                query += f" AND intent_id IN ({','.join('?' for _ in intent_ids)})"
                params.extend(sorted(intent_ids))
            query = (
                f"SELECT * FROM ({query} ORDER BY created_at DESC, intent_id DESC LIMIT ?)"
                " ORDER BY created_at, intent_id"
            )
            params.append(limit)
        jobs = wf.store.rows(query, tuple(params))
        result = []
        for job in jobs:
            if intent_ids is not None and job["intent_id"] not in intent_ids:
                continue
            root = Path(job["execution_root"]) if job.get("execution_root") else wf.root(eid)
            item = {
                key: job[key]
                for key in (
                    "intent_id",
                    "stage_id",
                    "job_id",
                    "state",
                    "scheduler_state",
                    "exit_code",
                    "error",
                    "progress",
                    "started_at",
                    "ended_at",
                )
            }
            item["attempt"] = job["attempt"]
            if summary_only:
                item.pop("progress", None)
                result.append(item)
                continue
            item["outputs"] = wf.store.rows(
                "SELECT * FROM output_refs WHERE intent_id=?", (job["intent_id"],)
            )
            manifest = json.loads(job["execution_manifest"] or "{}")
            item["command"] = manifest.get("command", manifest.get("plan", {}))
            item["attempt"] = job["attempt"]
            if iteration.manifest_mode(manifest):
                execution = manifest.get("execution")
                spec = next(s for s in manifest["plan"]["stages"] if s["id"] == job["stage_id"])
                item["declared_outputs"] = {
                    "context": "jean-zay" if execution else "acacia",
                    "host": execution["layout"]["host"] if execution else wf.config.executor_id,
                    "root": execution["layout"]["remote_run"] if execution else str(root),
                    "names": spec["outputs"] + spec.get("local_outputs", []),
                    "note": "Unregistered partial files are diagnostic context; inspect only needed paths on this host.",
                }
            item["logs"] = log_tail(root, job, 4000)
            result.append(item)
        return result

    def schedule(self, eid, identity, delay, reason, action_id=None):
        identity = str(UUID(str(identity)))
        if not 60 <= delay <= 86400 or not reason.strip() or len(reason) > 1000:
            raise ValueError("wakeup needs a reason and delay between 60 and 86400 seconds")
        store = self.workflow.store
        with store.tx() as db:
            prior = db.execute("SELECT * FROM agent_wakeups WHERE id=?", (identity,)).fetchone()
            payload = encode({"delay_seconds": delay, "reason": reason})
            if prior:
                if prior["experiment_id"] != eid or prior["reason"] != payload:
                    raise Conflict("wakeup request identity was reused for different work")
                return dict(prior)
            now = clock.now()
            # A new request replaces pending checks, while queued turns retain
            # their exact outcome. Keep old receipts for idempotent replay.
            replaced = [
                row["id"]
                for row in db.execute(
                    "SELECT id FROM agent_wakeups WHERE experiment_id=? AND state='pending'", (eid,)
                )
            ]
            db.execute(
                "UPDATE agent_wakeups SET state='superseded' WHERE experiment_id=? AND state='pending'",
                (eid,),
            )
            for replaced_id in replaced:
                wakeup_obligations.sync_wakeup(db, replaced_id)
            db.execute(
                "INSERT INTO agent_wakeups(id,experiment_id,reason,due_at,action_id,created_at) VALUES(?,?,?,?,?,?)",
                (identity, eid, payload, now + delay, action_id, now),
            )
            wakeup_obligations.sync_wakeup(db, identity)
            store.emit(
                db,
                eid,
                "agent_wakeup_scheduled",
                {"wakeup_id": identity, "due_at": now + delay, "reason": reason},
            )
            return dict(db.execute("SELECT * FROM agent_wakeups WHERE id=?", (identity,)).fetchone())

    async def call(self, eid, name, args):
        wf = self.workflow
        action = wf.store.one(
            "SELECT * FROM agent_actions WHERE experiment_id=? AND state IN ('dispatching','running') ORDER BY rowid DESC LIMIT 1",
            (eid,),
        )
        if not action or action["experiment_id"] != eid or action["state"] not in ("dispatching", "running"):
            raise ValueError("tool call requires this experiment's active owned session")
        exp = wf.store.experiment(eid)
        if execution_frozen(exp) and name != "job_status":
            raise ValueError("legacy execution is archived; only status reads are available")
        research_result = (
            (exp.get("contract") or {}).get("execution_contract") == 4
            and action["phase"] == "research"
            and name == "register_result"
        )
        if name in STAGE_TOOLS and not iteration.enabled(exp) and not research_result:
            raise ValueError("stage-engine tools require execution contract 3")
        if (exp.get("contract") or {}).get("execution_contract") == 4 and name not in RESEARCH_TOOL_NAMES:
            raise ValueError("contract 4 turns permit only the listed research tools")
        if name == "submit_job":
            request = SubmitJobRequest.model_validate(args)
            require_research_write(wf, exp, action)
            reservation = wf.agent_pool.lookup("agent_actions", action["id"])
            if (
                reservation is None
                or reservation.owner != experiment_owner(eid)
                or wf.agent_pool.live_slot(reservation) is None
            ):
                raise ValueError("submission requires exact live pool ownership")
            row = await dispatch_research_job(wf, eid, action, request)
            return {
                "state": row["state"],
                "intent_id": row["intent_id"],
                "job_id": row["job_id"],
                "commit": json.loads(row["execution_manifest"] or "{}").get("commit"),
            }
        if name == "cancel_job" and (exp.get("contract") or {}).get("execution_contract") == 4:
            require_live_research(exp, action)
            return cancel_job(wf.store, exp, action, CancelJob.model_validate(args))
        if name == "cluster_info":
            request = ClusterInfo.model_validate(args)
            require_live_research(exp, action)
            return cluster_info(wf, exp, request)
        if name == "job_logs":
            request = JobLogs.model_validate(args)
            require_live_research(exp, action)
            return job_logs(wf, exp, request)
        if name == "job_status" and (exp.get("contract") or {}).get("execution_contract") == 4:
            require_live_research(exp, action)
            request = JobStatus.model_validate(args)
            return job_status(wf, exp, request)
        if (
            name not in ("job_status", "check_manifest", "check_agent_stage")
            and action.get("autopilot_control_revision") is not None
        ):
            if action["autopilot_control_revision"] != wf.research.service.control_revision(
                eid
            ) or wf.research.service.authority(
                eid, "interpret" if action["phase"] == "interpret" else action["phase"]
            ):
                raise ValueError("current project authority/hold rejects stale child turn effects")
        if name == "save_checkpoint":
            require_research_write(wf, exp, action)
            request = SaveCheckpoint.model_validate(args)
            return checkpoint_receipt(save_checkpoint(wf.store, action, request))
        if name == "update_report" and (exp.get("contract") or {}).get("execution_contract") == 4:
            require_research_write(wf, exp, action)
            request = UpdateReport.model_validate(args)
            return update_research_report(wf.store, exp, action, request)
        if name == "ask_owner":
            require_research_write(wf, exp, action)
            request = AskOwnerQuestion.model_validate(args)
            return ask_owner(wf.store, action, request)
        if name == "request_integration":
            require_research_write(wf, exp, action)
            request = RequestIntegration.model_validate(args)
            return request_integration(wf.store, exp, action, request)
        if research_result:
            require_research_write(wf, exp, action)
            request = RegisterResult.model_validate(args)
            return await asyncio.to_thread(register_research_result, wf, exp, action, request)
        if name == "register_figure":
            require_research_write(wf, exp, action)
            request = RegisterFigure.model_validate(args)
            # The copy and hash are bounded filesystem work; keep the controller
            # loop free while the handler repeats its own saved-turn gates.
            return await asyncio.to_thread(register_figure, wf, exp, action, request)
        if action["phase"].startswith("agent:"):
            if name == "check_agent_stage":
                job = iteration.agent_job(wf, exp, action["phase"])
                iteration.check_stage(
                    Path(job["execution_root"]), job["stage_id"], json.loads(job["execution_manifest"])
                )
                return {"valid": True}
            if name not in ("job_status",):
                raise ValueError(
                    "agent stages permit output checks and status reads; finish the current task before dispatching other work"
                )
        if action["phase"] == "monitor" and name not in ("job_status", "schedule_wakeup"):
            raise ValueError("monitoring only permits status reads and future backend checks")
        if name == "cancel_job":
            if action["phase"] != "prepare":
                raise ValueError("cancellation requires the active repair preparation turn")
            return await early_repair.cancel(wf, exp, args["intent_id"], args["request_id"], args["reason"])
        if name == "set_project_execution":
            if action["phase"] != "prepare":
                raise ValueError("execution settings can only be changed during implementation")
            if not wf.is_remote(exp):
                raise ValueError(
                    "this experiment uses acacia; select Jean Zay in Project settings for a new experiment"
                )
            from .execution_settings import ExecutionSettings

            project_id = exp["contract"]["project"]["id"]
            current = wf.projects.settings(project_id)
            execution = ExecutionSettings.model_validate(
                {
                    **exp["contract"]["execution"],
                    **{key: args[key] for key in ("remote_project", "rsync_up", "rsync_down")},
                }
            ).model_dump()
            receipt = wf.projects.enqueue_settings(
                project_id,
                {
                    "request_id": str(UUID(args["request_id"])),
                    "instructions": current["instructions"],
                    "execution": execution,
                },
                origin="agent",
            )
            wf.projects.settings_tick()
            receipt = wf.projects.settings_receipt(receipt["request_id"])
            if receipt["state"] == "applied" and not wf.store.one(
                "SELECT 1 FROM jobs WHERE experiment_id=?", (eid,)
            ):
                contract = {**exp["contract"], "execution": execution}
                if contract != exp["contract"]:
                    with wf.store.tx() as db:
                        db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
                        wf.store.emit(db, eid, "execution_configured", {"execution": execution})
            return receipt
        if name == "update_report":
            if not exp["snapshot"]:
                raise ValueError("working reports require an iteration-first saved implementation")
            document = ReportDocument.model_validate(args["document"])
            records = wf.store.rows("SELECT * FROM artifacts WHERE experiment_id=?", (eid,))
            reporting.validate_document(reporting.Evidence(exp, wf.root(eid), records), document)
            version = wf.store.working_report(
                eid, document.model_dump(mode="json"), str(UUID(args["request_id"]))
            )
            return {"report_version": version, "state": "queued"}
        if name == "check_manifest":
            return await self.check(exp, args["kind"], args["manifest"])
        if name == "job_status":
            requested = args.get("intent_ids")
            if requested is not None:
                if not isinstance(requested, list) or len(requested) > 32:
                    raise ValueError("select at most 32 job intents")
                requested = {str(UUID(value)) for value in requested}
            if action["phase"] == "monitor":
                payload = json.loads(action["input"].split("\nPAYLOAD:\n", 1)[1])
                permitted = {job["intent_id"] for job in payload["jobs"]}
                requested = permitted if requested is None else requested & permitted
            return self.status(eid, intent_ids=requested, summary_only=args.get("summary_only", False))
        if name == "schedule_wakeup":
            return self.schedule(eid, args["request_id"], args["delay_seconds"], args["reason"], action["id"])
        if name == "submit_stage":
            return await wf.submit_prepared_stage(exp, args["stage_id"], str(UUID(args["request_id"])))
        if name == "register_result":
            job = wf.store.one(
                "SELECT j.* FROM jobs j JOIN stage_producers p ON p.intent_id=j.intent_id WHERE p.experiment_id=? AND p.stage_id=?",
                (eid, args["stage_id"]),
            )
            if not job or job["state"] != "complete":
                raise ValueError("result registration requires a validated terminal successful job")
            return wf.store.rows(
                "SELECT id,name,intent_id,context,host,path,kind FROM output_refs WHERE intent_id=?",
                (job["intent_id"],),
            )
        if name == "register_analysis_file":
            return wf.register_analysis_file(exp, args["filename"])
        raise ValueError("unknown experiment tool")

    def diagnostic_jobs(self, eid):
        """Require fresh scheduler evidence and sustained missing telemetry.

        Unknown instrumentation is not evidence of a stall. Each observed
        progress value can trigger one diagnosis; a durable per-job ceiling
        prevents recurring monitoring from consuming the research allowance.
        """
        wf, now = self.workflow, clock.now()
        interval = wf.config.codex.monitor_interval_seconds
        prior = wf.store.rows(
            "SELECT input,created_at FROM agent_actions WHERE experiment_id=? AND phase='monitor' AND attempts>0",
            (eid,),
        )
        if any(now - action["created_at"] < interval for action in prior):
            return []
        diagnosed, counts = set(), {}
        for action in prior:
            try:
                payload = json.loads(action["input"].split("\nPAYLOAD:\n", 1)[1])
            except (ValueError, IndexError):
                continue
            for job in payload.get("jobs", []):
                identity = job["intent_id"]
                counts[identity] = counts.get(identity, 0) + 1
                if job.get("diagnostic_key"):
                    diagnosed.add(job["diagnostic_key"])
        candidates = []
        for job in wf.store.rows(
            "SELECT * FROM jobs WHERE experiment_id=? AND state='submitted' AND scheduler_state='RUNNING'",
            (eid,),
        ):
            execution = wf.store.experiment(eid)["contract"].get("execution", {})
            remote_interval = (
                execution.get("poll_seconds", 600) if execution.get("context") == "jean-zay" else 0
            )
            if now - (job["observed_at"] or 0) > max(
                60, 3 * wf.config.scheduler_seconds, 2 * remote_interval
            ):
                continue
            progress = json.loads(job["progress"]) if job["progress"] else None
            stall_seconds = max(
                wf.config.codex.monitor_stall_seconds, 2 * remote_interval + 60 if remote_interval else 0
            )
            if not progress or now - progress["reported_at"] < stall_seconds:
                continue
            key = encode([job["intent_id"], progress["completed"], progress["total"]])
            if (
                key in diagnosed
                or counts.get(job["intent_id"], 0) >= wf.config.codex.monitor_max_turns_per_job
            ):
                continue
            root = Path(job["execution_root"]) if job.get("execution_root") else wf.root(eid)
            stdout = root / f"et-{job['intent_id']}-{job['job_id']}.out"
            if stdout.is_file() and now - stdout.stat().st_mtime < stall_seconds:
                continue
            candidates.append((job["intent_id"], key))
        if not candidates:
            return []
        keys = dict(candidates[:8])
        return [
            {**job, "diagnostic_key": keys[job["intent_id"]]}
            for job in self.status(eid, intent_ids=keys)
            if job["intent_id"] in keys
        ]

    def follow_up(self, eid, delay):
        wf = self.workflow
        if wf.store.one(
            "SELECT 1 FROM jobs WHERE experiment_id=? AND state='submitted'", (eid,)
        ) and not wf.store.one(
            "SELECT 1 FROM agent_wakeups WHERE experiment_id=? AND state IN ('pending','queued')", (eid,)
        ):
            self.schedule(
                eid, uuid4(), max(delay, wf.config.codex.monitor_interval_seconds), "Backend compute check"
            )

    async def advance(self):
        wf = self.workflow
        if not self.server:
            return
        # The backend checks healthy jobs without opening a model turn.
        for row in wf.store.rows(
            "SELECT DISTINCT experiment_id FROM jobs WHERE state='submitted' AND scheduler_state='RUNNING'"
        ):
            eid = row["experiment_id"]
            exp = wf.store.experiment(eid)
            if not iteration.enabled(exp):
                continue
            if not wf.store.one(
                "SELECT 1 FROM agent_wakeups WHERE experiment_id=? AND state IN ('pending','queued')", (eid,)
            ):
                self.schedule(eid, uuid4(), wf.config.codex.monitor_interval_seconds, "Backend compute check")
        # Coalesce backlog from older owners without discarding queued outcomes.
        for row in wf.store.rows("SELECT DISTINCT experiment_id FROM agent_wakeups WHERE state='pending'"):
            exp = wf.store.experiment(row["experiment_id"])
            if not iteration.enabled(exp):
                continue
            pending = wf.store.rows(
                "SELECT id FROM agent_wakeups WHERE experiment_id=? AND state='pending' ORDER BY due_at,created_at,id",
                (row["experiment_id"],),
            )
            with wf.store.tx() as db:
                for obsolete in pending[1:]:
                    db.execute("UPDATE agent_wakeups SET state='superseded' WHERE id=?", (obsolete["id"],))
                    wakeup_obligations.sync_wakeup(db, obsolete["id"])
        for wake in wf.store.rows(
            "SELECT * FROM agent_wakeups WHERE state IN ('pending','queued') ORDER BY due_at LIMIT 50"
        ):
            exp = wf.store.experiment(wake["experiment_id"])
            # Monitor turns are contract-3 controller steering; contract 4 wakes on
            # events and its own timer instead.
            if not iteration.enabled(exp):
                continue
            terminal = exp["deleted_at"] or exp["phase"] in ("complete", "failed", "cancelled")
            if terminal and wake["state"] == "pending":
                with wf.store.tx() as db:
                    db.execute("UPDATE agent_wakeups SET state='superseded' WHERE id=?", (wake["id"],))
                    wakeup_obligations.sync_wakeup(db, wake["id"])
                continue
            if wake["state"] == "queued":
                action = wf.store.one("SELECT * FROM agent_actions WHERE id=?", (wake["action_id"],))
                if action["state"] == "pending" and (
                    terminal
                    or not wf.store.one(
                        "SELECT 1 FROM jobs WHERE experiment_id=? AND state='submitted'", (exp["id"],)
                    )
                ):
                    with wf.store.tx() as db:
                        before = db.execute(
                            "SELECT * FROM agent_actions WHERE id=? AND state='pending'", (action["id"],)
                        ).fetchone()
                        if before:
                            db.execute(
                                "UPDATE agent_actions SET state='superseded' WHERE id=? AND state='pending'",
                                (action["id"],),
                            )
                            action_obligations.sync_action(db, action["id"], before=before)
                            wf.store.emit(
                                db,
                                exp["id"],
                                "agent_action_changed",
                                {"action_id": action["id"], "state": "superseded"},
                            )
                        db.execute("UPDATE agent_wakeups SET state='superseded' WHERE id=?", (wake["id"],))
                        wakeup_obligations.sync_wakeup(db, wake["id"])
                    continue
                if action["state"] == "completed":
                    try:
                        decision = ControllerDecision.model_validate_json(action["result"])
                    except ValueError as exc:
                        wf._action_state(
                            action["id"],
                            "failed",
                            error="agent output validation: " + wf.validation_feedback(exc),
                        )
                        continue
                    wf.store.activity(
                        action, "controller-summary", "agentMessage", decision.summary, "complete"
                    )
                    with wf.store.tx() as db:
                        db.execute("UPDATE agent_wakeups SET state='complete' WHERE id=?", (wake["id"],))
                        wakeup_obligations.sync_wakeup(db, wake["id"])
                        wf.store.emit(db, exp["id"], "agent_wakeup_completed", {"wakeup_id": wake["id"]})
                    self.follow_up(exp["id"], decision.next_check_seconds)
                    continue
                if action["state"] == "failed":
                    # Monitor failures must not fail successful scientific compute.
                    with wf.store.tx() as db:
                        db.execute("UPDATE agent_wakeups SET state='failed' WHERE id=?", (wake["id"],))
                        wakeup_obligations.sync_wakeup(db, wake["id"])
                        wf.store.emit(
                            db,
                            exp["id"],
                            "agent_wakeup_failed",
                            {"wakeup_id": wake["id"], "error": action["error"]},
                        )
                    continue
                if action["state"] in ("running", "dispatching"):
                    # After owner restart there is no local task to finish the
                    # recorded monitor turn. Reconcile its exact identity before
                    # quota/new-dispatch admission; its own running row must not
                    # prevent this read or cause a replacement submission.
                    pool = wf.agent_pool
                    reservation = pool.lookup("agent_actions", action["id"])
                    if reservation is not None and reservation.slot is not None:
                        continue
                    slot = pool.read_slot(reservation)
                    if slot is None or not action["thread_id"] or not action["turn_id"]:
                        continue
                    try:
                        wf._action_state(
                            action["id"],
                            "completed",
                            result=await slot.agent.read_turn(action["thread_id"], action["turn_id"]),
                            expected=action,
                        )
                    except TerminalTurnError as exc:
                        wf._action_state(action["id"], "failed", error=str(exc), expected=action)
                    except Exception as exc:
                        wf._action_state(action["id"], "uncertain", error=str(exc), expected=action)
                    pool.settle_row("agent_actions", action["id"])
                    continue
            else:
                if wake["due_at"] > clock.now():
                    continue
                action = None
                jobs = self.diagnostic_jobs(exp["id"])
                if not jobs or exp["phase"] not in ("training", "evaluating"):
                    with wf.store.tx() as db:
                        db.execute("UPDATE agent_wakeups SET state='complete' WHERE id=?", (wake["id"],))
                        wakeup_obligations.sync_wakeup(db, wake["id"])
                    self.follow_up(exp["id"], wf.config.codex.monitor_interval_seconds)
                    continue
            cap = wf.agent_turn_ceiling(exp)
            if (
                cap is not None
                and wf.store.one(
                    "SELECT COUNT(*) AS n FROM agent_actions WHERE experiment_id=? AND attempts>0",
                    (exp["id"],),
                )["n"]
                >= cap
            ):
                continue
            if (
                exp["pause_agent"]
                or exp["pending_request"]
                or not wf.agent_ready
                or not wf.agent_pool.quota(eid=exp["id"]).admitted
            ):
                continue
            if action is None:
                prompt = (
                    "Diagnose the supplied sustained telemetry gap from this snapshot. Stale logs can reflect delayed storage visibility; do not infer a compute failure. Use job_status only if essential, then return a concise finding immediately. Do not poll, sleep, run shell commands, change sources, cancel or submit jobs. The backend handles monitoring, terminal accounting, repair and future checks.\nPAYLOAD:\n"
                    + encode(
                        {
                            "phase": "monitor",
                            "experiment_id": exp["id"],
                            "title": (exp.get("title") or "Experiment")[:600],
                            "reason": json.loads(wake["reason"])["reason"],
                            "jobs": jobs,
                        }
                    )
                )
                identity, now = str(uuid4()), clock.now()
                with wf.store.tx() as db:
                    db.execute(
                        "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at) VALUES(?,?,'monitor',?,'pending',?,?,?)",
                        (identity, exp["id"], exp["evidence_revision"], prompt, now, now),
                    )
                    action_obligations.sync_action(db, identity)
                    db.execute(
                        "UPDATE agent_wakeups SET state='queued',action_id=? WHERE id=?",
                        (identity, wake["id"]),
                    )
                    wakeup_obligations.sync_wakeup(db, wake["id"])
                    wf.store.emit(
                        db, exp["id"], "agent_wakeup_queued", {"action_id": identity, "wakeup_id": wake["id"]}
                    )
                action = wf.store.one("SELECT * FROM agent_actions WHERE id=?", (identity,))
            if action["state"] == "pending":
                wf.propose_agent(
                    lambda slot, action=action: wf.dispatch(action, slot),
                    role="monitor",
                    source_table="agent_actions",
                    row=action,
                    owner=experiment_owner(exp["id"]),
                    priority=EXPERIMENT_PRIORITY,
                    any_lane=True,
                )
                return

    async def close(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.socket_path.unlink(missing_ok=True)
            self.server = None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", required=True)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--research", action="store_true")
    args = parser.parse_args()
    for line in sys.stdin:
        try:
            request = json.loads(line)
            if "id" not in request:
                continue
            method = request["method"]
            if method == "initialize":
                result = {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "experimenttracker", "version": "1"},
                }
            elif method == "tools/list":
                result = {"tools": served_tools(args.research)}
            elif method == "tools/call":
                with socket.socket(socket.AF_UNIX) as connection:
                    connection.settimeout(120)
                    connection.connect(args.socket)
                    connection.sendall(
                        (
                            encode(
                                {
                                    "experiment_id": args.experiment,
                                    "name": request["params"]["name"],
                                    "arguments": request["params"].get("arguments", {}),
                                }
                            )
                            + "\n"
                        ).encode()
                    )
                    with connection.makefile("rb") as stream:
                        answer = json.loads(stream.readline(1024 * 1024))
                result = {
                    "content": [
                        {
                            "type": "text",
                            "text": encode(
                                answer.get("result") if answer["ok"] else {"error": answer["error"]}
                            ),
                        }
                    ],
                    "isError": not answer["ok"],
                }
            elif method == "ping":
                result = {}
            else:
                raise ValueError("unsupported MCP method")
            response = {"jsonrpc": "2.0", "id": request["id"], "result": result}
        except Exception as exc:
            response = {
                "jsonrpc": "2.0",
                "id": request.get("id"),
                "error": {"code": -32603, "message": str(exc) or type(exc).__name__},
            }
        print(json.dumps(response), flush=True)


if __name__ == "__main__":
    main()
