"""Persistent experiment wakeups and a local MCP tool bridge owned by the executor."""

import argparse
import asyncio
import json
import socket
import sys
from pathlib import Path
from uuid import UUID

from pydantic import Field

from . import artifacts, autopilot_policy, autopilot_tools, clock, iteration, reporting
from .agent_pool import experiment_owner
from .config import StrictModel
from .core import wakeup_obligations
from .legacy_archive import execution_frozen
from .models import Analysis, Implementation, ResearchPlan
from .research.admission import SubmitJobRequest
from .research.cancel_tool import CancelJob, cancel_job
from .research.checkpoints import SaveCheckpoint, save_checkpoint
from .research.figure_tool import RegisterFigure, register_figure
from .research.integration_tool import RequestIntegration, request_integration
from .research.job_dispatch import _replay as replay_research_job
from .research.job_dispatch import dispatch as dispatch_research_job
from .research.owner_tool import AskOwnerQuestion, ask_owner
from .research.remote_dispatch import dispatch as dispatch_remote_research_job
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
TOOLS = [
    tool(
        "job_status",
        "Read this experiment's saved jobs. Use summary_only for a compact list; select intent_ids to inspect bounded exact logs and details.",
        {
            "intent_ids": {"type": "array", "items": {"type": "string"}, "maxItems": 32},
            "summary_only": {"type": "boolean"},
        },
        [],
    ),
]

# Contract 4 advertises only research tools, never the stage-engine tools.
RESEARCH_TOOLS = [
    tool_from_model(
        "submit_job",
        "Submit one locally scoped research job after exact owner approval. Optionally declare result_file as the bounded relative path register_result must use for this job. Repeating a request_id adopts the saved outcome and never resubmits an uncertain job.",
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
# An Autopilot lead (C8) gets its project tools and these shared research tools only;
# compute tools such as submit_job and cancel_job are never served to it.
LEAD_SHARED_TOOLS = ("cluster_info", "save_checkpoint", "update_report", "ask_owner")
LEAD_TOOLS = autopilot_tools.TOOLS + [item for item in RESEARCH_TOOLS if item["name"] in LEAD_SHARED_TOOLS]
LEAD_TOOL_NAMES = frozenset(item["name"] for item in LEAD_TOOLS)


def served_tools(research=False, lead=False):
    """The tools an MCP process advertises for one saved execution contract."""
    return LEAD_TOOLS if lead else RESEARCH_TOOLS if research else TOOLS


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
        contract = (json.loads(row["contract"]) or {}) if row else {}
        research = contract.get("execution_contract") == 4
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
        if research and autopilot_policy.lead_run_id(contract):
            args.append("--autopilot-lead")
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
            runtime = await wf.finish_threaded(wf.workspaces.shared_runtime, exp["id"])
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
        lead = autopilot_policy.lead_run_id(exp.get("contract"))
        if lead and (exp.get("contract") or {}).get("execution_contract") == 4:
            if name not in LEAD_TOOL_NAMES:
                raise ValueError("an Autopilot lead may call only its project tools")
            if name in autopilot_tools.MODELS:
                require_research_write(wf, exp, action)
                from .autopilot import Autopilot

                # The service replays a saved receipt, then checks the action's control revision.
                return Autopilot(wf.store, wf.config).invoke(action["id"], name, args)
            run = wf.store.one("SELECT control_revision FROM autopilot_runs WHERE id=?", (lead,))
            if not run or action["autopilot_control_revision"] != run["control_revision"]:
                raise ValueError("project turn authority changed; refresh at a safe continuation")
        elif (exp.get("contract") or {}).get("execution_contract") == 4 and name not in RESEARCH_TOOL_NAMES:
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
            remote_route = (exp.get("contract") or {}).get("execution_contract") == 4 and (
                exp.get("contract") or {}
            ).get("execution", {}).get("context") == "jean-zay"
            if remote_route:
                row = replay_research_job(wf.store, eid, action, request)
                if row is not None:
                    manifest = json.loads(row["execution_manifest"] or "{}")
                    if (manifest.get("execution") or {}).get("context") != "jean-zay":
                        raise Conflict("job intent identity already belongs to another submission")
                else:
                    result = await dispatch_remote_research_job(wf, eid, action, request)
                    row = result["job"]
            else:
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
            return await wf.finish_threaded(register_research_result, wf, exp, action, request)
        if name == "register_figure":
            require_research_write(wf, exp, action)
            request = RegisterFigure.model_validate(args)
            # The copy and hash are bounded filesystem work; keep the controller
            # loop free while the handler repeats its own saved-turn gates.
            return await wf.finish_threaded(register_figure, wf, exp, action, request)
        if name == "job_status":
            # Discussion turns on saved legacy records keep a read-only status view.
            requested = args.get("intent_ids")
            if requested is not None:
                if not isinstance(requested, list) or len(requested) > 32:
                    raise ValueError("select at most 32 job intents")
                requested = {str(UUID(value)) for value in requested}
            return self.status(eid, intent_ids=requested, summary_only=args.get("summary_only", False))
        raise ValueError("unknown experiment tool")

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
    parser.add_argument("--autopilot-lead", action="store_true")
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
                result = {"tools": served_tools(args.research, args.autopilot_lead)}
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
