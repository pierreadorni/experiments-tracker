"""Persistent experiment wakeups and a local MCP tool bridge owned by the executor."""

import argparse
import asyncio
import json
import socket
import sys
import time
from pathlib import Path
from uuid import UUID, uuid4

from pydantic import Field

from . import artifacts, iteration, reporting
from .codex_client import TerminalTurnError
from .config import StrictModel
from .models import Analysis, Implementation, ReportDocument, ResearchPlan
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


TOOLS = [
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
        "Read only this experiment's saved jobs, measured progress and bounded exact job logs.",
        {},
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
        return {
            "mcp_servers.experimenttracker": {
                "command": sys.executable,
                "args": [
                    "-m",
                    "experimenttracker.controller",
                    "--socket",
                    str(self.socket_path),
                    "--experiment",
                    eid,
                ],
                "startup_timeout_sec": 15,
                "tool_timeout_sec": 120,
            }
        }

    async def connection(self, reader, writer):
        try:
            request = json.loads(await asyncio.wait_for(reader.readline(), 10))
            result = await self.call(request["experiment_id"], request["name"], request.get("arguments", {}))
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
        if kind in ("plan", "prepare"):
            wf.validate_resource_plan(exp, parsed)
        if kind == "prepare":
            python = wf.prepared_python(exp, parsed)
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

    def status(self, eid, *, intent_ids=None):
        wf = self.workflow
        jobs = wf.store.rows("SELECT * FROM jobs WHERE experiment_id=? ORDER BY created_at", (eid,))
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
            item["logs"] = {}
            if job["job_id"]:
                for suffix in ("out", "err"):
                    path = root / f"et-{job['intent_id']}-{job['job_id']}.{suffix}"
                    if path.is_file() and not path.is_symlink():
                        path = artifacts.allowed_path(path, root, max_bytes=None)
                        with path.open("rb") as stream:
                            stream.seek(max(0, path.stat().st_size - 4000))
                            item["logs"][suffix] = stream.read(4000).decode(errors="replace")
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
            now = time.time()
            # A new request replaces pending checks, while queued turns retain
            # their exact outcome. Keep old receipts for idempotent replay.
            db.execute(
                "UPDATE agent_wakeups SET state='superseded' WHERE experiment_id=? AND state='pending'",
                (eid,),
            )
            db.execute(
                "INSERT INTO agent_wakeups(id,experiment_id,reason,due_at,action_id,created_at) VALUES(?,?,?,?,?,?)",
                (identity, eid, payload, now + delay, action_id, now),
            )
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
        if action["phase"] == "monitor" and name not in ("job_status", "schedule_wakeup"):
            raise ValueError("monitoring only permits status reads and future backend checks")
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
            if not iteration.enabled(exp) or not exp["snapshot"]:
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
            if action["phase"] == "monitor":
                payload = json.loads(action["input"].split("\nPAYLOAD:\n", 1)[1])
                return self.status(eid, intent_ids={job["intent_id"] for job in payload["jobs"]})
            return self.status(eid)
        if name == "schedule_wakeup":
            return self.schedule(eid, args["request_id"], args["delay_seconds"], args["reason"], action["id"])
        if name == "submit_stage":
            return await wf.submit_prepared_stage(exp, args["stage_id"], str(UUID(args["request_id"])))
        if name == "register_result":
            stage = args["stage_id"]
            job = wf.store.one(
                "SELECT j.* FROM jobs j JOIN stage_producers p ON p.intent_id=j.intent_id WHERE p.experiment_id=? AND p.stage_id=?"
                if iteration.enabled(exp)
                else "SELECT * FROM jobs WHERE experiment_id=? AND stage_id=? ORDER BY rowid DESC LIMIT 1",
                (eid, stage),
            )
            if not job or job["state"] != "complete":
                raise ValueError("result registration requires a validated terminal successful job")
            if iteration.enabled(exp):
                return wf.store.rows(
                    "SELECT id,name,intent_id,context,host,path,kind FROM output_refs WHERE intent_id=?",
                    (job["intent_id"],),
                )
            names = artifacts.validate_stage(wf.root(eid), stage, exp["snapshot"])
            artifacts.register(wf.store, eid, stage, wf.root(eid), names)
            return wf.store.rows(
                "SELECT id,name,sha256 FROM artifacts WHERE experiment_id=? AND stage_id=? AND evidence_revision=?",
                (eid, stage, exp["evidence_revision"]),
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
        wf, now = self.workflow, time.time()
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
            if not wf.store.one(
                "SELECT 1 FROM agent_wakeups WHERE experiment_id=? AND state IN ('pending','queued')", (eid,)
            ):
                self.schedule(eid, uuid4(), wf.config.codex.monitor_interval_seconds, "Backend compute check")
        # Coalesce backlog from older owners without discarding queued outcomes.
        for row in wf.store.rows("SELECT DISTINCT experiment_id FROM agent_wakeups WHERE state='pending'"):
            pending = wf.store.rows(
                "SELECT id FROM agent_wakeups WHERE experiment_id=? AND state='pending' ORDER BY due_at,created_at,id",
                (row["experiment_id"],),
            )
            with wf.store.tx() as db:
                for obsolete in pending[1:]:
                    db.execute("UPDATE agent_wakeups SET state='superseded' WHERE id=?", (obsolete["id"],))
        for wake in wf.store.rows(
            "SELECT * FROM agent_wakeups WHERE state IN ('pending','queued') ORDER BY due_at LIMIT 50"
        ):
            exp = wf.store.experiment(wake["experiment_id"])
            terminal = exp["deleted_at"] or exp["phase"] in ("complete", "failed", "cancelled")
            if terminal and wake["state"] == "pending":
                with wf.store.tx() as db:
                    db.execute("UPDATE agent_wakeups SET state='superseded' WHERE id=?", (wake["id"],))
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
                        db.execute("UPDATE agent_actions SET state='superseded' WHERE id=?", (action["id"],))
                        db.execute("UPDATE agent_wakeups SET state='superseded' WHERE id=?", (wake["id"],))
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
                        wf.store.emit(db, exp["id"], "agent_wakeup_completed", {"wakeup_id": wake["id"]})
                    self.follow_up(exp["id"], decision.next_check_seconds)
                    continue
                if action["state"] == "failed":
                    # Monitor failures must not fail successful scientific compute.
                    with wf.store.tx() as db:
                        db.execute("UPDATE agent_wakeups SET state='failed' WHERE id=?", (wake["id"],))
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
                    if any(lane.active_action == action["id"] for lane in wf.pool_owner.lanes):
                        continue
                    if not wf.agent_ready or not action["thread_id"] or not action["turn_id"]:
                        continue
                    try:
                        wf._action_state(
                            action["id"],
                            "completed",
                            result=await wf.agent.read_turn(action["thread_id"], action["turn_id"]),
                        )
                    except TerminalTurnError as exc:
                        wf._action_state(action["id"], "failed", error=str(exc))
                    except Exception as exc:
                        wf._action_state(action["id"], "uncertain", error=str(exc))
                    continue
            else:
                if wake["due_at"] > time.time():
                    continue
                action = None
                jobs = self.diagnostic_jobs(exp["id"])
                if not jobs or exp["phase"] not in ("training", "evaluating"):
                    with wf.store.tx() as db:
                        db.execute("UPDATE agent_wakeups SET state='complete' WHERE id=?", (wake["id"],))
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
                or wf.agent_task
                or wf.store.one(
                    "SELECT 1 FROM agent_actions WHERE state IN ('dispatching','running','uncertain')"
                )
                or not wf.gate.decide().admitted
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
                identity, now = str(uuid4()), time.time()
                with wf.store.tx() as db:
                    db.execute(
                        "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at) VALUES(?,?,'monitor',?,'pending',?,?,?)",
                        (identity, exp["id"], exp["evidence_revision"], prompt, now, now),
                    )
                    db.execute(
                        "UPDATE agent_wakeups SET state='queued',action_id=? WHERE id=?",
                        (identity, wake["id"]),
                    )
                    wf.store.emit(
                        db, exp["id"], "agent_wakeup_queued", {"action_id": identity, "wakeup_id": wake["id"]}
                    )
                action = wf.store.one("SELECT * FROM agent_actions WHERE id=?", (identity,))
            if action["state"] == "pending":
                wf.active_action = action["id"]
                wf.agent_task = asyncio.create_task(wf.dispatch(action))
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
                result = {"tools": TOOLS}
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
