"""Opt-in, fully synthetic contract-4 CLI demonstration."""

import asyncio
import hashlib
import json
import tempfile
from pathlib import Path
from uuid import uuid4

from .. import clock
from ..config import GatewayConfig, ProjectConfig
from ..slurm import DemoScheduler, Observation
from ..store import Store
from ..workspaces import initialize_project
from .demo_fixture import result_bytes

BRIEF = "Compare two synthetic measurements using one fixed split."
OBJECTIVE = "Compare two synthetic measurements with the fixed synthetic dataset."


class _ResearchDemoScheduler(DemoScheduler):
    async def preflight(self, intent_id, script, root, resources):
        if resources.gpus_per_job:
            raise ValueError("demo mode cannot execute or simulate GPU allocations")
        available = await asyncio.to_thread(lambda: Path(script).is_file() and Path(root).is_dir())
        if not available:
            raise ValueError("synthetic demo job source is unavailable")

    async def observe(self, job_id):
        observed = await super().observe(job_id)
        if observed.state == "COMPLETED" and observed.ended_at is None:
            return Observation(
                observed.state,
                observed.exit_code,
                started_at=observed.started_at,
                ended_at=clock.now(),
                observed_at=clock.now(),
            )
        return observed


class _ScriptedDemoAgent:
    """Bounded deterministic turn script; all actions use the production controller."""

    is_demo = False
    available = True
    resolved_model = "scripted-contract-4-demo"
    models = [{"id": resolved_model, "reasoning_efforts": ["low"], "default_effort": "low"}]
    models_observed_at = None
    start_count = 0

    def __init__(self, workflow):
        self.workflow = workflow
        self.turn = 0
        self.results = {}
        self.turns = {}
        self.approval_id = str(uuid4())

    async def start(self):
        self.start_count += 1

    async def close(self):
        pass

    async def quota(self):
        return {
            "ordinaryUsageAllowed": True,
            "rateLimitsByLimitId": {
                "codex": {
                    "primary": {"usedPercent": 0, "windowDurationMins": 300},
                    "secondary": {"usedPercent": 0, "windowDurationMins": 10080},
                }
            },
        }

    async def session(self, thread_id, workspace, **kwargs):
        return thread_id or "synthetic-contract-4-thread", self.resolved_model

    async def begin(self, thread_id, prompt, schema, **kwargs):
        self.turn += 1
        turn_id = f"synthetic-contract-4-turn-{self.turn}"
        self.turns.setdefault(thread_id, []).append(
            {
                "id": turn_id,
                "status": "inProgress",
                "prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest(),
            }
        )
        return turn_id

    async def finish(self, thread_id, turn_id):
        if self.turn == 1:
            await self.workflow.controller.call(
                self.eid,
                "ask_owner",
                {
                    "request_id": self.approval_id,
                    "question": "Approve this synthetic comparison?",
                    "kind": "approval",
                    "objective": OBJECTIVE,
                },
            )
            result = {"status": "wait", "summary": "Synthetic approval requested", "next": "timer:61"}
        elif self.turn == 2:
            receipt = await self.workflow.controller.call(
                self.eid,
                "submit_job",
                {
                    "request_id": str(uuid4()),
                    "approval_request_id": self.approval_id,
                    "objective": OBJECTIVE,
                    "name": "synthetic-c4-measurement",
                    "command": [
                        "python3",
                        "-c",
                        "from pathlib import Path; Path('results.json').write_bytes(bytes.fromhex('"
                        + result_bytes().hex()
                        + "'))",
                    ],
                    "resources": {"cpus_per_job": 1, "memory_mb_per_job": 64, "walltime_minutes_per_job": 1},
                    "result_file": "source/results.json",
                },
            )
            self.intent_id = receipt["intent_id"]
            result = {
                "status": "wait",
                "summary": "Synthetic CPU measurement submitted",
                "next": "timer:61",
            }
        elif self.turn == 3:
            await self.workflow.controller.call(
                self.eid,
                "register_result",
                {"request_id": str(uuid4()), "intent_id": self.intent_id, "path": "source/results.json"},
            )
            document = {
                "title": "Synthetic contract-4 demo report",
                "standfirst": "A deterministic workflow demonstration, not scientific evidence.",
                "blocks": [
                    {"kind": "paragraph", "text": "The measurements below are synthetic and bounded."},
                    {
                        "kind": "table",
                        "caption": "Synthetic measurements",
                        "columns": ["Variant", "MSE"],
                        "rows": [
                            [{"text": variant}, {"metric": {"name": "mse", "variant": variant}}]
                            for variant in ("linear", "quadratic")
                        ],
                    },
                ],
            }
            await self.workflow.controller.call(
                self.eid, "update_report", {"request_id": str(uuid4()), "document": document}
            )
            result = {"status": "continue", "summary": "Synthetic report is ready", "next": "Complete"}
        else:
            result = {
                "status": "done",
                "summary": "Synthetic contract-4 report complete",
                "next": "None",
                "integrate": {"value": False, "reason": "Synthetic demonstration"},
            }
        self.results[(thread_id, turn_id)] = result
        for turn in self.turns[thread_id]:
            if turn["id"] == turn_id:
                turn["status"] = "completed"
        return result

    async def read_thread(self, thread_id):
        return list(self.turns.get(thread_id, []))

    async def read_turn(self, thread_id, turn_id, *, expected_prompt_hash=None):
        turn = next((item for item in self.turns.get(thread_id, []) if item["id"] == turn_id), None)
        if turn is None or turn["status"] != "completed":
            raise ValueError("synthetic demo turn is not exactly completed")
        if expected_prompt_hash is not None and turn["prompt_sha256"] != expected_prompt_hash:
            raise ValueError("synthetic demo turn prompt identity mismatch")
        return self.results[(thread_id, turn_id)]


def _temporary_config(config, root):
    checkout = root / "project"
    checkout.mkdir()
    initialize_project(checkout)
    project = ProjectConfig(id="synthetic-demo", name="Synthetic demo", checkout=checkout, research_loop=True)
    return config.model_copy(
        update={
            "state_dir": root / "state",
            "job_output_root": root / "outputs",
            "backup_dir": root / "backups",
            "projects_root": root / "projects",
            "projects": [ProjectConfig(id="test-project", name="Test project"), project],
            "scheduler_seconds": 0.1,
            "gateway": GatewayConfig(),
            "api_token_file": root / "unused-token",
        }
    )


async def run_contract4_demo(config, max_seconds):
    """Run an isolated real C4 lifecycle; no state escapes the temporary directory."""
    if config.mode != "demo":
        print("demo refuses a live configuration before creating demo state.", flush=True)
        return 1
    with tempfile.TemporaryDirectory(prefix="experimenttracker-c4-demo-") as temporary:
        root = Path(temporary)
        demo_config = _temporary_config(config, root)
        lock = None
        store = None
        workflow = None
        try:
            from ..cli import OwnershipLock
            from ..codex_client import DemoAgent
            from ..workflow import Workflow

            # Own only the isolated temporary state, never the configured state directory.
            lock = OwnershipLock(demo_config.state_dir)
            store = Store(demo_config.state_dir / "workflow.sqlite")
            workflow = Workflow(
                demo_config,
                store,
                _ResearchDemoScheduler(demo_config.state_dir / "demo-scheduler"),
                DemoAgent(),
            )
            agent = _ScriptedDemoAgent(workflow)
            workflow.agent_pool.primary.agent = agent
            workflow._attach_handlers(workflow.agent_pool.primary)
            workflow.agent_pool.primary.agent_ready = True
            workflow.agent_pool.primary.resolved_model = agent.resolved_model
            workflow.agent_pool.primary.models = agent.models
            workflow.gate.update(await agent.quota())
            eid = store.command(
                uuid4(),
                "create",
                {
                    "brief": BRIEF,
                    "recipe_id": "agent_experiment_v1",
                    "execution_contract": 4,
                    "project_id": "synthetic-demo",
                },
            )["experiment_id"]
            agent.eid = eid
            workflow.workspaces.ensure_branch(eid)
            started = asyncio.get_running_loop().time()
            wall_now = clock.now
            synthetic_elapsed = [0, 0]
            clock.now = lambda: wall_now() + synthetic_elapsed[0]
            answered = False
            job_window_advanced = False
            while asyncio.get_running_loop().time() - started < max_seconds:
                await workflow.tick()
                # Advance only after each scripted turn so owner/job debounce windows
                # are deterministic without a real-minute wait.
                if agent.turn > synthetic_elapsed[1]:
                    synthetic_elapsed[1] = agent.turn
                    synthetic_elapsed[0] += 61
                experiment = store.experiment(eid)
                if not job_window_advanced and store.one(
                    "SELECT 1 FROM jobs WHERE experiment_id=? AND state='complete'", (eid,)
                ):
                    synthetic_elapsed[0] += 61
                    job_window_advanced = True
                pending = experiment.get("pending_request")
                if (
                    pending
                    and not answered
                    and pending.get("id") == agent.approval_id
                    and pending.get("kind") == "approval"
                    and pending.get("params", {}).get("objective") == OBJECTIVE
                    and not pending.get("answer")
                ):
                    answer_id = str(uuid4())
                    store.command(
                        answer_id,
                        "answer",
                        {
                            "request_id": answer_id,
                            "schema_version": 1,
                            "pending_request_id": pending["id"],
                            "answer": "accept",
                        },
                        eid=eid,
                    )
                    answered = True
                    synthetic_elapsed[0] += 61
                report = store.one(
                    "SELECT state,document FROM report_versions WHERE experiment_id=? ORDER BY version DESC LIMIT 1",
                    (eid,),
                )
                if experiment["phase"] == "complete" and report and report["state"] == "ready":
                    decoded = json.loads(report["document"])
                    print(
                        json.dumps(
                            {
                                "contract": 4,
                                "mode": "demo",
                                "phase": experiment["phase"],
                                "report_state": report["state"],
                                "report_title": decoded["title"],
                                "synthetic_only": True,
                            }
                        ),
                        flush=True,
                    )
                    return 0
                await asyncio.sleep(demo_config.scheduler_seconds)
            print(
                "Bounded contract-4 demo ended before a ready report: "
                + json.dumps(
                    {
                        "phase": store.experiment(eid)["phase"],
                        "hold": store.experiment(eid)["hold_reason"],
                        "pending_request": store.experiment(eid)["pending_request"],
                        "action_results": store.rows(
                            "SELECT result FROM agent_actions WHERE experiment_id=?", (eid,)
                        ),
                        "actions": store.rows(
                            "SELECT phase,state,error FROM agent_actions WHERE experiment_id=?", (eid,)
                        ),
                        "jobs": store.rows("SELECT state,error FROM jobs WHERE experiment_id=?", (eid,)),
                        "reports": store.rows(
                            "SELECT state,error FROM report_versions WHERE experiment_id=?", (eid,)
                        ),
                    }
                ),
                flush=True,
            )
            return 2
        finally:
            if "wall_now" in locals():
                clock.now = wall_now
            if workflow:
                await workflow.close()
            if store:
                store.close()
            if lock:
                lock.close()
