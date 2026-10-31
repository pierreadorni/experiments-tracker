"""Mixed agent/Slurm infrastructure acceptance; all measurements are synthetic."""

import asyncio
import copy
import json
import sys
from pathlib import Path
from uuid import uuid4

import pytest
from test_generic import approve_plan
from test_iteration import PLAN, IterationAgent
from test_workflow import until

from experimenttracker import iteration
from experimenttracker.codex_client import NeedsAttention
from experimenttracker.models import Implementation, ResearchPlan
from experimenttracker.workflow import Workflow

CHECK = {
    "id": "model_check",
    "title": "Check synthetic model before training",
    "executor": "agent",
    "instruction": "Run the tiny forward check, fix errors interactively and save ready.json.",
    "resources": None,
    "needs": [],
    "outputs": ["ready.json"],
}


def mixed_plan():
    plan = copy.deepcopy(PLAN)
    plan["stages"].insert(0, CHECK.copy())
    plan["stages"][1]["needs"] = ["model_check"]
    return plan


class MixedAgent(IterationAgent):
    def __init__(self):
        super().__init__()
        self.check_calls = 0
        self.lose_response = False
        self.missing_output_once = False
        self.roots = []

    async def read_turn(self, thread_id, turn_id, **kwargs):
        return await super().read_turn(thread_id, turn_id)

    async def begin(self, thread_id, prompt, schema, *, workspace=None, **kwargs):
        payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
        phase = payload["phase"]
        if not phase.startswith("agent:"):
            turn = await super().begin(thread_id, prompt, schema, workspace=workspace)
            if phase in ("plan", "prepare"):
                value = self.results[(thread_id, turn)]
                stages = mixed_plan()["stages"]
                if phase == "plan":
                    stages = [
                        {k: s[k] for k in ("id", "title", "needs", "resources", "executor") if k in s}
                        | {"summary": "Synthetic task"}
                        for s in stages
                    ]
                value["stages"] = stages
            return turn
        self.calls += 1
        self.check_calls += 1
        self.roots.append(workspace)
        assert Path(payload["workspace"]) in kwargs["additional_writable_roots"]
        assert workspace == Path(payload["attempt_directory"])
        assert "summary" in schema["properties"]
        job = iteration.agent_job(self.wf, self.store.experiment(self.eid), phase)
        assert job["state"] == "agent"  # Intent persisted before the tool loop.
        assert (
            self.store.one(
                "SELECT state FROM agent_actions WHERE phase=? ORDER BY rowid DESC LIMIT 1", (phase,)
            )["state"]
            == "dispatching"
        )
        assert self.scheduler.submissions == 0
        if not (self.missing_output_once and self.check_calls == 1):
            # A failing tool call is corrected in the SAME simulated model turn.
            script = workspace / "tiny_check.py"
            script.write_text("assert 2 + 2 == 5\n")
            process = await asyncio.create_subprocess_exec(
                sys.executable, str(script), stderr=asyncio.subprocess.PIPE
            )
            await process.communicate()
            assert process.returncode == 1
            script.write_text(
                "import json\nfrom pathlib import Path\nassert 2 + 2 == 4\nPath('ready.json').write_text(json.dumps({'ok':True}))\n"
            )
            process = await asyncio.create_subprocess_exec(sys.executable, str(script), cwd=workspace)
            assert await process.wait() == 0
            assert await self.wf.controller.call(self.eid, "check_agent_stage", {}) == {"valid": True}
            source = Path(payload["workspace"]) / "recipe.py"
            source.write_text(source.read_text() + "\n# Corrected by the interactive model check.\n")
        turn = f"check-{self.check_calls}"
        self.results[(thread_id, turn)] = {"summary": "Synthetic check fixed and passed."}
        if self.lose_response:
            raise NeedsAttention("simulated lost turn response")
        return turn


async def setup_mixed(runtime, agent=None):
    store, scheduler, _, old = runtime
    agent = agent or MixedAgent()
    wf = Workflow(old.config, store, scheduler, agent)
    eid = store.command(
        uuid4(), "create", {"brief": "Synthetic mixed graph", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]
    agent.wf, agent.store, agent.eid, agent.scheduler = wf, store, eid, scheduler
    await approve_plan(wf, store, eid)
    return store, scheduler, agent, wf, eid


async def test_agent_check_fixes_in_turn_and_gates_slurm_with_original_outputs(runtime):
    store, scheduler, agent, wf, eid = await setup_mixed(runtime)
    await until(wf, store, eid, lambda e: e["phase"] == "complete")
    assert agent.check_calls == 1
    assert scheduler.submissions == 3  # No scheduler job for the small check.
    job = iteration.agent_job(wf, store.experiment(eid), "agent:model_check")
    assert job["state"] == "complete" and job["job_id"] is None
    train = store.one("SELECT * FROM jobs WHERE stage_id='train'")
    assert (
        "Corrected by the interactive model check"
        in (Path(train["execution_root"]) / "snapshot/recipe.py").read_text()
    )
    assert (
        "Corrected by the interactive model check"
        not in (Path(job["execution_root"]) / "snapshot/recipe.py").read_text()
    )
    ref = store.one("SELECT * FROM output_refs WHERE intent_id=?", (job["intent_id"],))
    binding = store.one("SELECT * FROM output_bindings WHERE output_id=?", (ref["id"],))
    assert binding is not None
    projection = store.projection(eid)
    assert len(projection["jobs"]) == 3
    assert next(s for s in projection["stages"] if s["id"] == "model_check")["state"] == "complete"
    plan = Implementation.model_validate(store.experiment(eid)["snapshot"]["plan"])
    plan.reuse_stages = ["model_check"]
    plan.reuse_rationale = "The synthetic check already passed."
    assert iteration.reuse(wf, store.experiment(eid), plan)["model_check"]["intent_id"] == job["intent_id"]
    await wf.close()


async def test_missing_output_corrects_only_agent_stage_in_same_attempt(runtime):
    store, scheduler, agent, wf, eid = await setup_mixed(runtime)
    agent.missing_output_once = True
    await until(wf, store, eid, lambda e: e["phase"] == "complete")
    assert agent.check_calls == 2
    assert len(set(agent.roots)) == 1
    assert scheduler.submissions == 3
    assert store.experiment(eid)["evidence_revision"] == 1
    actions = store.rows("SELECT * FROM agent_actions WHERE phase='agent:model_check' ORDER BY created_at")
    assert [a["state"] for a in actions] == ["failed", "completed"]
    assert actions[1]["previous_action_id"] == actions[0]["id"]
    await wf.close()


async def test_uncertain_agent_turn_never_repeats_or_submits_dependent_work(runtime):
    store, scheduler, agent, wf, eid = await setup_mixed(runtime)
    agent.lose_response = True
    await until(wf, store, eid, lambda e: e["hold_reason"] == "agent_dispatch_uncertain")
    for _ in range(4):
        await wf.tick()
    assert agent.check_calls == 1 and scheduler.submissions == 0
    action = store.one("SELECT * FROM agent_actions WHERE phase='agent:model_check'")
    assert action["state"] == "uncertain"
    # Simulate owner restart: exact recorded turn is adopted, never rerun.
    await wf.close()
    replacement = Workflow(wf.config, store, scheduler, agent)
    agent.wf = replacement
    await replacement.reconcile_action(action["id"], turn_id="check-1")
    await until(replacement, store, eid, lambda e: e["phase"] == "complete")
    assert agent.check_calls == 1 and scheduler.submissions == 3
    await replacement.close()


async def test_agent_pause_and_compute_pause_hold_dispatch(runtime):
    store, scheduler, agent, wf, eid = await setup_mixed(runtime)
    await until(wf, store, eid, lambda e: bool(e["snapshot"]))
    store.update(eid, pause_compute=1)
    await wf.tick()
    assert agent.check_calls == 0 and scheduler.submissions == 0
    store.update(eid, pause_compute=0, pause_agent=1)
    await wf.tick()
    assert agent.check_calls == 0 and scheduler.submissions == 0
    store.update(eid, pause_agent=0)
    await until(wf, store, eid, lambda e: e["phase"] == "complete")
    assert agent.check_calls == 1
    await wf.close()


def test_executor_contract_validation():
    plan = mixed_plan()
    assert Implementation.model_validate(plan).stages[0].executor == "agent"
    for change in (
        {"resources": PLAN["stages"][0]["resources"]},
        {"instruction": None},
        {"script": "check.sh"},
    ):
        invalid = copy.deepcopy(plan)
        invalid["stages"][0].update(change)
        with pytest.raises(ValueError):
            Implementation.model_validate(invalid)
    historical = copy.deepcopy(plan)
    historical["schema_version"] = 2
    with pytest.raises(ValueError, match="schema 3"):
        Implementation.model_validate(historical)
    review = {
        "schema_version": 3,
        "title": "Mixed",
        "summary": "Synthetic",
        "protocol": plan["protocol"],
        "stages": [
            {
                "id": "check",
                "title": "Check",
                "summary": "Small check",
                "executor": "agent",
                "needs": [],
                "resources": None,
            }
        ],
    }
    assert ResearchPlan.model_validate(review).stages[0].executor == "agent"


async def test_agent_rejects_foreign_inputs_and_scheduler_dispatch(runtime):
    store, scheduler, agent, wf, eid = await setup_mixed(runtime)
    await until(wf, store, eid, lambda e: bool(e["snapshot"]))
    with pytest.raises(ValueError, match="managed agent loop"):
        await wf.submit_prepared_stage(store.experiment(eid), "model_check")
    await until(wf, store, eid, lambda e: e["phase"] == "complete")
    exp = store.experiment(eid)
    job = iteration.agent_job(wf, exp, "agent:model_check")
    with store.tx() as db:
        db.execute(
            "UPDATE output_refs SET context='jean-zay',host='remote' WHERE intent_id=?", (job["intent_id"],)
        )
    plan = copy.deepcopy(exp["snapshot"])
    # A local agent consumer cannot treat a remote producer path as local.
    plan["plan"]["stages"][1].update(executor="agent", resources=None, instruction="Synthetic check")
    exp = {**exp, "snapshot": plan}
    with pytest.raises(ValueError, match="another host"):
        iteration.stage_attempt(wf, exp, "train", str(uuid4()))
    await wf.close()


async def test_agent_stage_obeys_quota_and_does_not_spend_job_budget(runtime):
    store, scheduler, agent, wf, eid = await setup_mixed(runtime)
    await until(wf, store, eid, lambda e: bool(e["snapshot"]))
    # Three scheduled tasks fit exactly; the extra agent check is not a fourth job.
    wf.config.slurm.max_jobs_per_experiment = 3
    wf.validate_resource_plan(
        store.experiment(eid), Implementation.model_validate(store.experiment(eid)["snapshot"]["plan"])
    )
    original_quota = agent.quota

    async def denied_quota():
        return {**await original_quota(), "ordinaryUsageAllowed": False}

    agent.quota = denied_quota
    await wf.refresh_quota()
    await until(wf, store, eid, lambda e: e["hold_reason"] == "quota_hold")
    assert store.experiment(eid)["hold_reason"] == "quota_hold"
    assert agent.check_calls == 0 and scheduler.submissions == 0
    agent.quota = original_quota
    await wf.refresh_quota()
    await until(wf, store, eid, lambda e: e["phase"] == "complete")
    assert scheduler.submissions == 3 and agent.check_calls == 1
    await wf.close()


async def test_ordering_only_gate_does_not_bind_local_files_for_remote_training(runtime):
    store, scheduler, agent, wf, eid = await setup_mixed(runtime)
    await until(wf, store, eid, lambda e: e["phase"] == "complete")
    exp = store.experiment(eid)
    plan = copy.deepcopy(exp["snapshot"])
    plan["plan"]["stages"][1]["input_output_ids"] = []
    exp = {
        **exp,
        "snapshot": plan,
        "contract": {**exp["contract"], "execution": {**exp["contract"]["execution"], "context": "jean-zay"}},
    }
    root, manifest, refs = iteration.stage_attempt(wf, exp, "train", str(uuid4()))
    assert refs == []
    assert json.loads((root / "inputs.json").read_text()) == []
    assert manifest["plan"]["stages"][1]["needs"] == ["model_check"]
    await wf.close()


async def test_remote_runtime_requires_scripts_only_for_slurm_stages(runtime):
    store, scheduler, agent, wf, eid = await setup_mixed(runtime)
    exp = store.experiment(eid)
    exp = {
        **exp,
        "contract": {**exp["contract"], "execution": {**exp["contract"]["execution"], "context": "jean-zay"}},
    }
    value = mixed_plan()
    value["files"].append("run.sh")
    for stage in value["stages"][1:]:
        stage["script"] = "run.sh"
    plan = Implementation.model_validate(value)
    assert wf.prepared_python(exp, plan) == Path(sys.executable)
    assert plan.stages[0].script is None
    plan.stages[1].script = None
    with pytest.raises(ValueError, match="Jean Zay stages"):
        wf.prepared_python(exp, plan)
    await wf.close()


async def test_agent_evaluation_publishes_metrics_before_dependent_plot(runtime):
    class AgentEvaluation(MixedAgent):
        async def begin(self, thread_id, prompt, schema, *, workspace=None, **kwargs):
            payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
            if payload["phase"] == "agent:evaluate":
                assert self.scheduler.submissions == 1
                model = next(ref for ref in payload["inputs"] if ref["name"] == "model.json")
                value = json.loads(await asyncio.to_thread(Path(model["path"]).read_text))["weight"]
                (workspace / "metrics.json").write_text(
                    json.dumps(
                        {
                            "schema_version": 1,
                            "protocol": payload["protocol"],
                            "sample_count": 12,
                            "metrics": [
                                {"name": "score", "variant": "synthetic", "value": value, "unit": ""}
                            ],
                        }
                    )
                )
                assert await self.wf.controller.call(self.eid, "check_agent_stage", {}) == {"valid": True}
                self.results[(thread_id, "agent-evaluation")] = {"summary": "Synthetic evaluation."}
                return "agent-evaluation"
            turn = await super().begin(thread_id, prompt, schema, workspace=workspace, **kwargs)
            if payload["phase"] in ("plan", "prepare"):
                stage = self.results[(thread_id, turn)]["stages"][2]
                stage.update(executor="agent", resources=None)
                if payload["phase"] == "prepare":
                    stage["instruction"] = "Calculate the small synthetic metric from the saved model."
            return turn

    store, scheduler, agent, wf, eid = await setup_mixed(runtime, AgentEvaluation())
    await until(wf, store, eid, lambda e: bool(e["partial_results"]))
    assert store.experiment(eid)["partial_results"] is not None
    assert store.one("SELECT state FROM stages WHERE id='plot'")["state"] != "complete"
    assert store.rows("SELECT * FROM report_versions WHERE experiment_id=?", (eid,))
    await until(wf, store, eid, lambda e: e["phase"] == "complete")
    assert scheduler.submissions == 2
    await wf.close()
