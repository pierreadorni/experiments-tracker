"""Contract 3 monitor scenario: healthy telemetry never opens an extra agent turn.

Synthetic mechanism acceptance only: no live scheduler, model runtime or research.
"""

import json
import os
import sys
from pathlib import Path

import pytest
from helpers import seed_saved_contract3

from experimenttracker.config import JobResources
from experimenttracker.slurm import Observation
from scenario import settle

PLAN = {
    "schema_version": 3,
    "summary": "Synthetic healthy long job.",
    "protocol": "Synthetic mechanism check, not scientific research.",
    "synthetic_demo": True,
    "parameters_json": "{}",
    "files": ["recipe.py"],
    "figures": [],
    "stages": [
        {
            "id": "train",
            "title": "Synthetic training",
            "needs": [],
            "outputs": ["metrics.json"],
            "result_file": "metrics.json",
            "resources": JobResources().model_dump(),
        }
    ],
}
SOURCE = "print('synthetic scenario recipe')\n"


def plan_turn(payload, workspace):
    return {
        "schema_version": 3,
        "title": "Synthetic healthy job",
        "summary": PLAN["summary"],
        "protocol": PLAN["protocol"],
        "stages": [
            {
                "id": "train",
                "title": "Synthetic training",
                "summary": "Synthetic task.",
                "needs": [],
                "resources": JobResources().model_dump(),
            }
        ],
    }


def prepare_turn(payload, workspace):
    runtime = workspace / "runtime/bin/python"
    runtime.parent.mkdir(parents=True, exist_ok=True)
    if not runtime.exists():
        runtime.symlink_to(sys.executable)
    (workspace / "recipe.py").write_text(SOURCE)
    return {**PLAN, "python_executable": str(runtime)}


def saved_job(store, eid):
    return store.one("SELECT * FROM jobs WHERE experiment_id=?", (eid,))


def refresh_telemetry(scenario, job, *, progress, completed):
    """Append this exact intent/job's output tail and stamp it from the fake clock.

    The controller reads progress from the bounded stdout tail and its freshness
    from the real mtime, so both are refreshed in fake time rather than left stale.
    """
    path = Path(job["execution_root"]) / f"et-{job['intent_id']}-{job['job_id']}.out"
    sample = {
        "stage": job["stage_id"],
        "completed": completed,
        "total": 10,
        "timestamp": scenario.clock.now(),
    }
    line = "ET_PROGRESS " + json.dumps(sample) + "\n" if progress else "synthetic epoch output\n"
    with path.open("a") as log:
        log.write(line)
    os.utime(path, (scenario.clock.now(), scenario.clock.now()))


async def steps(scenario, count):
    """Advance through explicit ticks only; a condition is never inferred from yields."""
    for _ in range(count):
        await scenario.workflow.tick(force_observe=True)
        await settle(scenario.workflow)
        scenario.clock.advance(scenario.step)


@pytest.mark.parametrize("progress", [True, False])
async def test_healthy_long_job_makes_zero_additional_agent_turns(scenario, progress, contract_three_engine):
    store, slurm_backend, agent_backend = scenario.store, scenario.slurm_backend, scenario.agent_backend
    eid = seed_saved_contract3(store, "Synthetic healthy long job.")
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    scenario.agent.script("plan", turn=plan_turn, turns=1)
    scenario.agent.script("prepare", turn=prepare_turn, turns=1)
    await scenario.approve_plan(eid)

    # The engine submits through its own dispatch; the fake scheduler then reports
    # RUNNING, so no job row is inserted by this scenario.
    await scenario.run(lambda: saved_job(store, eid) is not None and saved_job(store, eid)["job_id"])
    job = saved_job(store, eid)
    slurm_backend.observations[job["job_id"]] = Observation("RUNNING", started_at=scenario.clock.now())
    await scenario.run(lambda: saved_job(store, eid)["scheduler_state"] == "RUNNING")
    job = saved_job(store, eid)
    assert job["state"] == "submitted" and slurm_backend.submit_calls == [job["intent_id"]]
    refresh_telemetry(scenario, job, progress=progress, completed=1)
    await steps(scenario, 1)
    # The synthetic tail was read as instrumented or uninstrumented telemetry.
    assert (saved_job(store, eid)["progress"] is not None) == progress

    # Baseline after dispatch: engine-driven planning and preparation turns only.
    actions = store.rows("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,))
    assert len(actions) == 2 and agent_backend.turns_started == 2 and len(agent_backend.starts) == 2

    # Only this scenario uses the documented sentinel so controller wakeups run in
    # demo mode; the workflow's own tick drives them.
    scenario.workflow.controller.server = object()
    try:
        for completed in range(2, 10):
            refresh_telemetry(scenario, job, progress=progress, completed=completed)
            await steps(scenario, 1)
            scenario.clock.advance(1200)  # long fake elapsed time between checks
        assert not store.rows("SELECT * FROM agent_actions WHERE experiment_id=? AND phase='monitor'", (eid,))
        assert len(store.rows("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,))) == 2
        assert agent_backend.turns_started == 2 and len(agent_backend.starts) == 2
        assert slurm_backend.submit_calls == [job["intent_id"]]
        # The zero-turn claim needs due wakeups that actually ran, and a job the
        # engine still considers healthy at the final fake time.
        assert store.rows("SELECT * FROM agent_wakeups WHERE experiment_id=? AND state='complete'", (eid,))
        assert scenario.workflow.controller.diagnostic_jobs(eid) == []
        assert saved_job(store, eid)["scheduler_state"] == "RUNNING"
    finally:
        scenario.workflow.controller.server = None
    await scenario.close()
