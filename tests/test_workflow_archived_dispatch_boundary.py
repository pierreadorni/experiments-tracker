"""Synthetic coverage for the active execution boundary after legacy removal."""

import json
from pathlib import Path

import pytest
from helpers import prepared, wait_for_experiment

from experimenttracker.store import encode


async def test_scriptless_current_stage_runs_saved_recipe_with_exact_job_identity(runtime):
    store, scheduler, _, workflow, eid = await prepared(runtime)
    exp = await wait_for_experiment(workflow, store, eid, lambda e: bool(e["snapshot"]))
    stage = exp["snapshot"]["plan"]["stages"][0]
    assert stage.get("script") is None

    result = await workflow.submit_prepared_stage(exp, stage["id"])

    job = store.one("SELECT * FROM jobs WHERE intent_id=?", (result["intent_id"],))
    assert result["state"] == "submitted"
    assert job["job_id"] == f"demo-{job['intent_id']}" == result["job_id"]
    assert json.loads(job["resources"]) == stage["resources"]
    receipt = json.loads((scheduler.directory / f"{job['intent_id']}.json").read_text())
    assert receipt["job_id"] == job["job_id"]
    assert (Path(job["execution_root"]) / stage["outputs"][0]).is_file()
    await workflow.close()


async def test_frozen_contract_never_reaches_scheduler_dispatch(runtime):
    store, scheduler, _, workflow, eid = await prepared(runtime)
    exp = await wait_for_experiment(workflow, store, eid, lambda e: bool(e["snapshot"]))
    contract = {**exp["contract"], "execution_contract": 2}
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))

    with pytest.raises(ValueError, match="active saved implementation"):
        await workflow.submit_prepared_stage(store.experiment(eid), "train")

    assert scheduler.submissions == 0
    assert store.one("SELECT COUNT(*) AS n FROM jobs WHERE experiment_id=?", (eid,))["n"] == 0
    await workflow.close()
