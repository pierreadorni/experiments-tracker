"""Inert bounded result reads scoped to the exact successful producer job."""

import json
from uuid import uuid4

import pytest
from pydantic import ValidationError
from test_research_tools import add_action, saved_contract4

from experimenttracker.research.result_tool import RegisterResult, register_result

RESULT = {
    "schema_version": 1,
    "protocol": "toy protocol",
    "metrics": [{"name": "loss", "variant": "v1", "value": 0.25, "unit": "fraction"}],
}


def request(intent, path="results.json"):
    return {"request_id": str(uuid4()), "intent_id": str(intent), "path": path}


def add_saved_job(store, eid, intent, root, *, state="complete", executor="slurm"):
    with store.tx() as db:
        db.execute(
            "INSERT INTO stages(experiment_id,id,state,attempt,updated_at) VALUES(?,'evaluate','complete',1,0)",
            (eid,),
        )
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,job_id,script_hash,resources,"
            "snapshot_hash,created_at,execution_root,execution_manifest,ended_at) "
            "VALUES(?,?,'evaluate',1,?,'123','s','{}','h',0,?,?,1)",
            (intent, eid, state, str(root), json.dumps({"executor": executor})),
        )


def active_turn(store, eid):
    store.update(eid, phase="research")
    action_id = add_action(store, eid)
    with store.tx() as db:
        db.execute("UPDATE experiments SET thread_id='thread-1' WHERE id=?", (eid,))
        db.execute("UPDATE agent_actions SET thread_id='thread-1' WHERE id=?", (action_id,))
    return store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,))


def test_result_request_requires_uuid_intent_and_bounded_relative_path():
    valid = request(uuid4())
    assert RegisterResult.model_validate(valid).path == "results.json"
    for path in ("../secret.json", "/tmp/result.json", "", "x" * 1025):
        with pytest.raises(ValidationError):
            RegisterResult.model_validate(request(uuid4(), path))
    with pytest.raises(ValidationError):
        RegisterResult.model_validate({**valid, "intent_id": "bad"})
    with pytest.raises(ValidationError):
        RegisterResult.model_validate({**valid, "extra": True})


def test_register_result_reads_exact_successful_slurm_job_and_validates(runtime, tmp_path):
    store, _, _, workflow = runtime
    eid = saved_contract4(store)
    action = active_turn(store, eid)
    intent = str(uuid4())
    root = workflow.projects.base_root(eid) / "attempts" / intent
    root.mkdir(parents=True)
    add_saved_job(store, eid, intent, root)
    (root / "results.json").write_text(json.dumps(RESULT))
    req = request(intent)
    result = register_result(workflow, store.experiment(eid), action, req)
    assert result["artifact_id"]
    assert result["artifact_id"]
    assert result["intent_id"] == intent
    assert register_result(workflow, store.experiment(eid), action, req) == result
    (root / "results.json").unlink()
    assert register_result(workflow, store.experiment(eid), action, req) == result
    mismatch = {**req, "path": "other.json"}
    with pytest.raises(Exception, match="request identity"):
        register_result(workflow, store.experiment(eid), action, mismatch)
    assert store.rows("SELECT * FROM events WHERE kind='research_result_registered'")[0]["payload"]
    other = request(intent)
    (root / "results.json").write_text('{"protocol":"p","metrics":[{"value":NaN}]}')
    with pytest.raises(ValueError):
        register_result(workflow, store.experiment(eid), action, other)


def test_register_result_refuses_foreign_failed_agent_jobs_and_symlink(runtime, tmp_path):
    store, _, _, workflow = runtime
    eid = saved_contract4(store)
    action = active_turn(store, eid)
    intent = str(uuid4())
    root = workflow.projects.base_root(eid) / "attempts" / intent
    root.mkdir(parents=True)
    add_saved_job(store, eid, intent, root, state="failed")
    with pytest.raises(ValueError, match="successful Slurm job"):
        register_result(workflow, store.experiment(eid), action, request(intent))
    with store.tx() as db:
        db.execute("UPDATE jobs SET state='complete' WHERE intent_id=?", (intent,))
    with store.tx() as db:
        db.execute(
            "UPDATE jobs SET execution_manifest=? WHERE intent_id=?",
            (json.dumps({"executor": "agent"}), intent),
        )
    with pytest.raises(ValueError, match="successful Slurm job"):
        register_result(workflow, store.experiment(eid), action, request(intent))
    with store.tx() as db:
        db.execute("UPDATE jobs SET execution_manifest=? WHERE intent_id=?", ("{}", intent))
    secret = tmp_path / "secret.json"
    secret.write_text(json.dumps(RESULT))
    (root / "results.json").symlink_to(secret)
    with pytest.raises(ValueError, match="regular file"):
        register_result(workflow, store.experiment(eid), action, request(intent))


def test_register_result_requires_saved_attempt_under_experiment_root(runtime, tmp_path):
    store, _, _, workflow = runtime
    eid = saved_contract4(store)
    action = active_turn(store, eid)
    intent = str(uuid4())
    outside = tmp_path / "outside"
    outside.mkdir()
    add_saved_job(store, eid, intent, outside)
    (outside / "results.json").write_text(json.dumps(RESULT))
    with pytest.raises(ValueError, match="contained under"):
        register_result(workflow, store.experiment(eid), action, request(intent))
