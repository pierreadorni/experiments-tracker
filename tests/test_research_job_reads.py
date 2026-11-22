"""Contract-4 job_status/job_logs stay owned, bounded reads through the controller bridge."""

import json
from uuid import uuid4

import pytest
from pydantic import ValidationError
from test_research_tools import add_action, saved_contract4

from experimenttracker import controller
from experimenttracker.research.tools import JobLogs, JobStatus


def add_job(store, eid, intent, root, *, job_id="123", state="failed", attempt=1, created_at=0):
    with store.tx() as db:
        db.execute("INSERT OR IGNORE INTO stages(experiment_id,id,updated_at) VALUES(?,'research',0)", (eid,))
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,job_id,script_hash,resources,"
            "snapshot_hash,created_at,execution_root) VALUES(?,?,'research',?,?,?,?,?,?,?,?)",
            (intent, eid, attempt, state, job_id, "s", "{}", "h", created_at, str(root)),
        )
    return intent


def write_log(root, intent, job_id, text, suffix="out"):
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"et-{intent}-{job_id}.{suffix}"
    path.write_text(text)
    return path


def request_status():
    return {"request_id": str(uuid4())}


def request_logs(intent, tail=None):
    payload = {"request_id": str(uuid4()), "intent_id": intent}
    if tail is not None:
        payload["tail"] = tail
    return payload


def test_job_read_schemas_are_strict_and_bounded():
    status = JobStatus.model_validate({"request_id": str(uuid4()), "intent_ids": [str(uuid4())]})
    assert status.summary_only is False and len(status.intent_ids) == 1
    with pytest.raises(ValidationError):
        JobStatus.model_validate({"request_id": str(uuid4()), "intent_ids": [str(uuid4())] * 33})
    with pytest.raises(ValidationError):
        JobStatus.model_validate({"intent_ids": []})
    with pytest.raises(ValidationError):
        JobStatus.model_validate({"request_id": str(uuid4()), "summary_only": "yes"})
    with pytest.raises(ValidationError):
        JobStatus.model_validate({"request_id": "not-a-uuid"})
    with pytest.raises(ValidationError):
        JobStatus.model_validate({"request_id": str(uuid4()), "extra": 1})

    logs = JobLogs.model_validate({"request_id": str(uuid4()), "intent_id": str(uuid4())})
    assert logs.tail == 4000
    for tail in (0, 65537):
        with pytest.raises(ValidationError):
            JobLogs.model_validate({"request_id": str(uuid4()), "intent_id": str(uuid4()), "tail": tail})
    with pytest.raises(ValidationError):
        JobLogs.model_validate({"request_id": str(uuid4()), "intent_id": str(uuid4()), "tail": "100"})
    with pytest.raises(ValidationError):
        JobLogs.model_validate({"request_id": str(uuid4())})


def test_served_v4_schemas_come_from_the_validating_models():
    served = {item["name"]: item["inputSchema"] for item in controller.served_tools(research=True)}
    status = served["job_status"]
    assert status["additionalProperties"] is False
    assert status["required"] == ["request_id"]
    assert status["properties"]["request_id"]["format"] == "uuid"
    intent_ids = status["properties"]["intent_ids"]["anyOf"][0]
    assert intent_ids["maxItems"] == 32 and intent_ids["items"]["format"] == "uuid"
    assert status["properties"]["summary_only"]["type"] == "boolean"

    logs = served["job_logs"]
    assert logs["required"] == ["request_id", "intent_id"]
    assert logs["properties"]["request_id"]["format"] == "uuid"
    assert logs["properties"]["intent_id"]["format"] == "uuid"
    assert logs["properties"]["tail"]["minimum"] == 1
    assert logs["properties"]["tail"]["maximum"] == 65536
    assert logs["properties"]["tail"]["default"] == 4000

    legacy = {item["name"]: item["inputSchema"] for item in controller.served_tools()}
    assert "request_id" not in legacy["job_status"]["properties"]


async def test_job_status_reads_only_this_experiments_saved_jobs(runtime, tmp_path):
    store, _, _, workflow = runtime
    owned = saved_contract4(store)
    foreign = saved_contract4(store)
    add_action(store, owned)
    own_intent = add_job(store, owned, str(uuid4()), tmp_path / "owned")
    foreign_intent = add_job(store, foreign, str(uuid4()), tmp_path / "foreign")

    result = await workflow.controller.call(owned, "job_status", request_status())
    assert result["request_id"] and [job["intent_id"] for job in result["jobs"]] == [own_intent]

    selected = await workflow.controller.call(
        owned, "job_status", {"request_id": str(uuid4()), "intent_ids": [own_intent]}
    )
    assert [job["intent_id"] for job in selected["jobs"]] == [own_intent]

    with pytest.raises(ValueError, match="this experiment's saved jobs"):
        await workflow.controller.call(
            owned, "job_status", {"request_id": str(uuid4()), "intent_ids": [foreign_intent]}
        )


async def test_unfiltered_job_status_returns_the_latest_32_in_stable_order(runtime, tmp_path):
    store, _, _, workflow = runtime
    eid = saved_contract4(store)
    add_action(store, eid)
    intents = [
        add_job(
            store,
            eid,
            str(uuid4()),
            tmp_path / "attempt",
            attempt=i + 1,
            created_at=i,
        )
        for i in range(40)
    ]
    payload = request_status()
    result = await workflow.controller.call(eid, "job_status", payload)
    assert [job["intent_id"] for job in result["jobs"]] == intents[8:]
    assert result["omitted"] == 8
    assert await workflow.controller.call(eid, "job_status", payload) == result

    selected = await workflow.controller.call(
        eid, "job_status", {"request_id": str(uuid4()), "intent_ids": intents[:2]}
    )
    assert [job["intent_id"] for job in selected["jobs"]] == intents[:2]
    assert "omitted" not in selected


async def test_contract4_job_status_refuses_a_non_research_turn(runtime):
    store, _, _, workflow = runtime
    eid = saved_contract4(store)
    add_action(store, eid, phase="plan")
    with pytest.raises(ValueError, match="active research turn"):
        await workflow.controller.call(eid, "job_status", request_status())


async def test_job_logs_is_bounded_contained_and_reports_availability(runtime, tmp_path):
    store, _, _, workflow = runtime
    eid = saved_contract4(store)
    add_action(store, eid)
    root = tmp_path / "attempt"
    intent = add_job(store, eid, str(uuid4()), root)
    write_log(root, intent, "123", "head" + "a" * 9000 + "TAIL")

    result = await workflow.controller.call(eid, "job_logs", request_logs(intent, tail=50))
    assert result["available"] is True and result["tail_bytes"] == 50
    assert result["logs"]["out"] == "a" * 46 + "TAIL"
    assert len(result["logs"]["out"]) == 50 and "err" not in result["logs"]
    assert result["intent_id"] == intent and result["job_id"] == "123"

    # A log path that escapes the attempt directory is unavailable, never read.
    secret = tmp_path / "secret.txt"
    secret.write_text("SECRET-CONTENT")
    (root / f"et-{intent}-123.err").symlink_to(secret)
    contained = await workflow.controller.call(eid, "job_logs", request_logs(intent))
    assert "err" not in contained["logs"] and "SECRET-CONTENT" not in json.dumps(contained)


async def test_job_logs_refuses_foreign_jobs_and_missing_local_logs(runtime, tmp_path):
    store, _, _, workflow = runtime
    eid = saved_contract4(store)
    foreign = saved_contract4(store)
    add_action(store, eid)
    foreign_intent = add_job(store, foreign, str(uuid4()), tmp_path / "foreign")
    write_log(tmp_path / "foreign", foreign_intent, "123", "FOREIGN-SECRET")
    silent_intent = add_job(store, eid, str(uuid4()), tmp_path / "missing", job_id=None)

    before = store.rows("SELECT * FROM events ORDER BY seq")
    with pytest.raises(ValueError, match="this experiment's saved job"):
        await workflow.controller.call(eid, "job_logs", request_logs(foreign_intent))
    assert store.rows("SELECT * FROM events ORDER BY seq") == before

    silent = await workflow.controller.call(eid, "job_logs", request_logs(silent_intent))
    assert silent["available"] is False and silent["logs"] == {}


async def test_repeating_a_job_read_is_the_same_read_without_side_effects(runtime, tmp_path, monkeypatch):
    store, scheduler, agent, workflow = runtime
    eid = saved_contract4(store)
    add_action(store, eid)
    intent = add_job(store, eid, str(uuid4()), tmp_path / "attempt")
    write_log(tmp_path / "attempt", intent, "123", "measured progress")
    payload = request_logs(intent)

    async def forbidden(*_args, **_kwargs):
        raise AssertionError("a job read must never dispatch or ask the scheduler")

    monkeypatch.setattr(scheduler, "submit", forbidden)
    before = store.rows("SELECT * FROM events ORDER BY seq")
    first = await workflow.controller.call(eid, "job_logs", payload)
    second = await workflow.controller.call(eid, "job_logs", payload)
    assert first == second
    assert store.rows("SELECT * FROM events ORDER BY seq") == before
    assert scheduler.submissions == 0 and agent.calls == 0


@pytest.mark.parametrize(
    "phase,pause_agent,contract,message",
    [
        ("plan", False, 4, "active research turn"),
        ("research", True, 4, "live research experiment"),
        ("research", False, 3, "execution contract 4|legacy execution is archived"),
    ],
)
async def test_job_logs_requires_the_exact_live_v4_research_turn(
    runtime, tmp_path, phase, pause_agent, contract, message
):
    store, _, _, workflow = runtime
    eid = saved_contract4(store, pause_agent=pause_agent)
    if contract != 4:
        with store.tx() as db:
            contract_value = json.loads(
                db.execute("SELECT contract FROM experiments WHERE id=?", (eid,)).fetchone()["contract"]
            )
            contract_value["execution_contract"] = contract
            db.execute("UPDATE experiments SET contract=? WHERE id=?", (json.dumps(contract_value), eid))
    add_action(store, eid, phase=phase)
    intent = add_job(store, eid, str(uuid4()), tmp_path / "attempt")
    with pytest.raises(ValueError, match=message):
        await workflow.controller.call(eid, "job_logs", request_logs(intent))
