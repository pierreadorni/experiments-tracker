"""Contract-4 checkpoint and working-report writes through the existing MCP bridge."""

import json
from uuid import uuid4

import pytest
from test_research_create import create, git_project
from test_research_tools import saved_contract4

from experimenttracker import controller
from experimenttracker.research.checkpoints import EVENT_KIND, SaveCheckpoint
from experimenttracker.research.report_tool import UpdateReport
from experimenttracker.research.result_tool import register_result

BRIEF = "Compare two toy optimizers under one fixed compute budget."


def add_research_turn(store, eid, *, thread="thread-1"):
    identity = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,input,"
            "created_at,updated_at) VALUES(?,?,'research',1,'running',?,'prompt',0,0)",
            (identity, eid, thread),
        )
        db.execute("UPDATE experiments SET thread_id=? WHERE id=?", (thread, eid))
    return store.one("SELECT * FROM agent_actions WHERE id=?", (identity,))


def document():
    return {
        "title": "A readable result",
        "standfirst": "A bounded fake research comparison.",
        "blocks": [
            {"kind": "paragraph", "text": "Question and interpretation, before the evidence."},
            {
                "kind": "table",
                "caption": "Held-out error.",
                "columns": ["Model", "MSE"],
                "rows": [
                    [{"text": "Linear"}, {"metric": {"name": "mse", "variant": "linear"}, "decimals": 4}]
                ],
            },
        ],
    }


def enter_maintenance(store):
    commit, request_id = "a" * 40, str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO releases(commit_id,summary,state,receipt,error,updated_at) VALUES(?,?,?,?,?,0)",
            (commit, "Synthetic release", "ready", None, None),
        )
        db.execute(
            "INSERT INTO maintenance(request_id,release_id,state,created_at,updated_at) "
            "VALUES(?,?,'restarting',0,0)",
            (request_id, commit),
        )


def test_research_write_schemas_are_advertised_exactly():
    served = {item["name"]: item["inputSchema"] for item in controller.served_tools(research=True)}
    assert served["save_checkpoint"] == SaveCheckpoint.model_json_schema()
    assert served["update_report"] == UpdateReport.model_json_schema()

    checkpoint = served["save_checkpoint"]
    assert checkpoint["additionalProperties"] is False
    assert checkpoint["required"] == ["request_id", "markdown"]
    assert checkpoint["properties"]["request_id"]["format"] == "uuid"
    assert checkpoint["properties"]["markdown"]["type"] == "string"

    report = served["update_report"]
    assert report["additionalProperties"] is False
    assert report["required"] == ["request_id", "document"]
    assert report["properties"]["request_id"]["format"] == "uuid"
    assert "ReportDocument" in report["$defs"]


async def test_save_checkpoint_returns_only_receipt_metadata(runtime):
    store, scheduler, agent, workflow = runtime
    eid = saved_contract4(store)
    action = add_research_turn(store, eid)
    request_id = uuid4()
    payload = {"request_id": str(request_id), "markdown": "# notebook\nfirst"}

    receipt = await workflow.controller.call(eid, "save_checkpoint", payload)

    assert receipt == {
        "request_id": str(request_id),
        "experiment_id": eid,
        "action_id": action["id"],
        "version": 1,
        "bytes": len(b"# notebook\nfirst"),
    }
    assert "markdown" not in receipt
    event = store.one("SELECT payload FROM events WHERE experiment_id=? AND kind=?", (eid, EVENT_KIND))
    assert json.loads(event["payload"])["markdown"] == "# notebook\nfirst"
    assert await workflow.controller.call(eid, "save_checkpoint", payload) == receipt
    assert len(store.rows("SELECT * FROM events WHERE experiment_id=? AND kind=?", (eid, EVENT_KIND))) == 1
    assert store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,)) == []
    assert store.rows("SELECT * FROM artifacts WHERE experiment_id=?", (eid,)) == []
    assert scheduler.submissions == 0 and agent.calls == 0


async def test_update_report_queues_one_working_version_without_markdown_or_jobs(runtime):
    store, scheduler, agent, workflow = runtime
    eid = saved_contract4(store)
    add_research_turn(store, eid)
    request_id = uuid4()
    payload = {"request_id": str(request_id), "document": document()}

    receipt = await workflow.controller.call(eid, "update_report", payload)

    assert receipt == {
        "request_id": str(request_id),
        "report_version": 1,
        "state": "queued",
    }
    saved = store.rows("SELECT version,state,request_id FROM report_versions WHERE experiment_id=?", (eid,))
    assert saved == [{"version": 1, "state": "queued", "request_id": str(request_id)}]
    assert await workflow.controller.call(eid, "update_report", payload) == receipt
    assert len(store.rows("SELECT * FROM report_versions WHERE experiment_id=?", (eid,))) == 1
    assert store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,)) == []
    assert scheduler.submissions == 0 and agent.calls == 0


@pytest.mark.parametrize(
    "name,payload",
    [
        ("save_checkpoint", {"request_id": str(uuid4()), "markdown": "# notebook"}),
        ("update_report", {"request_id": str(uuid4()), "document": document()}),
    ],
)
async def test_research_writes_refuse_during_maintenance(runtime, name, payload):
    store, _, _, workflow = runtime
    eid = saved_contract4(store)
    add_research_turn(store, eid)
    enter_maintenance(store)

    with pytest.raises(ValueError, match="maintenance forbids research writes"):
        await workflow.controller.call(eid, name, payload)

    if name == "save_checkpoint":
        assert store.rows("SELECT * FROM events WHERE experiment_id=? AND kind=?", (eid, EVENT_KIND)) == []
    else:
        assert store.rows("SELECT * FROM report_versions WHERE experiment_id=?", (eid,)) == []


@pytest.mark.parametrize(
    "phase,state,pause_agent,contract,message",
    [
        ("plan", "running", False, 4, "active research turn"),
        ("research", "pending", False, 4, "active owned session"),
        ("research", "running", True, 4, "live research experiment"),
        ("research", "running", False, 3, "execution contract 4|legacy execution is archived"),
    ],
)
async def test_save_checkpoint_keeps_the_common_live_v4_gate(
    runtime, phase, state, pause_agent, contract, message
):
    store, _, _, workflow = runtime
    eid = saved_contract4(store, pause_agent=pause_agent)
    if contract != 4:
        with store.tx() as db:
            value = json.loads(
                db.execute("SELECT contract FROM experiments WHERE id=?", (eid,)).fetchone()[0]
            )
            value["execution_contract"] = contract
            db.execute("UPDATE experiments SET contract=? WHERE id=?", (json.dumps(value), eid))
    add_research_turn(store, eid, thread="thread-1")
    with store.tx() as db:
        db.execute("UPDATE agent_actions SET phase=?,state=? WHERE experiment_id=?", (phase, state, eid))

    with pytest.raises(ValueError, match=message):
        await workflow.controller.call(
            eid, "save_checkpoint", {"request_id": str(uuid4()), "markdown": "# notebook"}
        )
    assert store.rows("SELECT * FROM events WHERE experiment_id=? AND kind=?", (eid, EVENT_KIND)) == []


async def test_update_report_rejects_tampered_result_receipt_scope(runtime, config, tmp_path):
    store, _, _, workflow = runtime
    git_project(config, store, tmp_path, research_loop=True)
    _, _, created = create(store)
    eid = created["experiment_id"]
    add_research_turn(store, eid)
    intent = str(uuid4())
    root = workflow.projects.base_root(eid) / "attempts" / intent
    root.mkdir(parents=True)
    with store.tx() as db:
        db.execute(
            "INSERT INTO stages(experiment_id,id,state,attempt,updated_at) VALUES(?,'evaluate','complete',1,0)",
            (eid,),
        )
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,job_id,script_hash,resources,"
            "snapshot_hash,created_at,execution_root,execution_manifest,ended_at) "
            "VALUES(?,?,'evaluate',1,'complete','synthetic-job','script','{}','snapshot',0,?,?,1)",
            (intent, eid, str(root), json.dumps({"executor": "slurm"})),
        )
    (root / "results.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "protocol": "synthetic test",
                "metrics": [{"name": "mse", "variant": "linear", "value": 1.0, "unit": ""}],
            }
        )
    )
    action = store.one("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,))
    register_result(
        workflow,
        store.experiment(eid),
        action,
        {
            "request_id": str(uuid4()),
            "intent_id": intent,
            "path": "results.json",
        },
    )
    event = store.one(
        "SELECT id,payload FROM events WHERE experiment_id=? AND kind='research_result_registered'", (eid,)
    )
    payload = json.loads(event["payload"])
    payload["experiment_id"] = str(uuid4())
    with store.tx() as db:
        db.execute("UPDATE events SET payload=? WHERE id=?", (json.dumps(payload), event["id"]))
    await workflow.controller.call(
        eid,
        "update_report",
        {
            "request_id": str(uuid4()),
            "document": document(),
        },
    )

    await workflow.advance_reports()

    assert store.one("SELECT state FROM report_versions WHERE experiment_id=?", (eid,))["state"] == "failed"
    assert store.rows("SELECT * FROM stages WHERE experiment_id=?", (eid,))
    assert store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))


async def test_update_report_reaches_ready_through_the_runner_without_stage_rows(runtime, config, tmp_path):
    store, scheduler, agent, workflow = runtime
    git_project(config, store, tmp_path, research_loop=True)
    _, _, receipt = create(store)
    eid = receipt["experiment_id"]
    assert store.rows("SELECT * FROM stages WHERE experiment_id=?", (eid,)) == []
    add_research_turn(store, eid)

    intent = str(uuid4())
    root = workflow.projects.base_root(eid) / "attempts" / intent
    root.mkdir(parents=True)
    with store.tx() as db:
        db.execute(
            "INSERT INTO stages(experiment_id,id,state,attempt,updated_at) VALUES(?,'evaluate','complete',1,0)",
            (eid,),
        )
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,job_id,script_hash,resources,"
            "snapshot_hash,created_at,execution_root,execution_manifest,ended_at) "
            "VALUES(?,?,'evaluate',1,'complete','synthetic-job','script','{}','snapshot',0,?,?,1)",
            (intent, eid, str(root), json.dumps({"executor": "slurm"})),
        )
    (root / "results.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "protocol": "synthetic test",
                "metrics": [{"name": "mse", "variant": "linear", "value": 1.0, "unit": ""}],
            }
        )
    )
    result_receipt = register_result(
        workflow,
        store.experiment(eid),
        store.one("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,)),
        {"request_id": str(uuid4()), "intent_id": intent, "path": "results.json"},
    )

    queued = await workflow.controller.call(
        eid,
        "update_report",
        {"request_id": str(uuid4()), "document": document()},
    )
    assert queued["report_version"] == 1 and queued["state"] == "queued"

    await workflow.advance_reports()

    saved = store.one("SELECT version,state,error FROM report_versions WHERE experiment_id=?", (eid,))
    assert saved == {"version": 1, "state": "ready", "error": None}, store.one(
        "SELECT error FROM report_versions WHERE experiment_id=?", (eid,)
    )
    names = {row["name"] for row in store.rows("SELECT name FROM artifacts WHERE experiment_id=?", (eid,))}
    assert {"report.html", "report-v1.json", result_receipt["name"]} <= names
    assert result_receipt["experiment_id"] == eid
    assert store.rows(
        "SELECT * FROM events WHERE experiment_id=? AND kind='research_result_registered'", (eid,)
    )
    assert store.rows("SELECT * FROM stages WHERE experiment_id=?", (eid,))
    assert store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
    assert scheduler.submissions == 0 and agent.calls == 0
