"""Bounded contract-4 cluster_info routing through the existing controller bridge."""

import json
from uuid import uuid4

import pytest
from helpers import seed_saved_contract3, seed_saved_contract4
from pydantic import ValidationError
from test_research_dispatch import ScriptedAgent, completion, research_setup, run_pass, scripted

from experimenttracker import controller
from experimenttracker.research.dispatch import ResearchDispatch
from experimenttracker.research.tools import ClusterInfo, cluster_info
from experimenttracker.store import encode

BRIEF = "Compare two toy optimizers under one fixed compute budget."


def saved_contract4(store, *, phase="training", pause_agent=False):
    eid = seed_saved_contract4(store, BRIEF)
    store.update(eid, phase=phase, pause_agent=pause_agent)
    return eid


def add_action(store, eid, *, phase="research", state="running"):
    identity = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,"
            "created_at,updated_at) VALUES(?,?,?,1,?,'prompt',0,0)",
            (identity, eid, phase, state),
        )
    return identity


def test_cluster_info_input_is_strict_and_requires_a_uuid_request_id():
    request_id = uuid4()
    assert ClusterInfo.model_validate({"request_id": str(request_id)}).request_id == request_id
    for payload in ({}, {"request_id": "not-a-uuid"}, {"request_id": str(request_id), "extra": 1}):
        with pytest.raises(ValidationError):
            ClusterInfo.model_validate(payload)


def test_v4_selects_only_research_tools_and_keeps_contract_three_listing(runtime):
    store, _, _, workflow = runtime
    eid = saved_contract4(store)
    workflow.controller.server = object()
    try:
        config = workflow.controller.session_config(eid)["mcp_servers.experimenttracker"]
    finally:
        workflow.controller.server = None
    assert config["args"][config["args"].index("--experiment") + 1] == eid
    assert "--research" in config["args"]

    contract3 = seed_saved_contract3(store, BRIEF)
    workflow.controller.server = object()
    try:
        legacy = workflow.controller.session_config(contract3)["mcp_servers.experimenttracker"]
    finally:
        workflow.controller.server = None
    assert "--research" not in legacy["args"]
    assert {item["name"] for item in controller.served_tools(research=True)} == {
        "ask_owner",
        "cancel_job",
        "cluster_info",
        "job_logs",
        "job_status",
        "register_figure",
        "register_result",
        "request_integration",
        "save_checkpoint",
        "submit_job",
        "update_report",
    }
    assert "cluster_info" not in {item["name"] for item in controller.served_tools()}
    assert "submit_stage" in {item["name"] for item in controller.served_tools()}


async def test_cluster_info_returns_bounded_cached_caps_without_live_effects(runtime, monkeypatch):
    store, scheduler, agent, workflow = runtime
    eid = saved_contract4(store)
    add_action(store, eid)
    workflow.cluster_snapshot = {
        "observed_at": 123.0,
        "source": "live Slurm",
        "nodes": "node1|gpu:2",
        "partitions": "short* up",
        "errors": {"nodes": "e" * 2000},
    }
    before = store.rows("SELECT * FROM events ORDER BY seq")

    async def forbidden(*_args, **_kwargs):
        raise AssertionError("cluster_info must never refresh cluster discovery")

    monkeypatch.setattr(workflow, "refresh_cluster", forbidden)
    request_id = uuid4()
    result = await workflow.controller.call(eid, "cluster_info", {"request_id": str(request_id)})

    assert result["request_id"] == str(request_id) and result["cached"] is True
    assert result["capabilities"]["nodes"] == "node1|gpu:2"
    assert result["capabilities"]["errors"]["nodes"].endswith("[truncated]")
    assert len(result["capabilities"]["errors"]["nodes"]) < 2000
    assert result["owner_caps"] == workflow.config.slurm.limits.model_dump(mode="json")
    assert store.rows("SELECT * FROM events ORDER BY seq") == before
    assert scheduler.submissions == 0 and agent.calls == 0
    assert not store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))


def test_cluster_capabilities_bound_long_cached_fields(runtime):
    store, _, _, workflow = runtime
    eid = saved_contract4(store)
    workflow.cluster_snapshot = {"nodes": "x" * 20000}
    result = cluster_info(workflow, store.experiment(eid), ClusterInfo(request_id=uuid4()))
    assert len(result["capabilities"]["nodes"]) < 20000
    assert result["capabilities"]["nodes"].endswith("[truncated]")


@pytest.mark.parametrize(
    "phase,state,pause_agent,contract,message",
    [
        ("plan", "running", False, 4, "active research turn"),
        ("research", "pending", False, 4, "active owned session"),
        ("research", "running", True, 4, "live research experiment"),
        ("research", "running", False, 3, "execution contract 4"),
    ],
)
async def test_cluster_info_refuses_anything_but_the_live_research_action(
    runtime, phase, state, pause_agent, contract, message
):
    store, _, _, workflow = runtime
    eid = saved_contract4(store, pause_agent=pause_agent)
    if contract != 4:
        with store.tx() as db:
            value = json.loads(
                db.execute("SELECT contract FROM experiments WHERE id=?", (eid,)).fetchone()["contract"]
            )
            value["execution_contract"] = contract
            db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(value), eid))
    add_action(store, eid, phase=phase, state=state)
    with pytest.raises(ValueError, match=message):
        await workflow.controller.call(eid, "cluster_info", {"request_id": str(uuid4())})


async def test_research_dispatch_wires_the_v4_runtime_config(runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    _, eid, _, action = research_setup(runtime, tmp_path, monkeypatch)

    class RecordingAgent(ScriptedAgent):
        def __init__(self):
            super().__init__(result={"status": "continue", "summary": "measured", "next": "compare"})
            self.session_options = []

        async def session(self, thread_id, workspace, *, writable=False, **kwargs):
            self.session_options.append(kwargs)
            return await super().session(thread_id, workspace, writable=writable, **kwargs)

    agent = scripted(workflow, RecordingAgent())
    workflow.controller.server = object()
    try:
        await run_pass(workflow, ResearchDispatch(workflow, completion([])))
    finally:
        workflow.controller.server = None

    assert len(agent.session_options) == 1
    server = agent.session_options[0]["runtime_config"]["mcp_servers.experimenttracker"]
    assert "--research" in server["args"]
    assert store.one("SELECT state FROM agent_actions WHERE id=?", (action["id"],))["state"] == "running"
