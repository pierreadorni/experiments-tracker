import pytest
from pydantic import ValidationError

from experimenttracker.research.session import first_turn
from experimenttracker.title import TitleSummary, luna_title_model, title_prompt


def test_luna_selection_requires_advertised_low_effort():
    catalog = [
        {"id": "gpt-6.1-sol", "name": "GPT-6.1 Sol", "reasoning_efforts": ["low", "medium"]},
        {"id": "gpt-6-luna", "name": "GPT-6 Luna", "reasoning_efforts": ["low", "medium"]},
    ]
    assert luna_title_model(catalog) == ("gpt-6-luna", "low")
    assert luna_title_model(catalog[:1]) is None
    assert luna_title_model([{**catalog[1], "reasoning_efforts": ["medium"]}]) is None


def test_title_output_is_bounded_and_prompt_includes_brief():
    assert TitleSummary.model_validate({"title": "DINOv3 decoder baseline on SpaceNet 7"}).title
    with pytest.raises(ValidationError):
        TitleSummary.model_validate({"title": "A" * 101})
    prompt = title_prompt("Frozen DINOv3 at scale 1", "anyup-rs", "Segmentation baseline")
    assert "Frozen DINOv3 at scale 1" in prompt
    assert "avoid adding claims about results" in prompt


@pytest.mark.parametrize(
    ("request_owner", "edit_title"), [(None, False), ("research", False), ("title", False), (None, True)]
)
async def test_an_untitled_contract_four_experiment_gets_one_small_title_turn(
    runtime, config, tmp_path, monkeypatch, request_owner, edit_title
):
    import asyncio
    from types import SimpleNamespace
    from uuid import uuid4

    from test_commands import run, services
    from test_research_create import create, git_project

    store, _, agent, workflow = runtime
    git_project(config, store, tmp_path, research_loop=True)
    _, _, receipt = create(store)
    eid = receipt["experiment_id"]
    assert store.rows("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,)) == []

    monkeypatch.setattr(
        agent,
        "models",
        [{"id": "gpt-6-luna", "name": "GPT-6 Luna", "reasoning_efforts": ["low"]}],
    )
    calls = []

    async def begin(thread_id, prompt, schema, **kwargs):
        calls.append(prompt)
        return "title-turn"

    async def finish(thread_id, turn_id):
        return {"schema_version": 1, "title": "Project method study"}

    monkeypatch.setattr(agent, "begin", begin)
    monkeypatch.setattr(agent, "finish", finish)
    monkeypatch.setattr("experimenttracker.workflow.blocked", lambda store: False)
    monkeypatch.setattr(
        workflow.research.service, "quota", lambda gate, eid=None: SimpleNamespace(admitted=True)
    )
    monkeypatch.setattr(workflow.research.service, "authority", lambda *args: None)
    monkeypatch.setattr(
        workflow,
        "refresh_quota",
        lambda _slot=None: asyncio.sleep(0, result=SimpleNamespace(admitted=True)),
    )
    workflow.agent_ready = True
    workflow.gate.decide = lambda: SimpleNamespace(admitted=True)

    await workflow.advance_title()
    queued = store.rows(
        "SELECT * FROM agent_actions WHERE experiment_id=? AND phase='summarize_title'", (eid,)
    )
    assert [row["state"] for row in queued] == ["pending"]
    assert store.rows("SELECT * FROM obligations WHERE target=?", (queued[0]["id"],))
    workflow.agent_pool.admit()
    assert workflow.agent_task is not None
    pending = None
    if request_owner is not None:
        action = first_turn(store, eid) if request_owner == "research" else queued[0]
        pending = {"id": "owner-request", "action_id": action["id"], "expired": False}
        if request_owner == "title":
            pending["rpc_id"] = 7
        store.update(eid, pending_request=pending)
    await workflow.agent_task
    workflow.agent_task = None
    if edit_title:
        request_id = str(uuid4())
        command = {
            "request_id": request_id,
            "kind": "edit_title",
            "target": f"experiment:{eid}",
            "payload": {"request_id": request_id, "title": "  Owner’s corrected title  "},
        }
        svc = services(store, workflow)
        first = run(svc, command)
        assert first["state"] == "applied"
        assert run(svc, command)["result"] == first["result"]
    await workflow.advance_title()

    assert len(calls) == 1 and "Measure a project-specific method." in calls[0]
    saved = store.one("SELECT state FROM agent_actions WHERE id=?", (queued[0]["id"],))
    assert saved["state"] == "completed"
    experiment = store.experiment(eid)
    assert experiment["title"] == ("Owner’s corrected title" if edit_title else "Project method study")
    if request_owner == "title":
        pending["expired"] = True
    assert experiment["pending_request"] == pending


async def test_no_title_turn_is_queued_without_a_small_model(runtime, config, tmp_path):
    from test_research_create import create, git_project

    store, _, _, workflow = runtime
    git_project(config, store, tmp_path, research_loop=True)
    _, _, receipt = create(store)
    await workflow.advance_title()
    assert store.rows("SELECT * FROM agent_actions WHERE experiment_id=?", (receipt["experiment_id"],)) == []
