"""A completed legacy discussion creates a current-contract follow-up."""

from pathlib import Path

import pytest
from legacy_fixture import load_legacy_completed
from test_conversation import send

from experimenttracker import creation_policy
from experimenttracker.config import ProjectConfig
from experimenttracker.store import Conflict, encode
from experimenttracker.workspaces import initialize_project


def saved_evidence(store, eid):
    """Read the parent's saved artifact bytes, which no discussion may rewrite."""
    paths = [row["path"] for row in store.rows("SELECT * FROM artifacts WHERE experiment_id=?", (eid,))]
    return {path: Path(path).read_bytes() for path in paths}


def followup_brief(agent):
    """Keep the deterministic demo reply valid and declare the follow-up it creates."""
    begin = agent.begin

    async def create(thread, prompt, schema, *, workspace=None, effort="low", project_workspace=None):
        turn = await begin(
            thread,
            prompt,
            schema,
            workspace=workspace,
            effort=effort,
            **({"project_workspace": project_workspace} if project_workspace else {}),
        )
        agent.results[(thread, turn)]["new_experiment"] = {
            "brief": "Repeat the fixed linear and quadratic protocol with new seeds."
        }
        return turn

    agent.begin = create


async def test_candidate_followup_reference_refusal_is_durable_and_replay_does_not_create(
    runtime, tmp_path, monkeypatch
):
    store, scheduler, agent, workflow = runtime
    eid = load_legacy_completed(store, workflow)
    checkout = tmp_path / "research"
    checkout.mkdir()
    initialize_project(checkout)
    project = ProjectConfig(id="anyup-rs", name="anyup-rs", checkout=checkout, writable=True)
    workflow.config.projects.append(project)
    store.projects[project.id] = project
    contract = store.experiment(eid)["contract"]
    contract["project"] = {"id": project.id, "name": project.name}
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
    monkeypatch.setattr(creation_policy, "MODE", "candidate")
    followup_brief(agent)
    identity = send(store, eid, "Start a follow-up with new seeds")
    await workflow.tick(force_observe=True)
    await workflow.agent_task

    message = store.one("SELECT * FROM discussions WHERE id=?", (identity,))
    action = store.one("SELECT * FROM agent_actions WHERE id=?", (message["action_id"],))
    assert action["state"] == "completed"
    await workflow.conversation.advance()

    failed = store.one("SELECT * FROM discussions WHERE id=?", (identity,))
    assert failed["state"] == "failed"
    assert "does not copy a reference experiment snapshot" in failed["error"]
    assert failed["followup_experiment_id"] is None
    assert store.rows("SELECT * FROM commands WHERE origin='discussion'") == []
    assert len(store.rows("SELECT * FROM experiments")) == 1

    # Replaying the exact saved model result cannot turn the refusal into a create.
    with pytest.raises(Conflict, match="does not copy a reference experiment snapshot"):
        workflow.conversation.complete(message, action)
    assert store.rows("SELECT * FROM commands WHERE origin='discussion'") == []
    assert len(store.rows("SELECT * FROM experiments")) == 1
    assert scheduler.submissions == 0 and agent.calls == 1
    await workflow.close()


async def test_completed_legacy_discussion_creates_current_contract_followup(runtime, tmp_path):
    store, scheduler, agent, workflow = runtime
    eid = load_legacy_completed(store, workflow)
    checkout = tmp_path / "research"
    checkout.mkdir()
    project = ProjectConfig(id="anyup-rs", name="anyup-rs", checkout=checkout, writable=False)
    workflow.config.projects.append(project)
    store.projects[project.id] = project
    # A real read-only create saves this project snapshot; Store.update cannot change a contract.
    contract = store.experiment(eid)["contract"]
    contract["project"] = {"id": project.id, "name": project.name}
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
    evidence = saved_evidence(store, eid)
    followup_brief(agent)
    identity = send(store, eid, "Start the follow-up with new seeds")
    await workflow.tick(force_observe=True)
    await workflow.agent_task
    action = store.one("SELECT * FROM agent_actions WHERE phase=?", ("discuss:" + identity,))
    assert action["state"] == "completed"
    # Model and effort are saved on the row at dispatch, after the message itself was queued.
    message = store.one("SELECT * FROM discussions WHERE id=?", (identity,))
    assert message["model"] == "deterministic-demo" and message["reasoning_effort"] == "medium"
    # Complete directly, twice, so the child never advances past its create command.
    workflow.conversation.complete(message, action)
    followup = store.one("SELECT followup_experiment_id FROM discussions WHERE id=?", (identity,))[
        "followup_experiment_id"
    ]
    workflow.conversation.complete(message, action)
    row = store.one("SELECT * FROM discussions WHERE id=?", (identity,))
    assert row["state"] == "completed" and row["followup_experiment_id"] == followup
    assert followup and len(store.rows("SELECT * FROM experiments")) == 2
    assert len(store.rows("SELECT * FROM commands WHERE origin='discussion'")) == 1
    child = store.experiment(followup)
    assert child["recipe_id"] == "agent_experiment_v1"
    assert child["contract"]["execution_contract"] == 3
    assert child["reference_experiment_id"] == eid
    assert child["contract"]["reference"]["experiment_id"] == eid
    assert child["contract"]["project"] == {"id": "anyup-rs", "name": "anyup-rs"}
    assert child["contract"]["agent"] == {
        "model": message["model"],
        "reasoning_effort": message["reasoning_effort"],
    }
    assert not store.rows("SELECT * FROM experiment_workspaces")
    assert scheduler.submissions == 0 and agent.calls == 1
    assert saved_evidence(store, eid) == evidence
    await workflow.close()
