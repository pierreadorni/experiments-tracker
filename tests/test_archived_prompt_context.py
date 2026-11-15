import pytest
from legacy_fixture import load_legacy_completed


def test_archived_execution_context_is_readable_and_prompts_are_inert(runtime):
    store, scheduler, agent, workflow = runtime
    eid = load_legacy_completed(store, workflow)
    exp = store.experiment(eid)
    saved = exp["contract"]["execution"]

    context = workflow.execution_context(exp)
    assert context["context"] == saved["context"]
    assert context["ssh_host"] == saved["ssh_host"]
    assert context["rsync_up"] == saved["rsync_up"]
    assert context["rsync_down"] == saved["rsync_down"]
    assert context["templates"]["host"] == saved["ssh_host"]
    assert "historical record" in context["guide"]
    assert "Do not prepare, submit, retry, or mutate execution" in context["guide"]

    before = {
        table: store.one(f"SELECT count(*) AS n FROM {table}")["n"]
        for table in ("experiments", "stages", "jobs", "agent_actions", "events", "outbox")
    }
    with pytest.raises(ValueError, match="archived legacy execution prompts are read-only"):
        workflow._prompt(exp, "interpret")
    after = {table: store.one(f"SELECT count(*) AS n FROM {table}")["n"] for table in before}
    assert after == before
    assert agent.calls == 0
    assert scheduler.submissions == 0
