from uuid import uuid4

import pytest
from legacy_fixture import load_legacy_completed
from test_reporting import document

from experimenttracker.maintenance import blocked


def execution_facts(store, eid):
    exp = store.experiment(eid)
    return (
        exp["phase"],
        exp["hold_reason"],
        store.rows("SELECT * FROM jobs WHERE experiment_id=? ORDER BY intent_id", (eid,)),
        store.rows("SELECT * FROM agent_actions WHERE experiment_id=? ORDER BY id", (eid,)),
    )


@pytest.mark.asyncio
async def test_archived_nonterminal_legacy_report_uses_saved_evidence(runtime):
    store, scheduler, agent, workflow = runtime
    eid = load_legacy_completed(store, workflow)
    store.command(uuid4(), "present", {"document": document()}, eid=eid)
    with store.tx() as db:
        db.execute("UPDATE experiments SET phase='training' WHERE id=?", (eid,))
    store.archive_legacy_experiments()
    before = execution_facts(store, eid)

    assert before[:2] == ("training", "legacy_contract_archived")
    assert not blocked(store)
    await workflow.tick(force_observe=True)

    version = store.one("SELECT state FROM report_versions WHERE experiment_id=?", (eid,))
    assert version["state"] == "ready"
    assert execution_facts(store, eid) == before
    assert scheduler.submissions == 0 and agent.calls == 0


@pytest.mark.asyncio
async def test_archived_nonterminal_legacy_invalid_metric_fails_only_report(runtime):
    store, scheduler, agent, workflow = runtime
    eid = load_legacy_completed(store, workflow)
    doc = document()
    doc["blocks"][1]["rows"][0][1]["metric"]["variant"] = "invented"
    store.command(uuid4(), "present", {"document": doc}, eid=eid)
    with store.tx() as db:
        db.execute("UPDATE experiments SET phase='training' WHERE id=?", (eid,))
    store.archive_legacy_experiments()
    before = execution_facts(store, eid)

    assert not blocked(store)
    await workflow.tick(force_observe=True)

    report = store.one("SELECT state,error FROM report_versions WHERE experiment_id=?", (eid,))
    assert report == {"state": "failed", "error": "report refers to missing or non-finite metric"}
    assert execution_facts(store, eid) == before
    assert scheduler.submissions == 0 and agent.calls == 0
