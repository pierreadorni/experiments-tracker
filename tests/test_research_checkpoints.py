"""C7 checkpoint tool: one private versioned notebook per saved research action (T4.2)."""

import json
from uuid import UUID, uuid4

import pytest
from helpers import seed_saved_contract4
from pydantic import ValidationError

from experimenttracker.codex_client import NeedsAttention
from experimenttracker.research.checkpoints import (
    EVENT_KIND,
    SaveCheckpoint,
    latest_checkpoint,
    save_checkpoint,
)
from experimenttracker.research.session import first_turn
from experimenttracker.store import Conflict, Store, encode, public_projection

BRIEF = "Compare two toy optimizers under one fixed compute budget."


def contract_four_experiment(store):
    return seed_saved_contract4(store, BRIEF)


def setup(runtime):
    """A live contract 4 experiment with one dispatching research turn."""
    store = runtime[0]
    eid = contract_four_experiment(store)
    return store, eid, owned(store, eid, first_turn(store, eid))


def owned(store, eid, action, *, thread="thread-1"):
    """Make one saved action the dispatching turn that holds the experiment thread."""
    with store.tx() as db:
        db.execute(
            "UPDATE agent_actions SET state='dispatching',thread_id=? WHERE id=?",
            (thread, action["id"]),
        )
        db.execute("UPDATE experiments SET thread_id=? WHERE id=?", (thread, eid))
    return saved_action(store, action["id"])


def saved_action(store, action_id):
    return store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,))


def checkpoint_events(store, eid):
    return store.rows(
        "SELECT experiment_id,payload FROM events WHERE experiment_id=? AND kind=? ORDER BY seq",
        (eid, EVENT_KIND),
    )


def revision(store, eid):
    return store.experiment(eid)["revision"]


def test_request_schema_requires_a_uuid_and_bounded_nonblank_markdown():
    request = SaveCheckpoint(request_id=uuid4(), markdown="# notebook")
    assert UUID(str(request.request_id))

    with pytest.raises(ValidationError):
        SaveCheckpoint(request_id="not-a-uuid", markdown="# notebook")
    for blank in ("", "   \n\t "):
        with pytest.raises(ValidationError):
            SaveCheckpoint(request_id=uuid4(), markdown=blank)
    with pytest.raises(ValidationError):
        SaveCheckpoint(request_id=uuid4(), markdown="é" * 2001)
    assert SaveCheckpoint(request_id=uuid4(), markdown="a" * 4000).markdown == "a" * 4000


def test_save_versions_the_checkpoint_in_one_saved_event(runtime):
    store, eid, action = setup(runtime)
    before = revision(store, eid)
    first_text = "# notebook\nfirst"
    second_text = "# notebook\nsecond"

    first = save_checkpoint(store, action, SaveCheckpoint(request_id=uuid4(), markdown=first_text))
    second = save_checkpoint(store, action, SaveCheckpoint(request_id=uuid4(), markdown=second_text))

    assert (first["version"], second["version"]) == (1, 2)
    assert first["experiment_id"] == eid and first["action_id"] == action["id"]
    assert first["bytes"] == len(first_text.encode())
    rows = [json.loads(row["payload"]) for row in checkpoint_events(store, eid)]
    assert [row["version"] for row in rows] == [1, 2]
    assert rows[-1]["markdown"] == second_text
    assert revision(store, eid) == before + 2
    assert latest_checkpoint(store, eid) == second
    assert latest_checkpoint(store, uuid4()) is None


def test_identical_replay_returns_the_saved_checkpoint_without_a_second_event(runtime):
    store, eid, action = setup(runtime)
    request = SaveCheckpoint(request_id=uuid4(), markdown="# notebook\nsame bytes")

    saved = save_checkpoint(store, action, request)
    before = revision(store, eid)

    assert save_checkpoint(store, action, request) == saved
    assert revision(store, eid) == before
    assert len(checkpoint_events(store, eid)) == 1
    assert latest_checkpoint(store, eid)["version"] == 1


def test_reusing_a_request_id_with_changed_bytes_or_scope_is_refused(runtime):
    store, eid, action = setup(runtime)
    request = SaveCheckpoint(request_id=uuid4(), markdown="# notebook original")
    save_checkpoint(store, action, request)

    with pytest.raises(Conflict):
        save_checkpoint(
            store, action, SaveCheckpoint(request_id=request.request_id, markdown="# notebook changed")
        )
    assert len(checkpoint_events(store, eid)) == 1

    other_eid = contract_four_experiment(store)
    other_action = owned(store, other_eid, first_turn(store, other_eid))
    with pytest.raises(Conflict):
        save_checkpoint(store, other_action, request)
    assert checkpoint_events(store, other_eid) == []


@pytest.mark.parametrize(
    "change",
    [
        ("agent_actions", "state", "pending"),
        ("agent_actions", "state", "uncertain"),
        ("agent_actions", "state", "completed"),
        ("agent_actions", "state", "superseded"),
        ("agent_actions", "phase", "discuss:1"),
        ("experiments", "phase", "complete"),
        ("experiments", "deleted_at", 1.0),
        ("contract", "execution_contract", 3),
    ],
)
def test_only_a_live_contract_four_research_action_can_save(runtime, change):
    store, eid, action = setup(runtime)
    table, column, value = change
    if table == "contract":
        contract = store.experiment(eid)["contract"]
        contract["execution_contract"] = value
        with store.tx() as db:
            db.execute("UPDATE experiments SET contract=? WHERE id=?", (json.dumps(contract), eid))
    else:
        identity = eid if table == "experiments" else action["id"]
        with store.tx() as db:
            db.execute(f"UPDATE {table} SET {column}=? WHERE id=?", (value, identity))

    with pytest.raises(NeedsAttention):
        save_checkpoint(
            store, saved_action(store, action["id"]), SaveCheckpoint(request_id=uuid4(), markdown="# nope")
        )
    assert checkpoint_events(store, eid) == []


def test_paused_or_foreign_thread_ownership_refuses_write_and_replay(runtime):
    store, eid, action = setup(runtime)
    request = SaveCheckpoint(request_id=uuid4(), markdown="# notebook owned")
    save_checkpoint(store, action, request)

    with store.tx() as db:
        db.execute("UPDATE experiments SET pause_agent=1 WHERE id=?", (eid,))
    with pytest.raises(NeedsAttention):
        save_checkpoint(store, action, request)
    with store.tx() as db:
        db.execute("UPDATE experiments SET pause_agent=0 WHERE id=?", (eid,))

    with store.tx() as db:
        db.execute("UPDATE agent_actions SET thread_id='thread-foreign' WHERE id=?", (action["id"],))
    with pytest.raises(NeedsAttention):
        save_checkpoint(
            store, saved_action(store, action["id"]), SaveCheckpoint(request_id=uuid4(), markdown="# x")
        )
    with store.tx() as db:
        db.execute("UPDATE agent_actions SET thread_id=NULL WHERE id=?", (action["id"],))
    with pytest.raises(NeedsAttention):
        save_checkpoint(
            store, saved_action(store, action["id"]), SaveCheckpoint(request_id=uuid4(), markdown="# x")
        )
    assert len(checkpoint_events(store, eid)) == 1


def test_a_changed_or_missing_action_saves_nothing(runtime):
    store, eid, action = setup(runtime)
    stale = saved_action(store, action["id"])
    with store.tx() as db:
        db.execute("UPDATE agent_actions SET turn_id='turn-9' WHERE id=?", (action["id"],))
    with pytest.raises(NeedsAttention):
        save_checkpoint(store, stale, SaveCheckpoint(request_id=uuid4(), markdown="# stale"))
    assert checkpoint_events(store, eid) == []

    settled = saved_action(store, action["id"])
    with store.tx() as db:
        db.execute("DELETE FROM agent_actions WHERE id=?", (action["id"],))
    with pytest.raises(NeedsAttention):
        save_checkpoint(store, settled, SaveCheckpoint(request_id=uuid4(), markdown="# gone"))
    assert checkpoint_events(store, eid) == []


def test_checkpoint_markdown_stays_out_of_public_projection_and_publication(runtime):
    store, eid, action = setup(runtime)
    markdown = "# notebook private observation 8f3c"
    request = SaveCheckpoint(request_id=uuid4(), markdown=markdown)
    save_checkpoint(store, action, request)

    # The exact bytes stay in the saved event on the local disk for the next turn.
    assert markdown in checkpoint_events(store, eid)[0]["payload"]

    # Every projected view, including the local API's, keeps only the reference.
    assert markdown not in encode(store.projection(eid))
    published = public_projection(store.projection(eid))
    event = next(e for e in published["events"] if e["kind"] == EVENT_KIND)
    payload = json.loads(event["payload"])
    assert "markdown" not in payload
    assert payload["request_id"] == str(request.request_id)
    assert payload["version"] == 1
    assert markdown not in encode(published)

    reopened = Store(store.path)
    try:
        assert public_projection(reopened.projection(eid)) == published
    finally:
        reopened.close()


def test_a_saved_prompt_carrying_the_checkpoint_never_projects_the_text(runtime):
    store, eid, action = setup(runtime)
    markdown = "# notebook private prompt seed 1a2b"
    save_checkpoint(store, action, SaveCheckpoint(request_id=uuid4(), markdown=markdown))
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,created_at,"
            "updated_at) VALUES(?,?,'research',2,'pending',?,0,0)",
            (str(uuid4()), eid, f"LATEST CHECKPOINT\n\n{markdown}"),
        )

    projected = store.projection(eid)
    assert markdown not in encode(projected["agent_actions"])
    assert all("input" not in row for row in projected["agent_actions"])
    assert markdown not in encode(public_projection(projected))
