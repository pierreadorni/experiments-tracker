"""Inert C7 ask_owner: one exact owner question and its existing answer path (T4.2)."""

import json
from uuid import uuid4

import pytest
from pydantic import ValidationError

from experimenttracker.core import action_obligations
from experimenttracker.research.checkpoints import SaveCheckpoint, save_checkpoint
from experimenttracker.research.owner_tool import AskOwnerQuestion, ask_owner
from experimenttracker.research.session import first_turn
from experimenttracker.store import Conflict, encode, public_projection

BRIEF = "Compare two toy optimizers under one fixed compute budget."
THREAD = "thread-1"
EVENT_KIND = "owner_question_asked"


def question(request_id=None, text="Which dataset should the comparison use?", options=()):
    return AskOwnerQuestion(request_id=request_id or uuid4(), question=text, options=list(options))


def make_turn(store):
    """Create a contract 4 experiment with one saved running research action."""
    eid = store.command(uuid4(), "create", {"brief": BRIEF, "recipe_id": "agent_experiment_v1"})[
        "experiment_id"
    ]
    saved = store.experiment(eid)
    saved["contract"]["execution_contract"] = 4
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(saved["contract"]), eid))
    action = first_turn(store, eid)
    assert action is not None
    with store.tx() as db:
        before = dict(db.execute("SELECT * FROM agent_actions WHERE id=?", (action["id"],)).fetchone())
        db.execute(
            "UPDATE agent_actions SET state='running',thread_id=?,turn_id='turn-1',attempts=1 WHERE id=?",
            (THREAD, action["id"]),
        )
        db.execute("UPDATE experiments SET thread_id=? WHERE id=?", (THREAD, eid))
        action_obligations.sync_action(db, action["id"], before=before)
    return eid, store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))


def pending(store, eid):
    return store.experiment(eid)["pending_request"]


def obligation(store, eid, request_id):
    return store.one("SELECT * FROM obligations WHERE id=?", (f"request:{eid}:{request_id}:0",))


def owner_events(store, eid):
    return store.rows("SELECT id,revision FROM events WHERE experiment_id=? AND kind=?", (eid, EVENT_KIND))


def answer(store, eid, request_id, value="dataset B"):
    store.command(uuid4(), "answer", {"pending_request_id": str(request_id), "answer": value}, eid=eid)


def counts(store, eid):
    return (
        len(owner_events(store, eid)),
        len(store.rows("SELECT * FROM inbox WHERE experiment_id=?", (eid,))),
        len(store.rows("SELECT * FROM obligations WHERE id LIKE 'request:%'")),
        store.experiment(eid)["revision"],
    )


def test_contract_four_projection_exposes_turn_and_checkpoint_metadata_only(runtime):
    store, _, _, _ = runtime
    eid, action = make_turn(store)
    save_checkpoint(
        store,
        action,
        SaveCheckpoint(request_id=uuid4(), markdown="# Private notebook\nsecret detail"),
    )
    with store.tx() as db:
        db.execute(
            "UPDATE agent_actions SET state='completed',result=? WHERE id=?",
            (
                encode({"status": "wait", "summary": "A safe turn summary", "next": "Check the job"}),
                action["id"],
            ),
        )

    projection = store.projection(eid)

    assert projection["research_turns"][0]["summary"] == "A safe turn summary"
    assert projection["research_turns"][0]["next"] == "Check the job"
    assert projection["research_checkpoint"]["version"] == 1
    assert projection["research_checkpoint"]["bytes"] == len(b"# Private notebook\nsecret detail")
    assert "markdown" not in projection["research_checkpoint"]
    assert "Private notebook" not in encode(projection)
    assert "secret detail" not in encode(projection)

    with store.tx() as db:
        db.execute(
            "UPDATE agent_actions SET state='failed',result=? WHERE id=?",
            (encode({"status": "done", "summary": "failed raw secret", "next": "private"}), action["id"]),
        )
        for index in range(55):
            db.execute(
                "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at) "
                "VALUES(?,?,?,0,'failed','{}',?,?)",
                (
                    f"research-{index:02d}",
                    eid,
                    "research",
                    1_000_000_000_000 + index,
                    1_000_000_000_000 + index,
                ),
            )
    bounded = store.projection(eid)
    assert len(bounded["research_turns"]) == 50
    assert bounded["research_turns"][0]["id"] == "research-05"
    assert all("summary" not in turn for turn in bounded["research_turns"])
    assert "failed raw secret" not in encode(bounded)


def test_ask_owner_saves_one_durable_question_and_its_responsibility(runtime):
    store, _, _, _ = runtime
    eid, action = make_turn(store)
    asked = question(options=["dataset A", "dataset B"])

    receipt = ask_owner(store, action, asked)

    saved = pending(store, eid)
    assert saved["id"] == str(asked.request_id)
    assert "rpc_id" not in saved and saved["expired"] is False and "answer" not in saved
    assert (saved["action_id"], saved["thread_id"]) == (action["id"], THREAD)
    assert saved["params"]["questions"] == [
        {"id": "question", "question": asked.question, "options": ["dataset A", "dataset B"]}
    ]
    assert (receipt["request_id"], receipt["action_id"], receipt["state"]) == (
        str(asked.request_id),
        action["id"],
        "open",
    )
    episode = obligation(store, eid, asked.request_id)
    assert (episode["owner"], episode["kind"], episode["state"]) == ("owner", "owner_answer", "open")
    assert (episode["target"], episode["wait"]) == (
        f"{eid}:{asked.request_id}",
        f"request:{asked.request_id}",
    )
    notices = store.rows("SELECT text FROM inbox WHERE experiment_id=? AND kind='required_decision'", (eid,))
    assert [row["text"] for row in notices] == [f"Owner answer needed: {asked.question}"]
    events = owner_events(store, eid)
    assert len(events) == 1
    assert json.loads(store.one("SELECT payload FROM events WHERE id=?", (events[0]["id"],))["payload"]) == {
        "request_id": str(asked.request_id),
        "action_id": action["id"],
        "thread_id": THREAD,
        "question": asked.question,
        "options": ["dataset A", "dataset B"],
    }
    assert events[0]["id"] in {row["id"] for row in store.rows("SELECT id FROM outbox")}


def test_the_private_question_text_is_redacted_from_published_events_only(runtime):
    store, _, _, _ = runtime
    eid, action = make_turn(store)
    asked = question(options=["dataset A"])
    ask_owner(store, action, asked)

    public = public_projection(store.projection(eid))

    event = next(event for event in public["events"] if event["kind"] == EVENT_KIND)
    assert json.loads(event["payload"]) == {
        "request_id": str(asked.request_id),
        "action_id": action["id"],
        "thread_id": THREAD,
    }
    assert public["pending_request"]["params"]["questions"][0] == {
        "id": "question",
        "question": asked.question,
        "options": ["dataset A"],
    }


def test_an_exact_replay_is_idempotent(runtime):
    store, _, _, _ = runtime
    eid, action = make_turn(store)
    asked = question(options=["one", "two"])

    first, saved, before = ask_owner(store, action, asked), pending(store, eid), counts(store, eid)

    assert ask_owner(store, action, asked) == first
    assert (pending(store, eid), counts(store, eid)) == (saved, before)


@pytest.mark.parametrize(
    "changed",
    [question(text="Which optimizer should the comparison use?"), question(options=["dataset A"])],
)
def test_a_changed_payload_for_the_same_request_id_refuses(runtime, changed):
    store, _, _, _ = runtime
    eid, action = make_turn(store)
    request_id = uuid4()
    ask_owner(store, action, question(request_id=request_id))
    saved, events = pending(store, eid), owner_events(store, eid)
    changed.request_id = request_id

    with pytest.raises(Conflict, match="different owner question"):
        ask_owner(store, action, changed)

    assert (pending(store, eid), owner_events(store, eid)) == (saved, events)


def test_a_second_unanswered_question_refuses(runtime):
    store, _, _, _ = runtime
    eid, action = make_turn(store)
    asked = question()
    ask_owner(store, action, asked)
    saved = pending(store, eid)

    with pytest.raises(Conflict, match="still unanswered"):
        ask_owner(store, action, question())

    assert pending(store, eid) == saved
    assert (len(owner_events(store, eid)), len(store.rows("SELECT * FROM inbox"))) == (1, 1)


def test_the_existing_answer_command_persists_and_satisfies_the_obligation(runtime):
    store, _, _, _ = runtime
    eid, action = make_turn(store)
    asked = question()
    ask_owner(store, action, asked)

    answer(store, eid, asked.request_id)

    assert pending(store, eid)["answer"] == "dataset B"
    assert obligation(store, eid, asked.request_id)["state"] == "satisfied"
    assert ask_owner(store, action, asked)["state"] == "answered"
    assert len(owner_events(store, eid)) == 1


def test_an_answered_but_unconsumed_request_is_never_replaced_on_consumption(runtime):
    store, _, _, _ = runtime
    eid, action = make_turn(store)
    first = question()
    ask_owner(store, action, first)
    answer(store, eid, first.request_id)

    with pytest.raises(Conflict, match="not been consumed"):
        ask_owner(store, action, question(text="Should the result be published?"))
    store.update(eid, pending_request=None)

    second = question(text="Should the result be published?")
    ask_owner(store, action, second)
    saved = pending(store, eid)
    assert ask_owner(store, action, first)["state"] == "closed"
    assert pending(store, eid) == saved
    assert (len(owner_events(store, eid)), obligation(store, eid, first.request_id)["state"]) == (
        2,
        "satisfied",
    )


@pytest.mark.parametrize("state", ["pending", "uncertain", "completed"])
def test_ask_owner_refuses_an_action_that_is_not_the_saved_running_turn(runtime, state):
    store, _, _, _ = runtime
    _, action = make_turn(store)
    with store.tx() as db:
        db.execute("UPDATE agent_actions SET state=? WHERE id=?", (state, action["id"]))
    snapshot = store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))

    with pytest.raises(Conflict, match="not running"):
        ask_owner(store, snapshot, question())


def test_ask_owner_refuses_a_changed_action_snapshot(runtime):
    from experimenttracker.codex_client import NeedsAttention

    store, _, _, _ = runtime
    _, action = make_turn(store)
    snapshot = dict(action)
    with store.tx() as db:
        db.execute("UPDATE agent_actions SET turn_id='turn-2' WHERE id=?", (action["id"],))

    with pytest.raises(NeedsAttention, match="saved action changed"):
        ask_owner(store, snapshot, question())


def test_ask_owner_refuses_a_different_action_thread(runtime):
    store, _, _, _ = runtime
    eid, action = make_turn(store)
    with store.tx() as db:
        db.execute("UPDATE experiments SET thread_id='another-thread' WHERE id=?", (eid,))

    with pytest.raises(Conflict, match="threads differ"):
        ask_owner(store, action, question())


@pytest.mark.parametrize(
    "contract,phase,deleted,paused,match",
    [
        (3, "preparing", False, False, "contract 4"),
        (4, "complete", False, False, "terminal experiment"),
        (4, "cancelled", False, False, "terminal experiment"),
        (4, "preparing", True, False, "deleted experiment"),
        (4, "preparing", False, True, "paused experiment"),
    ],
)
def test_ask_owner_refuses_a_frozen_deleted_terminal_or_paused_experiment(
    runtime, contract, phase, deleted, paused, match
):
    store, _, _, _ = runtime
    eid, action = make_turn(store)
    saved = store.experiment(eid)
    saved["contract"]["execution_contract"] = contract
    with store.tx() as db:
        db.execute(
            "UPDATE experiments SET contract=?,phase=?,deleted_at=?,pause_agent=? WHERE id=?",
            (encode(saved["contract"]), phase, 1.0 if deleted else None, int(paused), eid),
        )

    with pytest.raises(Conflict, match=match):
        ask_owner(store, action, question())


def test_ask_owner_refuses_during_maintenance(runtime):
    store, _, _, _ = runtime
    _, action = make_turn(store)
    release = "a" * 40
    with store.tx() as db:
        db.execute(
            "INSERT INTO releases(commit_id,summary,state,updated_at) VALUES(?,?,?,?)",
            (release, "maintenance release", "ready", 0),
        )
        db.execute(
            "INSERT INTO maintenance(request_id,release_id,state,created_at,updated_at) VALUES(?,?,?,?,?)",
            (str(uuid4()), release, "waiting", 0, 0),
        )

    with pytest.raises(Conflict, match="maintenance"):
        ask_owner(store, action, question())


def test_the_question_schema_is_strict_and_bounded():
    request_id = uuid4()
    assert AskOwnerQuestion(request_id=request_id, question="Why?").options == []
    invalid = [
        {"request_id": request_id, "question": "   "},
        {"request_id": request_id, "question": "x" * 2001},
        {"request_id": request_id, "question": "Why?", "kind": "approval"},
        {"request_id": request_id, "question": "Why?", "options": ["a"] * 11},
    ]
    for value in invalid:
        with pytest.raises(ValidationError):
            AskOwnerQuestion.model_validate(value)
