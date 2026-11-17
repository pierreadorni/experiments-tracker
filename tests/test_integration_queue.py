"""Durable integration queue state in C2 obligations (T3.4b-1)."""

import pytest

from experimenttracker import clock
from experimenttracker.core import obligations
from experimenttracker.engine import integration_queue as queue
from experimenttracker.store import Store


@pytest.fixture
def store(tmp_path):
    value = Store(tmp_path / "state" / "workflow.sqlite")
    yield value
    value.close()


def repo(tmp_path, name="repo"):
    path = tmp_path / name
    path.mkdir()
    return str(path)


def run(store, function, *args, **kwargs):
    with store.tx() as db:
        return function(db, *args, **kwargs)


def test_enqueue_dedupes_caller_id_and_canonical_repo(store, tmp_path, monkeypatch):
    path = repo(tmp_path)
    monkeypatch.setattr(clock, "now", lambda: 10.0)
    first = run(store, queue.enqueue, path, "r1", {"reason": "keep"})
    monkeypatch.setattr(clock, "now", lambda: 20.0)
    same = run(store, queue.enqueue, tmp_path / "repo", "r1", {"reason": "keep"})
    assert (same["obligation_id"], same["updated_at"]) == (first["obligation_id"], 10.0)
    with pytest.raises(ValueError, match="already bound"):
        run(store, queue.enqueue, path, "r1", {"reason": "other"})
    with pytest.raises(ValueError, match="already bound"):
        run(store, queue.enqueue, repo(tmp_path, "other"), "r1", {"reason": "keep"})
    with pytest.raises(ValueError, match="already bound"):
        run(store, queue.enqueue, path, "r1", {"reason": "keep"}, project_id="project")


def test_claim_is_fifo_and_holds_one_active_candidate_per_repository(store, tmp_path, monkeypatch):
    path, other = repo(tmp_path), repo(tmp_path, "other")
    for tick, rid in ((10.0, "r1"), (20.0, "r2"), (30.0, "r3")):
        monkeypatch.setattr(clock, "now", lambda tick=tick: tick)
        run(store, queue.enqueue, path if rid != "r3" else other, rid, {"reason": rid})
    first = run(store, queue.claim, path)
    assert (first["request_id"], first["stage"]) == ("r1", "claimed")
    assert run(store, queue.claim, path) is None
    assert run(store, queue.claim, other)["request_id"] == "r3"
    assert [view["request_id"] for view in run(store, queue.pending, path)] == ["r2"]
    assert run(store, queue.active, path)["request_id"] == "r1"


@pytest.mark.parametrize(
    ("stage", "owner"),
    [("conflict", "owner"), ("check_failed", "agent"), ("awaiting_approval", "owner")],
)
def test_wait_stages_cannot_apply(store, tmp_path, stage, owner):
    path = repo(tmp_path, stage)
    run(store, queue.enqueue, path, "r1", {"reason": stage})
    run(store, queue.claim, path)
    waiting = run(store, queue.checkpoint, "r1", stage, candidate={"state": stage})
    assert (waiting["owner"], waiting["state"]) == (owner, "open")
    if stage != "awaiting_approval":
        with pytest.raises(ValueError, match="cannot move"):
            run(store, queue.checkpoint, "r1", "ready")
    with pytest.raises(ValueError, match="cannot be applied"):
        run(store, queue.finish, "r1", "applied")
    if stage == "awaiting_approval":
        assert run(store, queue.checkpoint, "r1", "ready")["stage"] == "ready"
    else:
        assert run(store, queue.checkpoint, "r1", "claimed")["stage"] == "claimed"
    assert run(store, queue.finish, "r1", "abandoned")["state"] == "abandoned"
    assert run(store, queue.active, path) is None


def test_ready_applies_and_terminal_marking_releases_repository(store, tmp_path):
    path = repo(tmp_path)
    run(store, queue.enqueue, path, "r1", {"reason": "one"})
    run(store, queue.enqueue, path, "r2", {"reason": "two"})
    run(store, queue.claim, path)
    ready = run(store, queue.checkpoint, "r1", "ready", candidate={"key": "exp-1", "state": "ready"})
    assert ready["stage"] == "ready"
    assert run(store, queue.claim, path) is None
    applied = run(store, queue.finish, "r1", "applied")
    assert (applied["state"], applied["stage"]) == ("satisfied", "applied")
    assert run(store, queue.finish, "r1", "applied")["obligation_id"] == applied["obligation_id"]
    with pytest.raises(ValueError, match="saved outcome"):
        run(store, queue.finish, "r1", "abandoned")
    assert run(store, queue.claim, path)["request_id"] == "r2"


def test_candidate_identity_is_exact_on_replay_and_new_only_on_transition(store, tmp_path, monkeypatch):
    path = repo(tmp_path)
    run(store, queue.enqueue, path, "r1", {"reason": "one"})
    run(store, queue.claim, path)
    first = {"key": "exp-1", "state": "ready", "candidate_commit": "a" * 40}
    ready = run(store, queue.checkpoint, "r1", "ready", candidate=first)
    monkeypatch.setattr(clock, "now", lambda: 99.0)
    replay = run(store, queue.checkpoint, "r1", "ready", candidate=dict(first))
    assert (replay["updated_at"], replay["candidate"]) == (ready["updated_at"], first)
    with pytest.raises(ValueError, match="cannot change"):
        run(store, queue.checkpoint, "r1", "ready", candidate={"key": "other"})
    assert run(store, queue.checkpoint, "r1", "claimed")["candidate"] == first
    fresh = {"key": "exp-1", "state": "ready", "candidate_commit": "b" * 40}
    assert run(store, queue.checkpoint, "r1", "ready", candidate=fresh)["candidate"] == fresh
    run(store, queue.finish, "r1", "applied")
    assert run(store, queue.enqueue, path, "r1", {"reason": "one"})["stage"] == "applied"


def test_candidate_data_and_status_text_stay_separate(store, tmp_path):
    path = repo(tmp_path)
    run(store, queue.enqueue, path, "r1", {"reason": "one"})
    run(store, queue.claim, path)
    raw = {"state": "conflict", "conflicts": ["tracked.txt"], "note": "raw"}
    view = run(store, queue.checkpoint, "r1", "conflict", candidate=raw, status="owner should resolve")
    assert (view["candidate"], view["status"], view["stage"]) == (raw, "owner should resolve", "conflict")
    changed = run(store, queue.checkpoint, "r1", "conflict", status="ready")
    assert (changed["stage"], changed["candidate"], changed["status"]) == ("conflict", raw, "ready")


def test_generic_obligations_stay_untouched(store, tmp_path):
    path = repo(tmp_path)
    with store.tx() as db:
        obligations.open(
            db, "owner_answer:manual", kind="owner_answer", owner="owner", wait="answer:1", detail="keep"
        )
        obligations.open(
            db, "integration:manual", kind="integration", owner="backend", wait="none", detail="generic"
        )
    before = store.rows("SELECT * FROM obligations ORDER BY id")
    run(store, queue.enqueue, path, "r1", {"reason": "one"})
    run(store, queue.claim, path)
    run(store, queue.checkpoint, "r1", "ready")
    run(store, queue.finish, "r1", "applied")
    after = store.rows(
        "SELECT * FROM obligations WHERE id LIKE 'owner_answer:%' OR id='integration:manual' ORDER BY id"
    )
    assert after == before


def test_queue_depth_and_wait_history_are_bounded(store, tmp_path, monkeypatch):
    path = repo(tmp_path)
    monkeypatch.setattr(queue, "MAX_QUEUED", 2)
    run(store, queue.enqueue, path, "r1", {"reason": "one"})
    run(store, queue.enqueue, path, "r2", {"reason": "two"})
    with pytest.raises(ValueError, match="full"):
        run(store, queue.enqueue, path, "r3", {"reason": "three"})
    run(store, queue.claim, path)
    assert run(store, queue.enqueue, path, "r3", {"reason": "three"})["stage"] == "queued"
    monkeypatch.setattr(queue, "MAX_EPISODES", 2)
    run(store, queue.checkpoint, "r1", "conflict")
    with pytest.raises(ValueError, match="bounded wait"):
        run(store, queue.checkpoint, "r1", "claimed")
