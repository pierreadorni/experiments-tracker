"""Contract-4 register_figure: one inert, idempotent stable image copy.

Covers strict input, the sealed root copy and gateway path, replay after the
source changes or disappears, and refusal of a different source, caption or turn.
"""

import json
import stat
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import ValidationError
from test_integration_commands import branch_experiment
from test_tick_steps import block_maintenance

from experimenttracker import artifacts
from experimenttracker.codex_client import NeedsAttention
from experimenttracker.gateway_client import GatewayClient
from experimenttracker.research import figure_tool
from experimenttracker.research.figure_tool import RegisterFigure, register_figure
from experimenttracker.store import Conflict, encode

BRIEF = "Compare two toy optimizers under one fixed compute budget."
PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 32


def saved_contract4(store, *, thread="thread-1"):
    eid = store.command(uuid4(), "create", {"brief": BRIEF, "recipe_id": "agent_experiment_v1"})[
        "experiment_id"
    ]
    contract = {**store.experiment(eid)["contract"], "execution_contract": 4}
    with store.tx() as db:
        db.execute(
            "UPDATE experiments SET contract=?,thread_id=? WHERE id=?", (encode(contract), thread, eid)
        )
    return eid


def add_action(store, eid, *, phase="research", state="running", thread="thread-1"):
    identity = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,input,"
            "created_at,updated_at) VALUES(?,?,?,1,?,?,'prompt',0,0)",
            (identity, eid, phase, state, thread),
        )
    return store.one("SELECT * FROM agent_actions WHERE id=?", (identity,))


@pytest.fixture
def figure_tree(runtime, tmp_path, monkeypatch):
    """One live contract-4 turn with a light workspace and an existing image."""
    store, _, _, workflow = runtime
    eid = saved_contract4(store)
    action = add_action(store, eid)
    worktree = tmp_path / "worktree"
    (worktree / "figures").mkdir(parents=True)
    (worktree / "figures" / "plot.png").write_bytes(PNG)
    monkeypatch.setattr(workflow.workspaces, "branch_checkout", lambda experiment_id: str(worktree))
    return store, workflow, eid, action, worktree


def figure_request(request_id=None, *, path="figures/plot.png", caption="Training loss by step"):
    return RegisterFigure(request_id=request_id or uuid4(), path=path, caption=caption)


def test_register_figure_input_is_strict_and_bounded():
    request_id = uuid4()
    assert (
        RegisterFigure.model_validate(
            {"request_id": str(request_id), "path": "figures/plot.png", "caption": "Training loss"}
        ).path
        == "figures/plot.png"
    )
    for payload in (
        {},
        {"request_id": str(request_id), "path": "figures/plot.png"},
        {"request_id": str(request_id), "caption": "Training loss"},
        {"request_id": "not-a-uuid", "path": "figures/plot.png", "caption": "Training loss"},
        {"request_id": str(request_id), "path": "figures/plot.png", "caption": "Training loss", "extra": 1},
        {"request_id": str(request_id), "path": "   ", "caption": "Training loss"},
        {"request_id": str(request_id), "path": "../plot.png", "caption": "Training loss"},
        {"request_id": str(request_id), "path": "figures/../../plot.png", "caption": "Training loss"},
        {"request_id": str(request_id), "path": "p" * 1025, "caption": "Training loss"},
        {"request_id": str(request_id), "path": "figures/plot.png", "caption": "   "},
        {"request_id": str(request_id), "path": "figures/plot.png", "caption": "c" * 1501},
    ):
        with pytest.raises(ValidationError):
            RegisterFigure.model_validate(payload)


def assert_stable_registration(workflow, store, eid, action, request, receipt, figure, request_id):
    """Assert the sealed copy, its gateway path, and replay once the source is gone."""
    saved = store.rows("SELECT * FROM artifacts WHERE experiment_id=?", (eid,))
    assert [row["name"] for row in saved] == [f"figure-{request_id}.png"]
    assert receipt == {
        "request_id": str(request_id),
        "experiment_id": eid,
        "action_id": action["id"],
        "artifact_id": saved[0]["id"],
        "name": f"figure-{request_id}.png",
        "caption": "Training loss by step",
        "source": "worktree:figures/loss.png",
        "sha256": artifacts.sha256(figure),
        "size": len(PNG),
        "media_type": "image/png",
    }
    stable = Path(saved[0]["path"])
    assert stable != figure and stable.is_relative_to(workflow.projects.base_root(eid).resolve())
    assert stable.read_bytes() == PNG and not stable.is_symlink()
    assert stat.S_IMODE(stable.stat().st_mode) == 0o444
    # The gateway reads the saved artifact through the experiment base root.
    assert GatewayClient.artifact_bytes(dict(saved[0]), workflow.projects.base_root(eid)) == PNG
    assert [(row["stage_id"], row["evidence_revision"], row["remote_published"]) for row in saved] == [
        ("research", 1, 0)
    ]
    events = store.rows("SELECT kind,payload FROM events WHERE experiment_id=? ORDER BY seq", (eid,))
    assert [row["kind"] for row in events][-2:] == ["artifacts_registered", "research_figure_registered"]
    assert json.loads(events[-1]["payload"]) == receipt
    assert store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,)) == []

    assert register_figure(workflow, store.experiment(eid), action, request) == receipt
    assert len(store.rows("SELECT * FROM artifacts WHERE experiment_id=?", (eid,))) == 1
    # Editing or deleting the mutable source changes neither the receipt nor the copy.
    figure.write_bytes(PNG + b"edited")
    assert register_figure(workflow, store.experiment(eid), action, request) == receipt
    figure.unlink()
    assert register_figure(workflow, store.experiment(eid), action, request) == receipt
    assert stable.read_bytes() == PNG and artifacts.sha256(stable) == receipt["sha256"]
    assert (
        len(
            store.rows("SELECT * FROM events WHERE experiment_id=? AND kind=?", (eid, figure_tool.EVENT_KIND))
        )
        == 1
    )

    with pytest.raises(Conflict, match="different metadata or scope"):
        register_figure(workflow, store.experiment(eid), action, figure_request(request_id, caption="Other"))
    with pytest.raises(Conflict, match="different metadata or scope"):
        register_figure(
            workflow, store.experiment(eid), action, figure_request(request_id, path="figures/other.png")
        )


async def test_register_figure_seals_one_stable_worktree_copy_idempotently(runtime, tmp_path, monkeypatch):
    store, scheduler, agent, workflow = runtime
    _source, _store, _workflow, eid = await branch_experiment(runtime, tmp_path, monkeypatch)
    contract = {**store.experiment(eid)["contract"], "execution_contract": 4}
    with store.tx() as db:
        db.execute(
            "UPDATE experiments SET contract=?,thread_id=? WHERE id=?", (encode(contract), "thread-1", eid)
        )
    action = add_action(store, eid)
    figure = Path(workflow.workspaces.branch_checkout(eid)) / "figures" / "loss.png"
    figure.parent.mkdir(parents=True)
    figure.write_bytes(PNG)
    request_id = uuid4()
    request = figure_request(request_id, path="figures/loss.png")

    receipt = register_figure(workflow, store.experiment(eid), action, request)

    assert_stable_registration(workflow, store, eid, action, request, receipt, figure, request_id)
    assert scheduler.submissions == 0 and agent.calls == 0


def test_register_figure_copies_an_absolute_output_root_image_under_the_saved_root(figure_tree):
    store, workflow, eid, action, worktree = figure_tree
    output_root = workflow.root(eid)
    (output_root / "figures").mkdir(parents=True)
    image = output_root / "figures" / "evaluation.png"
    image.write_bytes(PNG)
    request_id = uuid4()

    receipt = register_figure(
        workflow,
        store.experiment(eid),
        action,
        figure_request(request_id, path=str(image), caption="Evaluation"),
    )

    saved = store.one("SELECT * FROM artifacts WHERE id=?", (receipt["artifact_id"],))
    assert Path(saved["path"]) == output_root.resolve() / f"figure-{request_id}.png"
    event = store.one(
        "SELECT payload FROM events WHERE experiment_id=? AND kind=?", (eid, figure_tool.EVENT_KIND)
    )
    assert json.loads(event["payload"])["source"] == "output:figures/evaluation.png"
    # A relative path never falls back to the output root: only the worktree is searched.
    with pytest.raises(ValueError, match="missing or inaccessible figure"):
        register_figure(
            workflow, store.experiment(eid), action, figure_request(path="figures/evaluation.png")
        )
    assert len(store.rows("SELECT * FROM artifacts WHERE experiment_id=?", (eid,))) == 1


def test_register_figure_refuses_unsafe_or_non_image_paths(figure_tree, tmp_path):
    store, workflow, eid, action, worktree = figure_tree
    outside = tmp_path / "outside.png"
    outside.write_bytes(PNG)
    (worktree / "escape.png").symlink_to(outside)
    (worktree / "linked").symlink_to(tmp_path)
    (worktree / "notes.txt").write_text("not an image")
    with (worktree / "huge.png").open("wb") as stream:
        stream.truncate(artifacts.MAX_ARTIFACT_BYTES + 1)

    for value, message in (
        (str(outside), "inside the saved worktree or output root"),
        ("escape.png", "inside the saved worktree or output root"),
        ("linked/outside.png", "inside the saved worktree or output root"),
        ("notes.txt", "registered image"),
        ("missing.png", "missing or inaccessible figure"),
        ("huge.png", "10 MiB"),
    ):
        with pytest.raises(ValueError, match=message):
            register_figure(workflow, store.experiment(eid), action, figure_request(path=value))
    assert store.rows("SELECT * FROM artifacts WHERE experiment_id=?", (eid,)) == []
    assert not workflow.root(eid).exists()


def test_register_figure_copy_reenforces_the_bound_and_never_overwrites(figure_tree, monkeypatch):
    store, workflow, eid, action, worktree = figure_tree
    output_root = workflow.root(eid)
    request_id = uuid4()
    target = output_root / f"figure-{request_id}.png"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"existing")

    with pytest.raises(ValueError, match="cannot change"):
        register_figure(workflow, store.experiment(eid), action, figure_request(request_id))
    assert target.read_bytes() == b"existing"

    grown = worktree / "grown.png"
    with grown.open("wb") as stream:
        stream.truncate(artifacts.MAX_ARTIFACT_BYTES + 1)
    monkeypatch.setattr(figure_tool, "_locate", lambda *args: grown)
    other = figure_request()
    with pytest.raises(ValueError, match="10 MiB"):
        register_figure(workflow, store.experiment(eid), action, other)
    assert not (output_root / f"figure-{other.request_id}.png").exists()


@pytest.mark.parametrize(
    "change,message",
    [
        ("contract", "execution contract 4"),
        ("phase", "active research turn"),
        ("pending", "active research turn"),
        ("paused", "live research experiment"),
        ("complete", "live research experiment"),
        ("deleted", "live research experiment"),
        ("thread", "exact saved research thread"),
        ("maintenance", "maintenance forbids research writes"),
    ],
)
def test_register_figure_refuses_a_non_live_or_blocked_research_identity(figure_tree, change, message):
    store, workflow, eid, action, _worktree = figure_tree
    if change == "contract":
        contract = {**store.experiment(eid)["contract"], "execution_contract": 3}
        with store.tx() as db:
            db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
    elif change == "phase":
        with store.tx() as db:
            db.execute("UPDATE agent_actions SET phase='interpret' WHERE id=?", (action["id"],))
    elif change == "pending":
        with store.tx() as db:
            db.execute("UPDATE agent_actions SET state='pending' WHERE id=?", (action["id"],))
    elif change == "paused":
        with store.tx() as db:
            db.execute("UPDATE experiments SET pause_agent=1 WHERE id=?", (eid,))
    elif change == "complete":
        with store.tx() as db:
            db.execute("UPDATE experiments SET phase='complete' WHERE id=?", (eid,))
    elif change == "deleted":
        with store.tx() as db:
            db.execute("UPDATE experiments SET deleted_at=1 WHERE id=?", (eid,))
    elif change == "thread":
        with store.tx() as db:
            db.execute("UPDATE agent_actions SET thread_id=NULL WHERE id=?", (action["id"],))
    elif change == "maintenance":
        block_maintenance(store)

    action = store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))
    with pytest.raises(ValueError, match=message):
        register_figure(workflow, store.experiment(eid), action, figure_request())
    assert store.rows("SELECT * FROM artifacts WHERE experiment_id=?", (eid,)) == []
    # The gate runs before the stable copy is written.
    assert not workflow.root(eid).exists()


def test_register_figure_refuses_another_experiments_action_and_a_moved_snapshot(figure_tree):
    store, workflow, eid, action, _worktree = figure_tree
    other_action = add_action(store, saved_contract4(store))
    with pytest.raises(ValueError, match="active research turn"):
        register_figure(workflow, store.experiment(eid), other_action, figure_request())
    with pytest.raises(NeedsAttention, match="saved action disappeared"):
        register_figure(workflow, store.experiment(eid), {"id": str(uuid4())}, figure_request())

    stale = dict(action)
    with store.tx() as db:
        db.execute("UPDATE agent_actions SET state='dispatching' WHERE id=?", (action["id"],))
    with pytest.raises(NeedsAttention, match="changed during reconciliation"):
        register_figure(workflow, store.experiment(eid), stale, figure_request())
    assert store.rows("SELECT * FROM artifacts WHERE experiment_id=?", (eid,)) == []
    assert not workflow.root(eid).exists()
