"""Contract-4 register_figure through the MCP bridge: one stable copy (T4.2).

The tool advertises exactly ``RegisterFigure``, copies the existing image off the
controller loop and returns the saved artifact receipt. Repeating the request UUID
adopts that receipt even after the mutable source changes or disappears; contract-3
stages, Git and live dispatch stay untouched.
"""

import hashlib
import json
import stat
import threading
from asyncio import to_thread
from pathlib import Path
from uuid import uuid4

import pytest
from test_research_settlement import setup

from experimenttracker import artifacts, controller
from experimenttracker.research import figure_tool
from experimenttracker.research.figure_tool import RegisterFigure
from experimenttracker.store import Conflict

CAPTION = "Training loss by step"
PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 32


def asked(request_id, *, path="figures/loss.png", caption=CAPTION):
    return {"request_id": str(request_id), "path": path, "caption": caption}


def test_register_figure_schema_is_advertised_from_the_actual_model():
    served = {item["name"]: item for item in controller.served_tools(research=True)}
    schema = served["register_figure"]["inputSchema"]
    assert schema == RegisterFigure.model_json_schema()
    assert schema["additionalProperties"] is False
    assert schema["required"] == ["request_id", "path", "caption"]
    assert set(schema["properties"]) == {"request_id", "path", "caption"}
    assert schema["properties"]["request_id"]["format"] == "uuid"
    assert "register_figure" not in {item["name"] for item in controller.served_tools()}


async def test_register_figure_seals_one_worktree_copy_and_replays_the_uuid(runtime, tmp_path, monkeypatch):
    store, scheduler, agent, _ = runtime
    _, eid, workflow, _action = setup(runtime, tmp_path, monkeypatch)
    worktree = Path(workflow.workspaces.branch(eid)["worktree"])
    figure = worktree / "figures" / "loss.png"
    figure.parent.mkdir(parents=True)
    figure.write_bytes(PNG)
    request_id = uuid4()
    payload = asked(request_id)

    main_thread = threading.main_thread()
    copy_threads = []
    publish = figure_tool._publish

    def record_thread(*args, **kwargs):
        copy_threads.append(threading.current_thread())
        return publish(*args, **kwargs)

    monkeypatch.setattr(figure_tool, "_publish", record_thread)
    stages = store.rows("SELECT * FROM stages WHERE experiment_id=? ORDER BY id", (eid,))
    head = workflow.workspaces.branch(eid)["head_commit"]

    receipt = await workflow.controller.call(eid, "register_figure", payload)

    # The bounded copy and hash run on a worker thread, not the controller loop.
    assert copy_threads and main_thread not in copy_threads
    saved = store.rows("SELECT * FROM artifacts WHERE experiment_id=?", (eid,))
    assert [row["name"] for row in saved] == [f"figure-{request_id}.png"]
    stable = Path(saved[0]["path"])
    assert receipt == {
        "request_id": str(request_id),
        "experiment_id": eid,
        "action_id": store.one("SELECT id FROM agent_actions WHERE experiment_id=?", (eid,))["id"],
        "artifact_id": saved[0]["id"],
        "name": f"figure-{request_id}.png",
        "caption": CAPTION,
        "source": "worktree:figures/loss.png",
        "sha256": hashlib.sha256(PNG).hexdigest(),
        "size": len(PNG),
        "media_type": "image/png",
    }
    assert stable.is_relative_to(workflow.projects.base_root(eid).resolve())
    stable_bytes, stable_stat = await to_thread(lambda: (stable.read_bytes(), stable.stat()))
    assert stable_bytes == PNG and stat.S_IMODE(stable_stat.st_mode) == 0o444
    kinds = [row["kind"] for row in store.rows("SELECT kind FROM events WHERE experiment_id=?", (eid,))]
    assert kinds[-2:] == ["artifacts_registered", figure_tool.EVENT_KIND]
    event = store.one(
        "SELECT payload FROM events WHERE experiment_id=? AND kind=?", (eid, figure_tool.EVENT_KIND)
    )
    assert json.loads(event["payload"]) == receipt

    # No contract-3 work: saved stages unchanged, no jobs, commit or live call.
    assert store.rows("SELECT * FROM stages WHERE experiment_id=? ORDER BY id", (eid,)) == stages
    assert store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,)) == []
    assert workflow.workspaces.branch(eid)["head_commit"] == head
    assert scheduler.submissions == 0 and agent.calls == 0

    # The repeated UUID adopts the saved receipt once the mutable source changes
    # or disappears; the sealed stable copy is what remains registered.
    figure.write_bytes(PNG + b"edited")
    assert await workflow.controller.call(eid, "register_figure", payload) == receipt
    figure.unlink()
    assert await workflow.controller.call(eid, "register_figure", payload) == receipt
    stable_bytes, stable_hash = await to_thread(lambda: (stable.read_bytes(), artifacts.sha256(stable)))
    assert stable_bytes == PNG and stable_hash == receipt["sha256"]
    assert len(store.rows("SELECT * FROM artifacts WHERE experiment_id=?", (eid,))) == 1
    assert (
        len(
            store.rows("SELECT * FROM events WHERE experiment_id=? AND kind=?", (eid, figure_tool.EVENT_KIND))
        )
        == 1
    )
    with pytest.raises(Conflict, match="different metadata or scope"):
        await workflow.controller.call(eid, "register_figure", asked(request_id, caption="Other"))
