"""Contract-4 register_result through the MCP bridge."""

import json
import threading
from uuid import uuid4

from test_research_result_tool import RESULT, add_saved_job, request
from test_research_settlement import setup

from experimenttracker import controller
from experimenttracker.research import result_tool
from experimenttracker.research.result_tool import RegisterResult


def test_register_result_schema_is_advertised_from_the_actual_model():
    served = {item["name"]: item for item in controller.served_tools(research=True)}
    schema = served["register_result"]["inputSchema"]
    assert schema == RegisterResult.model_json_schema()
    assert schema["additionalProperties"] is False
    assert schema["required"] == ["request_id", "intent_id", "path"]
    assert set(schema["properties"]) == {"request_id", "intent_id", "path"}
    legacy = {item["name"]: item for item in controller.served_tools()}
    assert legacy["register_result"]["inputSchema"]["required"] == ["stage_id"]


async def test_register_result_runs_off_loop_and_replays_stable_receipt(runtime, tmp_path, monkeypatch):
    store, scheduler, agent, _ = runtime
    _, eid, workflow, _ = setup(runtime, tmp_path, monkeypatch)
    intent = str(uuid4())
    root = workflow.projects.base_root(eid) / "attempts" / intent
    root.mkdir(parents=True)
    add_saved_job(store, eid, intent, root)
    (root / "results.json").write_text(json.dumps(RESULT))
    payload = request(intent)
    main_thread = threading.main_thread()
    worker_threads = []
    register = result_tool.register_result

    def record(*args):
        worker_threads.append(threading.current_thread())
        return register(*args)

    monkeypatch.setattr(controller, "register_research_result", record)
    receipt = await workflow.controller.call(eid, "register_result", payload)
    assert worker_threads and main_thread not in worker_threads
    assert receipt["intent_id"] == intent
    assert await workflow.controller.call(eid, "register_result", payload) == receipt
    assert len(store.rows("SELECT * FROM events WHERE kind=?", (result_tool.EVENT_KIND,))) == 1
    assert scheduler.submissions == 0 and agent.calls == 0
