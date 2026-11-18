"""T4.6: a failed plot must not prevent published metrics or a metrics-only report."""

import asyncio
import json
from pathlib import Path
from uuid import uuid4

import pytest
from test_research_create import create, git_project
from test_research_write_tools import add_research_turn, document

from experimenttracker.research.settlement import complete


@pytest.mark.asyncio
async def test_failed_plot_keeps_registered_metrics_and_allows_metrics_only_report(runtime, config, tmp_path):
    store, scheduler, agent, workflow = runtime
    git_project(config, store, tmp_path, research_loop=True)
    _, _, receipt = create(store)
    eid = receipt["experiment_id"]
    workflow.workspaces.ensure_branch(eid)
    action = add_research_turn(store, eid)
    with store.tx() as db:
        db.execute("UPDATE agent_actions SET turn_id='plot-turn' WHERE id=?", (action["id"],))
    action = store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))

    intent = str(uuid4())
    root = workflow.projects.base_root(eid) / "attempts" / intent
    root.mkdir(parents=True)
    with store.tx() as db:
        db.execute(
            "INSERT INTO stages(experiment_id,id,state,attempt,updated_at) "
            "VALUES(?,'evaluate','complete',1,0)",
            (eid,),
        )
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,job_id,script_hash,resources,"
            "snapshot_hash,created_at,execution_root,execution_manifest,ended_at) "
            "VALUES(?,?,'evaluate',1,'complete','synthetic-job','script','{}','snapshot',0,?,?,1)",
            (intent, eid, str(root), json.dumps({"executor": "slurm"})),
        )
    (root / "results.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "protocol": "synthetic comparison",
                "metrics": [{"name": "mse", "variant": "toy", "value": 0.25, "unit": "fraction"}],
            }
        )
    )

    registered = await workflow.controller.call(
        eid,
        "register_result",
        {"request_id": str(uuid4()), "intent_id": intent, "path": "results.json"},
    )
    assert registered["intent_id"] == intent
    metric_artifact = store.one("SELECT path FROM artifacts WHERE id=?", (registered["artifact_id"],))
    result_data = await asyncio.to_thread(Path(metric_artifact["path"]).read_text)
    assert json.loads(result_data)["metrics"][0]["value"] == 0.25

    with pytest.raises(ValueError, match="missing or inaccessible figure"):
        await workflow.controller.call(
            eid,
            "register_figure",
            {"request_id": str(uuid4()), "path": "figures/missing.png", "caption": "Comparison plot"},
        )
    assert store.one("SELECT id FROM artifacts WHERE id=?", (registered["artifact_id"],))

    report_document = document()
    report_document["blocks"][1]["rows"][0][1]["metric"]["variant"] = "toy"
    queued = await workflow.controller.call(
        eid,
        "update_report",
        {"request_id": str(uuid4()), "document": report_document},
    )
    assert queued["state"] == "queued"
    await workflow.advance_reports()

    saved = store.one("SELECT state,error FROM report_versions WHERE experiment_id=?", (eid,))
    assert saved == {"state": "ready", "error": None}
    names = {row["name"] for row in store.rows("SELECT name FROM artifacts WHERE experiment_id=?", (eid,))}
    assert registered["name"] in names
    assert "report.html" in names
    assert not any(name.startswith("figure-") for name in names)
    settled = await complete(
        workflow,
        action,
        {
            "status": "done",
            "summary": "Metrics report complete",
            "next": "None",
            "integrate": {"value": False, "reason": "Synthetic scenario"},
        },
    )
    assert settled["state"] == "completed"
    assert store.experiment(eid)["phase"] == "complete"
    assert scheduler.submissions == 0 and agent.calls == 0
