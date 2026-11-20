"""The CLI demo creates contract-3 work and finishes it with bounded synthetic compute.

This module keeps its own name so the historical fixture migration in
``conftest.py`` leaves the real execution contract in place.
"""

import asyncio
import json
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from experimenttracker import artifacts, cli, creation_policy
from experimenttracker.codex_client import DEMO_AGENT_SOURCE
from experimenttracker.store import Store

SUPPORTED_BRIEF = "Compare linear and quadratic regression on the fixed shared synthetic dataset."
UNSUPPORTED_BRIEF = "Train a neural classifier and draw its decision frontier."


def forbid_scientific_traversal(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("scientific provenance traversal")

    monkeypatch.setattr(artifacts, "local_output_identity", forbidden)
    monkeypatch.setattr(artifacts, "local_output_manifest", forbidden)


def run_demo(config, brief):
    return asyncio.run(cli.demo(config, brief, 60))


def read_store(config):
    return Store.read_only(config.state_dir / "workflow.sqlite")


def test_demo_runs_the_current_contract_end_to_end(monkeypatch, config):
    monkeypatch.setattr(creation_policy, "MODE", "legacy")
    forbid_scientific_traversal(monkeypatch)
    assert run_demo(config, SUPPORTED_BRIEF) == 0
    store = read_store(config)
    try:
        eid = store.rows("SELECT id FROM experiments")[0]["id"]
        exp = store.experiment(eid)
        assert exp["phase"] == "complete"
        assert exp["contract"]["execution_contract"] == 3
        assert exp["snapshot"]["plan"]["schema_version"] == 3
        assert exp["snapshot"]["plan"]["synthetic_demo"] is True
        assert not store.rows("SELECT id FROM stages WHERE experiment_id=? AND id='validate'", (eid,))
        approval = str(uuid5(NAMESPACE_URL, f"demo-plan-approval:{eid}:1"))
        assert store.rows("SELECT request_id FROM commands WHERE kind='plan_review'") == [
            {"request_id": approval}
        ]
        root = Path(config.job_output_root) / eid
        combined = json.loads((root / "results.json").read_text())
        measured = {(m["name"], m["variant"]): m["value"] for m in combined["metrics"]}
        assert set(measured) == {("mse", "linear"), ("mse", "quadratic")}
        assert measured[("mse", "quadratic")] < measured[("mse", "linear")]
        figure = store.one(
            "SELECT id,media_type FROM artifacts WHERE experiment_id=? AND name='predictions.svg'", (eid,)
        )
        assert figure["media_type"] == "image/svg+xml"
        assert (root / "predictions.svg").read_text().startswith("<svg")
        names = {
            row["name"] for row in store.rows("SELECT name FROM artifacts WHERE experiment_id=?", (eid,))
        }
        assert names >= {"metrics.json", "predictions.json", "predictions.svg", "results.json"}
        table, drawn = exp["analysis"]["presentation"]["blocks"][:2]
        assert [row[1]["metric"] for row in table["rows"]] == [
            {"name": "mse", "variant": variant} for variant in ("linear", "quadratic")
        ]
        assert drawn["artifact_id"] == figure["id"]
        assert exp["analysis"]["observations"][0]["evidence_ids"][-1] == figure["id"]
        versions = store.rows("SELECT * FROM report_versions WHERE experiment_id=? ORDER BY version", (eid,))
        final = versions[-1]
        assert final["request_id"].startswith("final-report:")
        assert final["state"] == "ready" and final["report_artifact_id"]
        assert len(versions) > 1 and final["version"] == max(row["version"] for row in versions)
        assert json.loads(versions[0]["document"])["title"] == "Synthetic working results"
        assert json.loads(final["document"])["title"] == "Synthetic regression demo report"
        assert [row["stage_id"] for row in store.rows("SELECT stage_id FROM jobs ORDER BY stage_id")] == [
            "figure",
            "measure",
        ]
        assert [row["phase"] for row in store.rows("SELECT phase FROM agent_actions ORDER BY rowid")] == [
            "plan",
            "prepare",
            "interpret",
        ]
        published = store.one(
            "SELECT id FROM artifacts WHERE experiment_id=? AND name=?",
            (eid, f"report-v{final['version']}.html"),
        )
        assert published["id"] == final["report_artifact_id"]
        assert store.projection(eid)["active_report_id"] == final["report_artifact_id"]
    finally:
        store.close()


def test_demo_recipe_keeps_held_out_points_off_the_training_grid():
    namespace = {"__name__": "demo_recipe_source"}
    exec(compile(DEMO_AGENT_SOURCE, "recipe.py", "exec"), namespace)
    assert (len(namespace["TRAIN_X"]), len(set(namespace["TEST_X"]))) == (24, 8)
    assert set(namespace["TRAIN_X"]).isdisjoint(namespace["TEST_X"])


def test_demo_refuses_a_live_configuration(monkeypatch, config):
    def forbidden(*args, **kwargs):
        raise AssertionError("the demo must not start a live runtime")

    monkeypatch.setattr(cli, "runtime", forbidden)
    assert run_demo(config.model_copy(update={"mode": "live"}), SUPPORTED_BRIEF) == 1
    assert not (config.state_dir / "workflow.sqlite").exists()


def test_demo_asks_before_running_an_unsupported_brief(monkeypatch, config):
    monkeypatch.setattr(creation_policy, "MODE", "legacy")
    forbid_scientific_traversal(monkeypatch)
    assert run_demo(config, UNSUPPORTED_BRIEF) == 1
    store = read_store(config)
    try:
        eid = store.rows("SELECT id FROM experiments")[0]["id"]
        review = store.experiment(eid)["plan_review"]
        assert review["state"] == "awaiting_review" and review["scope"] == "plan"
        assert review["questions"] and review["stages"] == []
        assert store.rows("SELECT * FROM jobs") == []
        assert not store.rows("SELECT * FROM commands WHERE kind='plan_review'")
    finally:
        store.close()
