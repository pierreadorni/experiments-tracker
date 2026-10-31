import json
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError
from test_workflow import submit, until

from experimenttracker import artifacts, reporting
from experimenttracker.api import create_app
from experimenttracker.models import DiscussionReply, ReportDocument
from experimenttracker.workflow import Workflow


def document():
    return {
        "title": "A readable result",
        "standfirst": "A bounded synthetic comparison.",
        "blocks": [
            {"kind": "paragraph", "text": "Question and interpretation, before the evidence."},
            {
                "kind": "table",
                "caption": "Held-out error (target squared).",
                "columns": ["Model", "MSE"],
                "rows": [
                    [{"text": "Linear"}, {"metric": {"name": "mse", "variant": "linear"}, "decimals": 4}]
                ],
            },
        ],
    }


async def test_report_revision_is_idempotent_survives_restart_and_preserves_science(runtime):
    store, scheduler, agent, workflow = runtime
    eid = submit(store)
    await until(workflow, store, eid, lambda e: e["phase"] == "complete")
    root = workflow.root(eid)
    original = {
        name: (root / name).read_bytes()
        for name in ("report.html", "results.json", "snapshot/recipe.py", "analysis.json")
    }
    request = uuid4()
    receipt = store.command(request, "present", {"document": document()}, eid=eid)
    assert store.command(request, "present", {"document": document()}, eid=eid) == receipt
    assert len(store.projection(eid)["report_versions"]) == 1
    # A crash after deterministic outputs but before their registration must adopt the same bytes.
    store.report_state(eid, 2, "building")
    records = store.rows("SELECT * FROM artifacts WHERE experiment_id=?", (eid,))
    _, files = reporting.build_report(store.experiment(eid), root, records, document(), 2)
    hashes = {name: artifacts.sha256(root / name) for name in files}
    await workflow.close()
    restarted = Workflow(workflow.config, store, scheduler, agent)
    await restarted.tick(force_observe=True)
    view = store.projection(eid)
    assert view["report_versions"][0]["state"] == "ready"
    assert view["active_report_id"] == reporting.artifact_id(view, "report-v2.html")
    assert all((root / name).read_bytes() == data for name, data in original.items())
    assert {name: artifacts.sha256(root / name) for name in files} == hashes
    assert scheduler.submissions == 3 and agent.calls == 2
    assert len(store.rows("SELECT * FROM inbox WHERE kind='report_ready'")) == 1
    page = (root / "report-v2.html").read_text()
    assert page.index("Question and interpretation") < page.index("<table>")
    assert "Runnable provenance" not in page and "<pre>" not in page
    metric = json.loads(original["results.json"])["metrics"]["linear"]["mse"]
    assert f"{metric:.4f}" in page
    await restarted.close()


async def test_unknown_metric_report_fails_without_replacing_original(runtime):
    store, _, _, workflow = runtime
    eid = submit(store)
    await until(workflow, store, eid, lambda e: e["phase"] == "complete")
    doc = document()
    doc["blocks"][1]["rows"][0][1]["metric"]["variant"] = "invented"
    store.command(uuid4(), "present", {"document": doc}, eid=eid)
    await workflow.tick(force_observe=True)
    view = store.projection(eid)
    assert view["phase"] == "complete" and view["active_report_id"] is None
    assert view["report_versions"][0]["state"] == "failed"
    assert not (workflow.root(eid) / "report-v2.html").exists()
    await workflow.close()


def evidence_fixture(tmp_path):
    exp = {"id": str(uuid4()), "evidence_revision": 1, "contract": {"synthetic_demo": True}}
    data = {
        "results.json": {
            "metrics": [
                {"name": "mean", "variant": "a", "value": 2, "unit": "y"},
                {"name": "sd", "variant": "a", "value": 0.5, "unit": "y"},
            ]
        },
        "raw.json": {
            "points": [
                {"x": 2, "y": 6, "target": 5},
                {"x": 1, "y": 1, "target": 2},
                {"x": 2, "y": 4, "target": 5},
                {"x": 1, "y": 3, "target": 2},
            ]
        },
    }
    records = []
    for name, value in data.items():
        p = tmp_path / name
        p.write_text(json.dumps(value))
        records.append(
            {
                "id": reporting.artifact_id(exp, name),
                "name": name,
                "path": str(p),
                "size": p.stat().st_size,
                "sha256": artifacts.sha256(p),
                "media_type": "application/json",
            }
        )
    return exp, records, reporting.Evidence(exp, tmp_path, records)


def test_curve_reduction_and_ordering_are_derived_from_saved_records(tmp_path):
    exp, records, evidence = evidence_fixture(tmp_path)
    doc = ReportDocument.model_validate(
        {
            "title": "Curves",
            "standfirst": "Summary",
            "blocks": [
                {
                    "kind": "curve_plot",
                    "artifact_id": records[1]["id"],
                    "records_key": "points",
                    "x_field": "x",
                    "target_field": "target",
                    "curves": [{"label": "Mean", "y_field": "y"}],
                    "average_repetitions": True,
                    "caption": "Mean repeated prediction at each x.",
                    "x_label": "x",
                    "y_label": "y",
                    "alt": "Sorted mean curve.",
                }
            ],
        }
    )
    points, targets = reporting.curve_data(evidence, doc.blocks[0], doc.blocks[0].curves[0])
    assert points == [(1, 2), (2, 5)] and targets == [(1, 2), (2, 5)]
    _, names = reporting.build_report(exp, tmp_path, records, doc, 2)
    before = {name: artifacts.sha256(tmp_path / name) for name in names}
    reporting.build_report(exp, tmp_path, records, doc, 2)
    assert before == {name: artifacts.sha256(tmp_path / name) for name in names}
    manifest = json.loads((tmp_path / "report-v2.json").read_text())
    assert len(manifest["source_artifacts"]) == 2
    assert set(manifest["outputs"]) == set(names) - {"report-v2.json"}
    (tmp_path / "raw.json").write_text("{}")
    with pytest.raises(ValueError, match="source artifact changed"):
        reporting.build_report(exp, tmp_path, records, doc, 3)


def test_error_bars_require_explanation_nonnegative_values_and_matching_units(tmp_path):
    exp, records, evidence = evidence_fixture(tmp_path)
    block = {
        "kind": "metric_plot",
        "caption": "Comparison",
        "alt": "Comparison",
        "x_label": "x",
        "y_label": "y",
        "series": [
            {
                "label": "Mean",
                "points": [
                    {
                        "x": 1,
                        "metric": {"name": "mean", "variant": "a"},
                        "uncertainty": {"name": "sd", "variant": "a"},
                    }
                ],
            }
        ],
    }
    with pytest.raises(ValidationError, match="explicit description"):
        ReportDocument.model_validate({"title": "t", "standfirst": "s", "blocks": [block]})
    block["error_description"] = "±1 SD across repeated initializations."
    doc = ReportDocument.model_validate({"title": "t", "standfirst": "s", "blocks": [block]})
    reporting.validate_document(evidence, doc)
    evidence.metrics[("sd", "a")]["value"] = -0.5
    with pytest.raises(ValueError, match="nonnegative"):
        reporting.validate_document(evidence, doc)
    evidence.metrics[("sd", "a")].update(value=0.5, unit="seconds")
    with pytest.raises(ValueError, match="unit"):
        reporting.validate_document(evidence, doc)


async def test_report_text_is_escaped_and_unregistered_output_cannot_be_overwritten(runtime):
    store, _, _, workflow = runtime
    eid = submit(store)
    await until(workflow, store, eid, lambda e: e["phase"] == "complete")
    doc = document()
    doc["blocks"][0]["text"] = "<script>unsafe</script>"
    records = store.rows("SELECT * FROM artifacts WHERE experiment_id=?", (eid,))
    root = workflow.root(eid)
    reporting.build_report(store.experiment(eid), root, records, doc, 2)
    assert "<script>" not in (root / "report-v2.html").read_text()
    assert "&lt;script&gt;" in (root / "report-v2.html").read_text()
    doc["standfirst"] = "Changed plan"
    with pytest.raises(ValueError, match="different bytes"):
        reporting.build_report(store.experiment(eid), root, records, doc, 2)
    await workflow.close()


def test_present_api_queues_one_revision(config, runtime):
    store, _, _, workflow = runtime
    eid = submit(store)
    # No dispatch is needed to check command routing and receipt deduplication.
    store.update(eid, phase="complete")
    body = {"request_id": str(uuid4()), "document": document()}
    with TestClient(create_app(config, components=(None, store, workflow))) as client:
        headers = {"Authorization": "Bearer " + "t" * 40}
        url = f"/api/experiments/{eid}/reports"
        response = client.post(url, json=body, headers=headers)
        assert response.status_code == 200
        assert client.post(url, json=body, headers=headers).json() == response.json()
        assert len(store.projection(eid)["report_versions"]) == 1


def test_report_has_no_editorial_figure_or_block_count_limit():
    doc = {
        "title": "Full results",
        "standfirst": "Requested comparisons.",
        "blocks": [
            {
                "kind": "figure",
                "artifact_id": str(uuid4()),
                "alt": f"Comparison {i}",
                "caption": f"Comparison {i}",
            }
            for i in range(40)
        ],
    }
    assert len(ReportDocument.model_validate(doc).blocks) == 40


def test_discussion_output_manifest_is_not_limited_to_twenty_files():
    reply = DiscussionReply.model_validate(
        {
            "reply": "Prepared requested figures and scripts.",
            "generated_artifacts": [
                {"name": f"figure-{i}.svg", "sha256": "a" * 64, "evidence_ids": []} for i in range(40)
            ],
        }
    )
    assert len(reply.generated_artifacts) == 40


async def test_pending_report_does_not_block_later_builds(config, runtime):
    store, _, _, workflow = runtime
    eid = submit(store)
    await until(workflow, store, eid, lambda e: e["phase"] == "complete")
    # Keep the first report waiting on remote publication while another is queued.
    config.gateway.url = "https://gateway.example"
    store.command(uuid4(), "present", {"document": document()}, eid=eid)
    await workflow.advance_reports()
    first = store.one("SELECT * FROM report_versions WHERE experiment_id=? AND version=2", (eid,))
    assert first["state"] == "publishing"
    names = [
        store.one("SELECT name FROM artifacts WHERE id=?", (identity,))["name"]
        for identity in json.loads(first["artifact_ids"])
    ]
    revision = store.experiment(eid)["revision"]
    artifacts.register(
        store, eid, "publish", workflow.root(eid), names, report_version=2, report_name="report-v2.html"
    )
    assert store.experiment(eid)["revision"] == revision
    store.command(uuid4(), "present", {"document": document()}, eid=eid)
    await workflow.advance_reports()
    assert (
        store.one("SELECT state FROM report_versions WHERE experiment_id=? AND version=3", (eid,))["state"]
        == "publishing"
    )
    assert (
        store.one("SELECT state FROM report_versions WHERE experiment_id=? AND version=2", (eid,))["state"]
        == "publishing"
    )
    with store.tx() as db:
        db.execute("UPDATE artifacts SET remote_published=1 WHERE experiment_id=?", (eid,))
    await workflow.advance_reports()
    assert {
        r["state"] for r in store.rows("SELECT state FROM report_versions WHERE experiment_id=?", (eid,))
    } == {"ready"}
