"""Iteration contract acceptance with real synthetic processes; no live research."""

import json
from pathlib import Path

import pytest
from helpers import PLAN, SOURCE, IterationAgent, prepared  # noqa: F401 - re-exported for importing tests

from experimenttracker import artifacts, iteration, remote_worker
from experimenttracker.models import Implementation


def test_directory_registration_never_reads_or_enumerates_scientific_bytes(tmp_path, monkeypatch):
    root = tmp_path / "attempt"
    root.mkdir()
    (root / "checkpoint").mkdir()
    (root / "checkpoint/large").write_text("do not read")
    monkeypatch.setattr(Path, "iterdir", lambda *a: pytest.fail("directory enumeration"))
    monkeypatch.setattr(artifacts, "sha256", lambda *a: pytest.fail("scientific hash"))
    assert iteration.metadata(root, "checkpoint")["kind"] == "directory"
    with pytest.raises(ValueError, match="missing or inaccessible declared output"):
        iteration.metadata(root, "missing")
    monkeypatch.setattr(remote_worker, "digest", lambda *a: pytest.fail("remote scientific hash"))
    remote_worker.iteration_operation(
        "seal",
        root,
        "train",
        "intent",
        {
            "intent_id": "intent",
            "snapshot_hash": "control",
            "plan": {"stages": [{"id": "train", "outputs": [], "local_outputs": ["checkpoint"]}]},
        },
    )
    assert json.loads((root / "remote-receipt-train-intent.json").read_text())["provenance_bytes_read"] == 0


def test_graph_has_no_final_barrier_and_detects_disconnected_cycles():
    plan = {**PLAN, "stages": [{**s, "needs": []} for s in PLAN["stages"]]}
    Implementation.model_validate(plan)
    with pytest.raises(ValueError, match="cycle"):
        Implementation.model_validate(
            {
                **PLAN,
                "stages": [
                    {**s, "needs": ["evaluate"] if s["id"] == "train" else s["needs"]} for s in PLAN["stages"]
                ],
            }
        )


@pytest.mark.parametrize(
    "change, message",
    [
        ({"sample_count": 11}, "sample count"),
        (
            {"metrics": [{"name": "wrong", "variant": "synthetic", "value": 42, "unit": ""}]},
            "expected metrics",
        ),
        (
            {"metrics": [{"name": "score", "variant": "synthetic", "value": float("nan"), "unit": ""}]},
            "non-finite",
        ),
    ],
)
def test_bounded_results_reject_bad_counts_missing_metrics_and_nonfinite(tmp_path, change, message):
    value = {
        "schema_version": 1,
        "protocol": PLAN["protocol"],
        "sample_count": 12,
        "metrics": [{"name": "score", "variant": "synthetic", "value": 42, "unit": ""}],
    }
    value.update(change)
    (tmp_path / "metrics.json").write_text(json.dumps(value))
    with pytest.raises(ValueError, match=message):
        iteration.check_stage(tmp_path, "evaluate", {"plan": PLAN})


def test_output_declarations_cannot_replace_attempt_sources_or_bindings():
    for path in ("snapshot", "snapshot/package", "inputs.json", "remote-worker.py"):
        stages = [
            {**stage, "local_outputs": [path]} if stage["id"] == "train" else stage
            for stage in PLAN["stages"]
        ]
        with pytest.raises(ValueError, match="protected attempt"):
            Implementation.model_validate({**PLAN, "stages": stages})
