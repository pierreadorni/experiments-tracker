"""Disposable intermediates stay available through repair, without evidence hashing."""

import copy
import time
from uuid import uuid4

import pytest
from test_generic import PLAN, SOURCE

from experimenttracker import artifacts
from experimenttracker.models import Implementation
from experimenttracker.store import encode

SEALED_CHECKPOINT = (
    '{"outputs":[{"name":"checkpoint","sha256":'
    '"bd83c6d7b441cafb97f02552040257fbbff7aa7b95b680d88cf47ee4ad2b1425","size":14}],'
    '"schema_version":1,"stage":"z_measure","storage":"executor-only"}'
)


@pytest.fixture
def temporary_run(runtime):
    store, scheduler, agent, workflow = runtime
    eid = store.command(
        uuid4(), "create", {"brief": "Synthetic cache lifecycle", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]
    # Explicit static legacy state: this fixture never advances the experiment, so the
    # saved contract must not depend on the global downgrade removing the flag.
    contract = store.experiment(eid)["contract"]
    contract.pop("execution_contract", None)
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
    root = workflow.root(eid)
    (root / "work").mkdir(parents=True)
    (root / "work/recipe.py").write_text(SOURCE)
    plan = Implementation.model_validate(PLAN)
    plan.stages[0].temporary_outputs = ["temporary/features"]
    plan.stages[0].local_outputs = ["checkpoint"]
    manifest = artifacts.freeze_implementation(root, plan, "")
    store.resolve_implementation(eid, manifest)
    (root / "temporary/features").mkdir(parents=True)
    (root / "temporary/features/data.pt").write_bytes(b"regenerable features")
    (root / "checkpoint").write_bytes(b"valuable model")
    (root / "raw.json").write_text('{"value":42}')
    with store.tx() as db:
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,state,job_id,script_hash,resources,"
            "snapshot_hash,created_at,execution_root,execution_manifest) "
            "VALUES(?,?,'z_measure','complete','12345','script','{}',?,?,?,?)",
            (str(uuid4()), eid, manifest["snapshot_hash"], time.time(), str(root), encode(manifest)),
        )
    return store, scheduler, agent, workflow, eid, root, manifest


@pytest.mark.parametrize(
    "paths",
    [
        ["../elsewhere"],
        ["/temporary/cache"],
        ["temporary/../snapshot"],
        ["cache"],
        ["temporary"],
        ["temporary/cache", "temporary/cache"],
        ["temporary/cache", "temporary/cache/nested"],
    ],
)
def test_temporary_declarations_reject_unsafe_or_ambiguous_ownership(paths):
    plan = copy.deepcopy(PLAN)
    plan["stages"][0]["temporary_outputs"] = paths
    with pytest.raises(ValueError):
        Implementation.model_validate(plan)


@pytest.mark.parametrize("retained", ["temporary/cache", "temporary/cache/checkpoint", "temporary"])
def test_temporary_declarations_cannot_delete_retained_outputs(retained):
    plan = copy.deepcopy(PLAN)
    plan["stages"][0].update(temporary_outputs=["temporary/cache"], local_outputs=[retained])
    with pytest.raises(ValueError, match="temporary output"):
        Implementation.model_validate(plan)


def test_cache_is_never_hashed_but_checkpoint_changes_are_detected(temporary_run, monkeypatch):
    _, _, _, _, _, root, manifest = temporary_run
    # A historical sealed record kept verbatim: verification must not need the writer.
    (root / "local-output-z_measure.json").write_text(SEALED_CHECKPOINT)
    digest = artifacts.sha256
    reads = []

    def tracked(path):
        reads.append(path)
        return digest(path)

    monkeypatch.setattr(artifacts, "sha256", tracked)
    artifacts.validate_stage(root, "z_measure", manifest)
    assert root / "checkpoint" in reads
    assert not any("temporary" in p.parts for p in reads)
    # The small scientific output still must be valid JSON.
    (root / "raw.json").write_text("invalid")
    with pytest.raises(ValueError):
        artifacts.validate_stage(root, "z_measure", manifest)
    (root / "raw.json").write_text('{"value":42}')
    (root / "checkpoint").write_bytes(b"changed")
    with pytest.raises(ValueError, match="checkpoint changed"):
        artifacts.validate_stage(root, "z_measure", manifest)
