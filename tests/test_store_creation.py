from uuid import uuid4

import pytest
from legacy_fixture import load_legacy_completed

from experimenttracker.models import CreateExperiment
from experimenttracker.store import Conflict


def test_only_brief_driven_creates_are_new_and_replays_stay_idempotent(runtime):
    store, _, _, workflow = runtime
    request_id = uuid4()
    payload = {
        "schema_version": 1,
        "request_id": str(request_id),
        "brief": "Compare the retired fixed synthetic toy recipe.",
        "recipe_id": "toy_compare_v1",
    }
    with pytest.raises(Conflict, match="recipe is retired"):
        store.command(request_id, "create", payload)
    assert store.rows("SELECT * FROM experiments") == []
    assert store.rows("SELECT * FROM stages") == []
    assert store.rows("SELECT * FROM commands") == []
    assert store.rows("SELECT * FROM events") == []
    assert store.rows("SELECT * FROM outbox") == []
    with pytest.raises(ValueError):
        CreateExperiment.model_validate(payload)

    payload["recipe_id"] = "agent_experiment_v1"
    payload["request_id"] = str(uuid4())
    accepted = store.command(payload["request_id"], "create", payload)
    replay = store.command(payload["request_id"], "create", payload)
    assert replay == accepted
    exp = store.projection(accepted["experiment_id"])
    assert exp["contract"]["execution_contract"] == 3
    assert [stage["id"] for stage in exp["stages"]] == ["plan", "prepare", "interpret", "publish"]
    assert len(store.rows("SELECT * FROM experiments")) == 1
    assert len(store.rows("SELECT * FROM commands")) == 1


def test_saved_legacy_recipe_projection_remains_readable(runtime):
    store, _, _, workflow = runtime
    eid = load_legacy_completed(store, workflow)
    exp = store.projection(eid)
    assert exp["recipe_id"] == "toy_compare_v1"
    assert {"train_linear", "train_quadratic", "evaluate_and_plot"} <= {
        stage["id"] for stage in exp["stages"]
    }
