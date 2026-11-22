import json
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from experimenttracker import creation_policy
from experimenttracker.api import create_app
from experimenttracker.config import ProjectConfig
from experimenttracker.publish.notifications import experiment_notification
from experimenttracker.publish.projections import Projections, digest, envelope, parse_key
from experimenttracker.workspaces import initialize_project

FIXTURES = Path(__file__).parents[1] / "gateway" / "test" / "fixtures"
HEADERS = {"Authorization": "Bearer " + "t" * 40}


def test_notification_equals_typescript_golden_file():
    inputs = json.loads((FIXTURES / "notification_inputs.json").read_text())
    expected = json.loads((FIXTURES / "notification_expected.json").read_text())
    assert expected.keys() == inputs["cases"].keys()
    for name, patch in inputs["cases"].items():
        got = experiment_notification({**inputs["base"], **patch})
        assert got.pop("targets") == [f"experiment:{inputs['base']['id']}"]
        assert got == expected[name], name


@pytest.fixture
def served(config, runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    checkout = tmp_path / "research"
    checkout.mkdir()
    initialize_project(checkout)
    project = ProjectConfig(id="research", name="Research", checkout=checkout, research_loop=True)
    config.projects.append(project)
    store.projects[project.id] = project
    monkeypatch.setattr(creation_policy, "MODE", "candidate")
    with TestClient(create_app(config, components=(None, store, workflow))) as client:
        yield client, store, tmp_path


def create(client, brief="Compare synthetic models safely."):
    body = {
        "schema_version": 1,
        "request_id": str(uuid4()),
        "brief": brief,
        "recipe_id": "agent_experiment_v1",
        "project_id": "research",
    }
    response = client.post("/api/experiments", json=body, headers=HEADERS)
    assert response.status_code == 202
    return response.json()["experiment_id"]


def test_local_projections_equal_builder_output_without_local_paths(served):
    client, store, tmp_path = served
    eid = create(client)
    assert client.get("/api/projections").status_code == 401
    listed = client.get("/api/projections", headers=HEADERS).json()["projections"]
    assert [p["key"] for p in listed] == ["projects", "status", "inbox", f"experiment:{eid}"]
    # The builder is the single source: a document equals what the route returns.
    one = client.get(f"/api/projections/experiment:{eid}", headers=HEADERS).json()
    built = Projections(store).build(f"experiment:{eid}")
    assert {k: v for k, v in one.items() if k != "schema_version"} == built
    assert built["revision"] == store.experiment(eid)["revision"]
    assert built["document"]["id"] == eid and "inbox" in built["document"]
    assert [p for p in listed if p["key"] == f"experiment:{eid}"] == [built]
    projects = client.get("/api/projections/projects", headers=HEADERS).json()
    assert [p["id"] for p in projects["document"]["projects"]] == ["test-project", "research"]
    assert client.get("/api/projections/status", headers=HEADERS).json()["document"]["executor_id"]
    assert client.get("/api/projections?prefix=experiment:&limit=1", headers=HEADERS).json()[
        "projections"
    ] == [built]
    for item in listed:
        assert str(tmp_path) not in json.dumps(item), item["key"]


def test_missing_deleted_and_malformed_keys_are_not_served(served):
    client, store, _ = served
    eid = create(client)
    assert client.get("/api/projections/experiment:not-a-uuid", headers=HEADERS).status_code == 404
    assert client.get(f"/api/projections/experiment:{uuid4()}", headers=HEADERS).status_code == 404
    assert client.get("/api/projections/other", headers=HEADERS).status_code == 404
    with store.tx() as db:
        db.execute("UPDATE experiments SET phase='failed' WHERE id=?", (eid,))
    deleted = {"schema_version": 1, "request_id": str(uuid4())}
    assert client.post(f"/api/experiments/{eid}/delete", json=deleted, headers=HEADERS).status_code == 200
    assert client.get(f"/api/projections/experiment:{eid}", headers=HEADERS).status_code == 404
    assert f"experiment:{eid}" not in Projections(store).keys()


def test_envelope_digest_and_aggregate_revisions(served):
    client, store, _ = served
    eid = create(client)
    projections = Projections(store, status={"agent_ready": True})
    inbox = projections.build("inbox", last_acked=2**60)
    assert inbox["revision"] == 2**60 + 1
    assert digest(inbox["document"]) == digest(json.loads(json.dumps(inbox["document"])))
    sent = envelope(projections.build(f"experiment:{eid}"))
    assert sent["schema_version"] == 1 and sent["notify"]["targets"] == [f"experiment:{eid}"]
    assert "notify" not in envelope(inbox)
    assert parse_key("status") == "status" and parse_key("experiment:x") is None
