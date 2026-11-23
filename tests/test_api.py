from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from experimenttracker.api import create_app
from experimenttracker.config import GatewayConfig, ProjectConfig
from experimenttracker.workspaces import initialize_project


def test_api_authentication_csrf_and_idempotency(config, runtime, tmp_path, monkeypatch):
    from experimenttracker import creation_policy

    store, scheduler, _, workflow = runtime
    checkout = tmp_path / "research"
    checkout.mkdir()
    initialize_project(checkout)
    project = ProjectConfig(id="research", name="Research", checkout=checkout, research_loop=True)
    config.projects.append(project)
    store.projects[project.id] = project
    monkeypatch.setattr(creation_policy, "MODE", "candidate")
    app = create_app(config, components=(None, store, workflow))
    # TestClient owns closing the store; fixture finalization tolerates close again.
    with TestClient(app) as client:
        assert client.get("/api/experiments").status_code == 401
        assert client.post("/api/experiments", json={}).status_code == 401
        headers = {"Authorization": "Bearer " + "t" * 40}
        body = {
            "schema_version": 1,
            "request_id": str(uuid4()),
            "brief": "Compare synthetic models safely.",
            "recipe_id": "agent_experiment_v1",
            "project_id": "research",
        }
        first = client.post("/api/experiments", json=body, headers=headers)
        assert first.status_code == 202
        receipt = client.get(f"/api/commands/{body['request_id']}", headers=headers)
        assert receipt.status_code == 200
        assert receipt.json()["state"] == "applied"
        assert "payload" not in receipt.json()
        assert client.post("/api/experiments", json=body, headers=headers).json() == first.json()
        assert (
            client.post(
                "/api/experiments", json={**body, "brief": "Another synthetic comparison."}, headers=headers
            ).status_code
            == 409
        )
        assert (
            client.post(
                "/api/experiments",
                json={**body, "request_id": str(uuid4()), "shell": "sbatch arbitrary"},
                headers=headers,
            ).status_code
            == 422
        )
        eid = first.json()["experiment_id"]
        assert store.experiment(eid)["contract"]["execution_contract"] == 4
        assert client.post("/api/session", headers=headers).status_code == 200
        note_id = str(uuid4())
        note = {
            "request_id": note_id,
            "kind": "note",
            "target": f"experiment:{eid}",
            "payload": {"schema_version": 1, "request_id": note_id, "text": "Preserve my text"},
        }
        assert (
            client.post("/api/commands", json=note, headers={"Origin": "https://evil.example"}).status_code
            == 403
        )
        assert (
            client.post("/api/commands", json=note, headers={"Origin": "http://testserver"}).status_code
            == 200
        )
        assert client.get("/api/artifacts/../../etc/passwd").status_code in (400, 404)
        assert client.get("/api/artifacts/not-a-uuid").status_code == 422
        assert client.get("/api/experiments", headers=headers).headers["cache-control"] == "no-store"
        assert (
            client.get(f"/api/experiments/{eid}", headers=headers).json()["notes"][0]["author"]
            == "researcher"
        )
    # Candidate-mode C4 creates are not advanced by the legacy stage engine.
    assert scheduler.submissions == 0


def test_create_explicit_contract4_and_invalid_discriminators(config, runtime, tmp_path):
    store, _, _, workflow = runtime
    checkout = tmp_path / "research"
    checkout.mkdir()
    initialize_project(checkout)
    project = ProjectConfig(id="research", name="Research", checkout=checkout, research_loop=True)
    config.projects.append(project)
    store.projects[project.id] = project
    headers = {"Authorization": "Bearer " + "t" * 40}
    with TestClient(create_app(config, components=(None, store, workflow))) as client:
        for invalid in (None, 3, 5, "4", True, 4.0):
            response = client.post(
                "/api/experiments",
                headers=headers,
                json={
                    "request_id": str(uuid4()),
                    "brief": "Explicit contract discriminator validation.",
                    "project_id": "research",
                    "execution_contract": invalid,
                },
            )
            assert response.status_code == 422
        response = client.post(
            "/api/experiments",
            headers=headers,
            json={
                "schema_version": 1,
                "request_id": str(uuid4()),
                "brief": "Explicit contract discriminator validation.",
                "project_id": "research",
                "execution_contract": 4,
            },
        )
        assert response.status_code == 202
        assert store.experiment(response.json()["experiment_id"])["contract"]["execution_contract"] == 4
        incompatible = client.post(
            "/api/experiments",
            headers=headers,
            json={
                "request_id": str(uuid4()),
                "brief": "Explicit contract must not silently downgrade.",
                "execution_contract": 4,
            },
        )
        assert incompatible.status_code == 409
        assert len(store.rows("SELECT * FROM experiments")) == 1


def test_stale_answer_rejected(config, runtime, tmp_path, monkeypatch):
    from experimenttracker import creation_policy

    store, scheduler, _, workflow = runtime
    checkout = tmp_path / "research"
    checkout.mkdir()
    initialize_project(checkout)
    project = ProjectConfig(id="research", name="Research", checkout=checkout, research_loop=True)
    config.projects.append(project)
    store.projects[project.id] = project
    monkeypatch.setattr(creation_policy, "MODE", "candidate")
    app = create_app(config, components=(None, store, workflow))
    with TestClient(app) as client:
        headers = {"Authorization": "Bearer " + "t" * 40}
        result = client.post(
            "/api/experiments",
            headers=headers,
            json={
                "request_id": str(uuid4()),
                "brief": "A safe synthetic comparison.",
                "recipe_id": "agent_experiment_v1",
                "project_id": "research",
            },
        ).json()
        assert store.experiment(result["experiment_id"])["contract"]["execution_contract"] == 4
        answer_id = str(uuid4())
        response = client.post(
            "/api/commands",
            headers=headers,
            json={
                "request_id": answer_id,
                "kind": "answer",
                "target": f"experiment:{result['experiment_id']}",
                "payload": {
                    "schema_version": 1,
                    "request_id": answer_id,
                    "pending_request_id": "old-request",
                    "answer": "accept",
                },
            },
        )
        assert response.json()["state"] == "rejected"
    assert scheduler.submissions == 0


def test_project_settings_command_has_durable_receipt(config, runtime):
    store, _, _, workflow = runtime
    app = create_app(config, components=(None, store, workflow))
    with TestClient(app) as client:
        headers = {"Authorization": "Bearer " + "t" * 40}
        request_id = str(uuid4())
        payload = {
            "request_id": request_id,
            "kind": "project_settings",
            "target": "knowledge:test-project",
            "payload": {
                "schema_version": 1,
                "request_id": request_id,
                "instructions": "Prefer the next capable GPU over an occupied A6000.",
            },
        }
        first = client.post("/api/commands", json=payload, headers=headers).json()
        assert first["state"] == "applied"
        assert client.post("/api/commands", json=payload, headers=headers).json()["result"] == first["result"]
        workflow.projects.settings_tick()
        changed = {**payload, "payload": {**payload["payload"], "instructions": "changed"}}
        assert client.post("/api/commands", json=changed, headers=headers).json()["state"] == "rejected"
        status = client.get("/api/status", headers=headers).json()
        assert status["projects"][0]["instructions"] == payload["payload"]["instructions"]


def test_executor_loop_failure_has_unhealthy_status(config, runtime):
    store, _, _, workflow = runtime

    async def fail(*args, **kwargs):
        raise RuntimeError("simulated local loop failure")

    workflow.step_control = fail
    app = create_app(config, components=(None, store, workflow))
    with TestClient(app) as client:
        response = client.get("/healthz")
        assert response.status_code == 503
        assert response.json() == {"ok": False}
        status = client.get("/api/status", headers={"Authorization": "Bearer " + "t" * 40}).json()
        assert status["last_tick"] is None
        assert status["executor_error"]


def test_gateway_startup_failure_closes_owned_runtime(config, runtime, tmp_path):
    import sqlite3

    store, _, agent, workflow = runtime
    closed = False
    original = agent.close

    async def close():
        nonlocal closed
        closed = True
        await original()

    agent.close = close
    config.gateway = GatewayConfig(url="https://gateway.test", credential_file=tmp_path / "missing-token")
    app = create_app(config, components=(None, store, workflow))
    with pytest.raises(FileNotFoundError), TestClient(app):
        pass
    assert closed
    with pytest.raises(sqlite3.ProgrammingError):
        store.rows("SELECT 1")
