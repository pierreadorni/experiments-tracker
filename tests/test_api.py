from uuid import uuid4

from fastapi.testclient import TestClient

from experimenttracker.api import create_app
from experimenttracker.config import GatewayConfig
from experimenttracker.execution_settings import ExecutionSettings


def test_api_authentication_csrf_and_idempotency(config, runtime):
    store, _, _, workflow = runtime
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
            "recipe_id": "toy_compare_v1",
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
        assert client.post("/api/session", headers=headers).status_code == 200
        note = {"request_id": str(uuid4()), "text": "Preserve my text"}
        assert (
            client.post(
                f"/api/experiments/{eid}/notes", json=note, headers={"Origin": "https://evil.example"}
            ).status_code
            == 403
        )
        assert (
            client.post(
                f"/api/experiments/{eid}/notes", json=note, headers={"Origin": "http://testserver"}
            ).status_code
            == 200
        )
        assert client.get("/api/artifacts/../../etc/passwd").status_code in (400, 404)
        assert client.get("/api/artifacts/not-a-uuid").status_code == 422
        assert client.get("/api/experiments", headers=headers).headers["cache-control"] == "no-store"
        assert (
            client.get(f"/api/experiments/{eid}", headers=headers).json()["notes"][0]["author"]
            == "researcher"
        )


def test_stale_answer_rejected(config, runtime):
    store, _, _, workflow = runtime
    app = create_app(config, components=(None, store, workflow))
    with TestClient(app) as client:
        headers = {"Authorization": "Bearer " + "t" * 40}
        result = client.post(
            "/api/experiments",
            headers=headers,
            json={
                "request_id": str(uuid4()),
                "brief": "A safe synthetic comparison.",
                "recipe_id": "toy_compare_v1",
            },
        ).json()
        response = client.post(
            f"/api/experiments/{result['experiment_id']}/answers",
            headers=headers,
            json={"request_id": str(uuid4()), "pending_request_id": "old-request", "answer": "accept"},
        )
        assert response.status_code == 409


def test_project_settings_api_has_durable_receipt(config, runtime):
    store, _, _, workflow = runtime
    app = create_app(config, components=(None, store, workflow))
    with TestClient(app) as client:
        headers = {"Authorization": "Bearer " + "t" * 40}
        payload = {
            "schema_version": 1,
            "request_id": str(uuid4()),
            "instructions": "Prefer the next capable GPU over an occupied A6000.",
        }
        url = "/api/projects/test-project/settings"
        response = client.patch(url, json=payload, headers=headers)
        assert response.status_code == 202
        workflow.projects.settings_tick()
        receipt = client.get(f"/api/projects/settings/requests/{payload['request_id']}", headers=headers)
        assert receipt.status_code == 200
        assert receipt.json()["settings"] == {
            "instructions": payload["instructions"],
            "revision": 1,
            "execution": ExecutionSettings().model_dump(),
        }
        assert client.patch(url, json=payload, headers=headers).json() == receipt.json()
        assert (
            client.patch(url, json={**payload, "instructions": "changed"}, headers=headers).status_code == 409
        )
        status = client.get("/api/status", headers=headers).json()
        assert status["projects"][0]["instructions"] == payload["instructions"]


def test_executor_loop_failure_has_unhealthy_status(config, runtime):
    store, _, _, workflow = runtime

    async def fail(**kwargs):
        raise RuntimeError("simulated local loop failure")

    workflow.tick = fail
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

    import pytest

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
