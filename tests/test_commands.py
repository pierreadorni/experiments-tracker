import json
from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient

from experimenttracker import creation_policy
from experimenttracker.api import create_app
from experimenttracker.config import GatewayConfig
from experimenttracker.gateway_client import GatewayClient
from experimenttracker.publish.commands import Services, dispatch


def services(store, workflow):
    return Services(
        store,
        projects=lambda: workflow.projects,
        memory=lambda: workflow.supervisor.memory,
        autopilot=lambda: workflow.research.service,
        maintenance=lambda: None,
    )


def create_command(request_id=None):
    request_id = request_id or str(uuid4())
    return {
        "request_id": request_id,
        "kind": "create",
        "target": "experiment:" + str(uuid4()),
        "payload": {
            "schema_version": 1,
            "request_id": request_id,
            "brief": "Compare shared synthetic models.",
            "recipe_id": "agent_experiment_v1",
        },
    }


def note_command(eid):
    request_id = str(uuid4())
    return {
        "request_id": request_id,
        "kind": "note",
        "target": "experiment:" + eid,
        "payload": {"schema_version": 1, "request_id": request_id, "text": "A note."},
    }


def run(svc, command, origin="gateway"):
    return dispatch(
        svc, command["request_id"], command["kind"], command.get("target"), command["payload"], origin=origin
    )


@pytest.fixture
def svc(runtime, monkeypatch):
    monkeypatch.setattr(creation_policy, "MODE", "legacy")
    store, _, _, workflow = runtime
    return services(store, workflow)


def test_replay_gives_one_effect_and_the_same_result(svc):
    command = create_command()
    first = run(svc, command)
    assert first["state"] == "applied"
    assert run(svc, command) == first
    assert run(svc, command, origin="local") == first
    assert len(svc.store.rows("SELECT * FROM commands WHERE request_id=?", (command["request_id"],))) == 1
    assert len(svc.store.rows("SELECT * FROM experiments")) == 1


def test_refusal_is_saved_with_an_inbox_notice_and_replays(svc):
    command = create_command()
    run(svc, command)
    eid = command["target"].split(":")[1]
    bad = note_command(eid)
    bad["payload"]["text"] = ""
    outcome = run(svc, bad)
    assert outcome["state"] == "rejected" and outcome["error"]
    saved = svc.store.one("SELECT * FROM rejected_gateway_commands WHERE request_id=?", (bad["request_id"],))
    assert saved["experiment_id"] == eid and saved["error"] == outcome["error"]
    assert svc.store.one("SELECT * FROM inbox WHERE id=? AND kind='command_rejected'", (bad["request_id"],))
    assert run(svc, bad) == outcome
    assert len(svc.store.rows("SELECT * FROM inbox WHERE kind='command_rejected'")) == 1


def test_unknown_kind_and_wrong_target_are_refused(svc):
    command = create_command()
    unknown = {**command, "request_id": str(uuid4()), "kind": "format_disk"}
    unknown["payload"] = {"request_id": unknown["request_id"]}
    assert run(svc, unknown)["state"] == "rejected"
    note = note_command(str(uuid4()))
    note["target"] = "status"
    assert run(svc, note)["state"] == "rejected"
    assert svc.store.rows("SELECT * FROM experiments") == []


def test_payload_identity_must_match_the_request(svc):
    command = create_command()
    command["payload"]["request_id"] = str(uuid4())
    assert run(svc, command)["state"] == "rejected"
    assert svc.store.rows("SELECT * FROM experiments") == []


def test_project_settings_for_an_unknown_project_is_refused(svc):
    request_id = str(uuid4())
    command = {
        "request_id": request_id,
        "kind": "project_settings",
        "target": "knowledge:nowhere",
        "payload": {"schema_version": 1, "request_id": request_id, "instructions": "Be brief."},
    }
    outcome = run(svc, command)
    assert outcome["state"] == "rejected" and "unknown" in outcome["error"]


def test_local_post_returns_a_terminal_receipt_and_get_serves_it(config, runtime, monkeypatch):
    monkeypatch.setattr(creation_policy, "MODE", "legacy")
    store, _, _, workflow = runtime
    headers = {"Authorization": "Bearer " + "t" * 40}
    command = create_command()
    with TestClient(create_app(config, components=(None, store, workflow))) as client:
        response = client.post("/api/commands", headers=headers, json=command)
        assert response.status_code == 200
        body = response.json()
        assert (
            body["state"] == "applied" and body["settled_at"] and body["request_id"] == command["request_id"]
        )
        assert (
            client.get("/api/commands/" + command["request_id"], headers=headers).json()["state"] == "applied"
        )
        bad = create_command()
        bad["payload"]["brief"] = ""
        refused = client.post("/api/commands", headers=headers, json=bad).json()
        assert refused["state"] == "rejected" and refused["error"]
        assert client.get("/api/commands/" + bad["request_id"], headers=headers).json()["state"] == "rejected"
        assert (
            client.post("/api/commands", headers=headers, json={**command, "kind": "Bad Kind"}).status_code
            == 422
        )


def client_for(config, store, tmp_path, handler):
    credential = tmp_path / "executor-token"
    credential.write_text("e" * 40)
    config.gateway = GatewayConfig(url="https://gateway.example", credential_file=credential)
    return GatewayClient(config, store, transport=httpx.MockTransport(handler))


async def test_relay_receipt_follows_the_commit_and_a_lost_receipt_redelivers(
    config, runtime, tmp_path, monkeypatch
):
    monkeypatch.setattr(creation_policy, "MODE", "legacy")
    store, _, _, _ = runtime
    command = create_command()
    receipts, seen_at_receipt = [], []

    async def handler(request):
        assert request.url.path.startswith("/api/executor/relay/")
        if request.url.path.endswith("/commands"):
            return httpx.Response(
                200, json={"schema_version": 1, "commands": [{**command, "received_at": 1.0}]}
            )
        seen_at_receipt.append(len(store.rows("SELECT * FROM commands")))
        receipts.append(json.loads(request.content))
        if len(receipts) == 1:
            raise httpx.ConnectError("receipt response lost")
        return httpx.Response(200, json={"schema_version": 1})

    client = client_for(config, store, tmp_path, handler)
    try:
        with pytest.raises(httpx.ConnectError):
            await client.relay_commands()
        assert seen_at_receipt == [1]  # committed before the receipt was sent
        await client.relay_commands()  # redelivery: answered from the saved outcome
        assert len(store.rows("SELECT * FROM experiments")) == 1
        assert receipts[0] == receipts[1] and receipts[0]["state"] == "applied"
    finally:
        await client.close()


async def test_relay_refusal_is_saved_before_its_receipt(config, runtime, tmp_path, monkeypatch):
    monkeypatch.setattr(creation_policy, "MODE", "legacy")
    store, _, _, _ = runtime
    created = create_command()
    eid = created["target"].split(":")[1]
    store.command(
        created["request_id"], "create", created["payload"] | {}, origin="gateway", experiment_id=eid
    )
    bad = note_command(eid)
    bad["payload"]["text"] = ""
    saved_first = []

    async def handler(request):
        if request.url.path.endswith("/commands"):
            return httpx.Response(200, json={"schema_version": 1, "commands": [bad]})
        saved_first.append(
            bool(
                store.one("SELECT 1 FROM rejected_gateway_commands WHERE request_id=?", (bad["request_id"],))
            )
            and bool(store.one("SELECT 1 FROM inbox WHERE id=?", (bad["request_id"],)))
        )
        body = json.loads(request.content)
        assert body["state"] == "rejected" and body["error"]
        return httpx.Response(200, json={"schema_version": 1})

    client = client_for(config, store, tmp_path, handler)
    try:
        await client.relay_commands()
        assert saved_first == [True]
    finally:
        await client.close()


async def test_old_and_relay_queues_give_one_effect(config, runtime, tmp_path, monkeypatch):
    monkeypatch.setattr(creation_policy, "MODE", "legacy")
    store, _, _, _ = runtime
    command = create_command()
    old = {
        "request_id": command["request_id"],
        "experiment_id": command["target"].split(":")[1],
        "executor_id": config.executor_id,
        "kind": "create",
        "payload": command["payload"],
    }
    receipts = []

    async def handler(request):
        path = request.url.path
        if path == "/api/executor/commands":
            return httpx.Response(200, json={"schema_version": 1, "commands": [old]})
        if path == "/api/executor/ack":
            return httpx.Response(200, json={"schema_version": 1})
        if path.endswith("/relay/commands"):
            return httpx.Response(200, json={"schema_version": 1, "commands": [command]})
        receipts.append(json.loads(request.content))
        return httpx.Response(200, json={"schema_version": 1})

    client = client_for(config, store, tmp_path, handler)
    try:
        await client.commands()
        await client.relay_commands()
        assert len(store.rows("SELECT * FROM experiments")) == 1
        assert len(store.rows("SELECT * FROM commands")) == 1
        assert receipts[0]["state"] == "applied"
    finally:
        await client.close()


async def test_relay_commands_404_is_quiet_and_backs_off(config, runtime, tmp_path):
    store, _, _, _ = runtime
    calls = []

    async def handler(request):
        calls.append(request.url.path)
        return httpx.Response(404)

    client = client_for(config, store, tmp_path, handler)
    try:
        await client.relay_commands()
        await client.relay_commands()
        assert calls == ["/api/executor/relay/commands"]
        assert client.operation_errors == {}
    finally:
        await client.close()
