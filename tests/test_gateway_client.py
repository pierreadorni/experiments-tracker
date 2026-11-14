import json
from uuid import uuid4

import httpx
import pytest

from experimenttracker.config import GatewayConfig
from experimenttracker.gateway_client import GatewayClient


async def test_lost_ack_redelivery_and_publication_retry(config, runtime, tmp_path):
    store, _, _, _ = runtime
    credential = tmp_path / "executor-token"
    credential.write_text("e" * 40)
    config.gateway = GatewayConfig(url="https://gateway.example", credential_file=credential)
    request_id, eid = str(uuid4()), str(uuid4())
    command = {
        "request_id": request_id,
        "experiment_id": eid,
        "executor_id": "acacia",
        "kind": "create",
        "payload": {
            "schema_version": 1,
            "request_id": request_id,
            "brief": "Compare shared synthetic models.",
            "recipe_id": "agent_experiment_v1",
        },
    }
    ack_attempts = 0
    publish_attempts = 0

    async def handler(request):
        nonlocal ack_attempts, publish_attempts
        if request.url.path == "/api/executor/commands":
            return httpx.Response(200, json={"schema_version": 1, "commands": [command]})
        if request.url.path == "/api/executor/ack":
            ack_attempts += 1
            if ack_attempts == 1:
                raise httpx.ConnectError("ACK response lost")
            return httpx.Response(200, json={"schema_version": 1, "acknowledged": [request_id]})
        if request.url.path == "/api/executor/publish":
            publish_attempts += 1
            if publish_attempts == 1:
                return httpx.Response(503)
            body = json.loads(request.content)
            return httpx.Response(
                200, json={"schema_version": 1, "accepted": [m["event_id"] for m in body["messages"]]}
            )
        return httpx.Response(404)

    client = GatewayClient(config, store, transport=httpx.MockTransport(handler))
    try:
        try:
            await client.commands()
        except httpx.ConnectError:
            pass
        await client.commands()
        assert len(store.rows("SELECT * FROM experiments")) == 1
        assert store.experiment(eid)["id"] == eid
        assert store.experiment(eid)["contract"]["execution_contract"] == 3
        await client.publish_outbox()
        pending = store.one("SELECT * FROM outbox")
        assert pending["delivered_at"] is None and pending["attempts"] == 1
        with store.tx() as db:
            db.execute("UPDATE outbox SET next_attempt_at=0")
        await client.publish_outbox()
        assert store.one("SELECT * FROM outbox")["delivered_at"] is not None
    finally:
        await client.close()


async def test_expired_command_refusal_is_durable_before_lost_ack(config, runtime, tmp_path):
    store, _, _, _ = runtime
    credential = tmp_path / "token"
    credential.write_text("e" * 40)
    config.gateway = GatewayConfig(url="https://gateway.example", credential_file=credential)
    eid = store.command(
        uuid4(), "create", {"brief": "Compare the fixed models.", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    request_id = str(uuid4())
    queued = {
        "request_id": request_id,
        "experiment_id": eid,
        "executor_id": "acacia",
        "kind": "answer",
        "payload": {
            "schema_version": 1,
            "request_id": request_id,
            "pending_request_id": "expired",
            "answer": "accept",
        },
    }
    attempts = 0

    async def handler(request):
        nonlocal attempts
        if request.url.path == "/api/executor/commands":
            return httpx.Response(200, json={"schema_version": 1, "commands": [queued]})
        if request.url.path == "/api/executor/reject":
            attempts += 1
            assert store.one("SELECT * FROM rejected_gateway_commands WHERE request_id=?", (request_id,))
            assert json.loads(request.content)["rejections"][0]["request_id"] == request_id
            if attempts == 1:
                raise httpx.ConnectError("refusal response lost")
            return httpx.Response(200, json={"schema_version": 1, "rejected": [request_id]})
        raise AssertionError("Rejected input must never be ACKed as applied")

    client = GatewayClient(config, store, transport=httpx.MockTransport(handler))
    try:
        try:
            await client.commands()
        except httpx.ConnectError:
            pass
        await client.commands()
        assert len(store.rows("SELECT * FROM rejected_gateway_commands")) == 1
        assert not store.one("SELECT * FROM commands WHERE request_id=?", (request_id,))
        assert attempts == 2
    finally:
        await client.close()


async def test_project_result_lost_ack_reuses_saved_folder_outcome(config, runtime, tmp_path):
    from experimenttracker.config import GatewayConfig
    from experimenttracker.gateway_client import GatewayClient

    store, _, _, _ = runtime
    token = tmp_path / "project-token"
    token.write_text("e" * 40)
    config.gateway = GatewayConfig(url="https://gateway.example", credential_file=token)
    config.projects_root = tmp_path / "new-projects"
    identity = str(uuid4())
    payload = {
        "schema_version": 1,
        "request_id": identity,
        "name": "Saved project",
        "mode": "new",
        "allow_edits": True,
    }
    attempts = 0
    outcomes = []

    async def handler(request):
        nonlocal attempts
        if request.url.path == "/api/executor/project-commands":
            return httpx.Response(
                200,
                json={
                    "schema_version": 1,
                    "commands": [
                        {
                            "request_id": identity,
                            "project_id": "project-" + identity.replace("-", ""),
                            "payload": payload,
                        }
                    ],
                },
            )
        if request.url.path == "/api/executor/project-results":
            outcomes.append(json.loads(request.content))
            attempts += 1
            if attempts == 1:
                raise httpx.ConnectError("Saved receipt response lost")
            return httpx.Response(200, json={"schema_version": 1, "accepted": [identity]})
        return httpx.Response(404)

    client = GatewayClient(config, store, transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(httpx.ConnectError):
            await client.project_commands()
        folder = config.projects_root / "Saved-project"
        original = folder.stat().st_ino
        await client.project_commands()
        assert folder.stat().st_ino == original
        assert outcomes[0] == outcomes[1]
        assert len(store.rows("SELECT * FROM projects")) == 1
        assert store.one("SELECT delivered_at FROM project_requests")["delivered_at"] is not None
    finally:
        await client.close()


async def test_project_settings_lost_ack_preserves_revision(config, runtime, tmp_path):
    store, _, _, workflow = runtime
    token = tmp_path / "settings-token"
    token.write_text("e" * 40)
    config.gateway = GatewayConfig(url="https://gateway.example", credential_file=token)
    identity = str(uuid4())
    payload = {"schema_version": 1, "request_id": identity, "instructions": "Prefer shorter GPU queues."}
    outcomes = []

    async def handler(request):
        if request.url.path == "/api/executor/project-settings-commands":
            return httpx.Response(
                200,
                json={
                    "schema_version": 1,
                    "commands": [{"request_id": identity, "project_id": "test-project", "payload": payload}],
                },
            )
        if request.url.path == "/api/executor/project-settings-results":
            outcomes.append(json.loads(request.content))
            if len(outcomes) == 1:
                raise httpx.ConnectError("Saved receipt response lost")
            return httpx.Response(200, json={"schema_version": 1, "accepted": [identity]})
        return httpx.Response(404)

    client = GatewayClient(config, store, transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(httpx.ConnectError):
            await client.project_settings_commands()
        assert workflow.projects.settings("test-project")["revision"] == 1
        await client.project_settings_commands()
        assert outcomes[0] == outcomes[1]
        assert workflow.projects.settings("test-project")["revision"] == 1
        assert store.one("SELECT delivered_at FROM project_settings_requests")["delivered_at"]
    finally:
        await client.close()


async def test_reserve_command_has_no_experiment_identity(config, runtime, tmp_path):
    store, _, _, _ = runtime
    token = tmp_path / "reserve-token"
    token.write_text("e" * 40)
    config.gateway = GatewayConfig(url="https://gateway.example", credential_file=token)
    identity = str(uuid4())
    queued = {
        "request_id": identity,
        "experiment_id": None,
        "executor_id": "acacia",
        "kind": "reserve",
        "payload": {
            "schema_version": 1,
            "request_id": identity,
            "five_hour_percent": 20,
            "weekly_percent": 15,
        },
    }

    async def handler(request):
        if request.url.path == "/api/executor/commands":
            return httpx.Response(200, json={"schema_version": 1, "commands": [queued]})
        if request.url.path == "/api/executor/ack":
            return httpx.Response(200, json={"schema_version": 1, "acknowledged": [identity]})
        return httpx.Response(404)

    client = GatewayClient(config, store, transport=httpx.MockTransport(handler))
    try:
        await client.commands()
        assert store.one("SELECT five_hour_percent,weekly_percent FROM quota_settings WHERE singleton=1") == {
            "five_hour_percent": 20,
            "weekly_percent": 15,
        }
        await client.commands()
        assert len(store.rows("SELECT * FROM commands WHERE kind='reserve'")) == 1
    finally:
        await client.close()


def publication_client(config, store, tmp_path, handler):
    token = tmp_path / "publication-token"
    token.write_text("p" * 40)
    config.gateway = GatewayConfig(url="https://gateway.example", credential_file=token)
    return GatewayClient(config, store, transport=httpx.MockTransport(handler))


def publication_experiment(store):
    eid = store.command(
        uuid4(),
        "create",
        {
            "brief": "Compare the fixed synthetic models.",
            "recipe_id": "agent_experiment_v1",
        },
    )["experiment_id"]
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    return eid


async def test_repair_publication_compacts_new_and_preexisting_outbox(config, runtime, tmp_path):
    from experimenttracker.store import encode, public_projection

    store, _, _, _ = runtime
    eid = publication_experiment(store)
    repair = {
        "revision": 2,
        "previous_revision": 1,
        "kind": "implementation",
        "reason": "automatic_repair_pending",
        "snapshot": {"files": {"recipe.py": "source-hash"}},
        "jobs": [{"execution_manifest": "x" * 1_100_000}],
    }
    contract = {**store.experiment(eid)["contract"], "auto_repair": repair, "repair_history": [repair]}
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
        store.emit(db, eid, "automatic_repair_queued")
    view = store.projection(eid)
    assert view["contract"]["auto_repair"]["revision"] == 2
    assert "jobs" not in view["contract"]["auto_repair"]
    assert "snapshot" not in view["contract"]["repair_history"][0]
    assert store.experiment(eid)["contract"] == contract

    # Simulate an oversized message persisted by the old owner before this fix.
    row = store.one("SELECT * FROM outbox ORDER BY rowid DESC LIMIT 1")
    message = {"schema_version": 1, "event_id": row["id"], "experiment": view}
    message["experiment"]["contract"] = contract
    message["experiment"]["events"].append(
        {
            "kind": "automatic_repair_started",
            "payload": encode({"snapshot": repair["snapshot"], "evidence_revision": 2}),
        }
    )
    saved = encode(message)
    with store.tx() as db:
        db.execute("UPDATE outbox SET payload=? WHERE id=?", (saved, row["id"]))
    deliveries = []

    async def handler(request):
        body = json.loads(request.content)
        assert len(request.content) < 1024 * 1024
        if request.url.path.endswith("/events"):
            return httpx.Response(
                200, json={"schema_version": 1, "accepted": [e["event_id"] for e in body["events"]]}
            )
        deliveries.extend(body["messages"])
        return httpx.Response(
            200, json={"schema_version": 1, "accepted": [m["event_id"] for m in body["messages"]]}
        )

    client = publication_client(config, store, tmp_path, handler)
    try:
        await client.publish_outbox()
        assert deliveries[0]["event_id"] == row["id"]
        assert deliveries[0]["experiment"] == public_projection(store.projection(eid))
        assert (
            store.one("SELECT payload,delivered_at FROM outbox WHERE id=?", (row["id"],))["payload"] == saved
        )
        assert store.one("SELECT delivered_at FROM outbox WHERE id=?", (row["id"],))["delivered_at"]
        assert store.experiment(eid)["contract"] == contract
    finally:
        await client.close()


async def test_current_failure_precedes_backlog_and_replay_survives_lost_ack(config, runtime, tmp_path):
    store, _, _, _ = runtime
    eid = publication_experiment(store)
    for i in range(505):
        store.update(eid, kind="job_progress", error=str(i))
    store.update(eid, phase="failed", hold_reason="compute_failed", error="saved failure")
    published = []
    receipts = set()
    lose_ack = True

    async def handler(request):
        nonlocal lose_ack
        body = json.loads(request.content)
        if request.url.path.endswith("/publish"):
            published.extend(body["messages"])
            assert body["messages"][0]["experiment"]["phase"] == "failed"
            return httpx.Response(
                200, json={"schema_version": 1, "accepted": [m["event_id"] for m in body["messages"]]}
            )
        assert request.url.path.endswith("/events")
        assert published
        assert len(body["events"]) <= 500
        assert all(set(e) == {"event_id", "experiment_id", "revision"} for e in body["events"])
        receipts.update(e["event_id"] for e in body["events"])
        if lose_ack:
            lose_ack = False
            raise httpx.ReadTimeout("response lost after commit")
        return httpx.Response(
            200, json={"schema_version": 1, "accepted": [e["event_id"] for e in body["events"]]}
        )

    client = publication_client(config, store, tmp_path, handler)
    with pytest.raises(httpx.ReadTimeout):
        await client.publish_outbox()
    assert len(published) == 1
    latest = store.one("SELECT id FROM events WHERE experiment_id=? ORDER BY seq DESC LIMIT 1", (eid,))["id"]
    assert store.one("SELECT delivered_at FROM outbox WHERE id=?", (latest,))["delivered_at"]
    assert store.one("SELECT COUNT(*) AS n FROM outbox WHERE delivered_at IS NULL")["n"] == 506
    await client.close()
    with store.tx() as db:
        db.execute("UPDATE outbox SET next_attempt_at=0")
    client = publication_client(config, store, tmp_path, handler)
    await client.publish_outbox()
    await client.publish_outbox()
    assert len(published) == 1
    assert len(receipts) == 506
    assert store.one("SELECT COUNT(*) AS n FROM outbox WHERE delivered_at IS NULL")["n"] == 0
    assert store.one("SELECT COUNT(*) AS n FROM events")["n"] == 507
    await client.close()


async def test_failed_bridge_operations_do_not_block_other_experiments_or_heartbeat(
    config, runtime, tmp_path
):
    store, _, _, _ = runtime
    bad, good = publication_experiment(store), publication_experiment(store)
    messages, heartbeats = [], []

    async def handler(request):
        if request.url.path.endswith("/project-commands"):
            raise httpx.ReadTimeout("project polling failed")
        if request.url.path.endswith("/project-settings-commands") or request.url.path.endswith("/commands"):
            return httpx.Response(200, json={"schema_version": 1, "commands": []})
        assert request.url.path.endswith("/publish")
        body = json.loads(request.content)
        if not body["messages"]:
            heartbeats.append(body["heartbeat"])
            return httpx.Response(200, json={"schema_version": 1, "accepted": []})
        if body["messages"][0]["experiment"]["id"] == bad:
            return httpx.Response(503)
        messages.extend(body["messages"])
        return httpx.Response(
            200, json={"schema_version": 1, "accepted": [m["event_id"] for m in body["messages"]]}
        )

    client = publication_client(config, store, tmp_path, handler)
    await client.once()
    assert [m["experiment"]["id"] for m in messages] == [good]
    assert heartbeats
    assert "project_commands" in client.operation_errors
    assert "projection:" + bad in client.operation_errors
    assert store.one("SELECT COUNT(*) AS n FROM outbox WHERE delivered_at IS NULL")["n"] == 1
    await client.close()


@pytest.mark.parametrize("execution_contract", [2, 3])
async def test_artifacts_wait_for_registration_and_failed_upload_cannot_starve_siblings(
    config, runtime, tmp_path, execution_contract
):
    from experimenttracker import artifacts

    store, _, _, workflow = runtime
    eid = publication_experiment(store)
    if execution_contract == 2:
        from experimenttracker.store import encode

        contract = {**store.experiment(eid)["contract"], "execution_contract": 2}
        with store.tx() as db:
            db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
    assert store.experiment(eid)["contract"]["execution_contract"] == execution_contract
    root = workflow.root(eid)
    root.mkdir(parents=True)
    names = ["bad.json", "good.json", "later.json", "retired.json"]
    for name in names:
        (root / name).write_text("{}")
    artifacts.register(store, eid, "prepare", root, names)
    store.db.execute("UPDATE artifacts SET evidence_revision=0 WHERE name='retired.json'")
    store.update(eid, error="Registration pending")
    uploads = []
    bad_id = store.one("SELECT id FROM artifacts WHERE name='bad.json'")["id"]

    async def handler(request):
        if request.method == "PUT":
            identity = request.url.path.rsplit("/", 1)[1]
            uploads.append(identity)
            if identity == bad_id:
                return httpx.Response(503)
            row = store.one("SELECT * FROM artifacts WHERE id=?", (identity,))
            return httpx.Response(
                200,
                json={
                    "schema_version": 1,
                    "artifact_id": identity,
                    "available": True,
                    "size": row["size"],
                    "sha256": row["sha256"],
                },
            )
        body = json.loads(request.content)
        items = body.get("messages", body.get("events", []))
        return httpx.Response(200, json={"schema_version": 1, "accepted": [x["event_id"] for x in items]})

    client = publication_client(config, store, tmp_path, handler)
    await client.upload_artifacts()
    assert not uploads
    await client.publish_outbox()
    await client.upload_artifacts()
    assert len(uploads) == 3
    assert store.one("SELECT remote_published FROM artifacts WHERE name='good.json'")["remote_published"] == 1
    assert (
        store.one("SELECT remote_published FROM artifacts WHERE name='later.json'")["remote_published"] == 1
    )
    assert (
        store.one("SELECT remote_published FROM artifacts WHERE name='retired.json'")["remote_published"] == 0
    )
    await client.publish_outbox()
    await client.upload_artifacts()
    if execution_contract == 3:
        assert len(uploads) == 4
        assert (
            store.one("SELECT remote_published FROM artifacts WHERE name='retired.json'")["remote_published"]
            == 1
        )
    else:
        assert len(uploads) == 3  # failed upload is backed off; siblings already succeeded
        assert (
            store.one("SELECT remote_published FROM artifacts WHERE name='retired.json'")["remote_published"]
            == 0
        )
    assert uploads.count(bad_id) == 1
    assert "artifact:" + bad_id in client.operation_errors
    await client.close()


async def test_partial_replay_ack_does_not_mark_any_unacknowledged_history_delivered(
    config, runtime, tmp_path
):
    store, _, _, _ = runtime
    eid = publication_experiment(store)
    store.update(eid, error="first")
    store.update(eid, phase="failed", error="latest")

    async def handler(request):
        body = json.loads(request.content)
        if request.url.path.endswith("/publish"):
            return httpx.Response(
                200, json={"schema_version": 1, "accepted": [m["event_id"] for m in body["messages"]]}
            )
        return httpx.Response(200, json={"schema_version": 1, "accepted": [body["events"][0]["event_id"]]})

    client = publication_client(config, store, tmp_path, handler)
    with pytest.raises(ValueError, match="exact replayed event identities"):
        await client.publish_outbox()
    assert store.one("SELECT COUNT(*) AS n FROM outbox WHERE delivered_at IS NULL")["n"] == 2
    assert all(
        r["attempts"] == 1 for r in store.rows("SELECT attempts FROM outbox WHERE delivered_at IS NULL")
    )
    await client.close()


async def test_project_memory_lost_ack_and_rejected_receipts_are_durable(config, runtime, tmp_path):
    from experimenttracker.knowledge import Knowledge

    store, _, _, _ = runtime
    command = {"request_id": str(uuid4()), "project_id": "test-project"}
    command["payload"] = {"request_id": command["request_id"], "kind": "chat", "text": "What next?"}
    attempts = 0

    async def handler(request):
        nonlocal attempts
        if request.url.path == "/api/executor/project-memory-commands" and request.method == "GET":
            return httpx.Response(200, json={"schema_version": 1, "commands": [command]})
        if request.url.path == "/api/executor/project-memory-commands":
            attempts += 1
            assert store.one("SELECT * FROM project_turns WHERE id=?", (command["request_id"],))
            if attempts == 1:
                raise httpx.ConnectError("ACK response lost")
            return httpx.Response(200, json={"schema_version": 1, "accepted": [command["request_id"]]})
        if request.url.path == "/api/executor/project-memory":
            value = json.loads(request.content)
            return httpx.Response(
                200,
                json={"schema_version": 1, "project_id": value["project_id"], "revision": value["revision"]},
            )
        raise AssertionError(request.url)

    client = publication_client(config, store, tmp_path, handler)
    with pytest.raises(httpx.ConnectError):
        await client.project_memory()
    await client.project_memory()
    assert len(store.rows("SELECT * FROM project_turns")) == 1
    assert store.one("SELECT delivered_at FROM project_memory_events")["delivered_at"]
    assert Knowledge(store).view("test-project")["messages"][0]["text"] == "What next?"
    await client.close()


@pytest.mark.parametrize("legacy_receipt", [False, True])
@pytest.mark.parametrize("execution_contract", [None, 3])
async def test_upload_uses_acknowledged_metadata_during_revision_churn(
    config, runtime, tmp_path, execution_contract, legacy_receipt
):
    from experimenttracker import artifacts
    from experimenttracker.retention import prune_batch

    store, _, _, workflow = runtime
    eid = publication_experiment(store)
    if execution_contract:
        from experimenttracker.store import encode

        contract = {**store.experiment(eid)["contract"], "execution_contract": execution_contract}
        with store.tx() as db:
            db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
            store.emit(db, eid, "contract_fixture_loaded")
    root = workflow.root(eid)
    root.mkdir(parents=True)
    (root / "known.json").write_text("{}")
    artifacts.register(store, eid, "evaluate", root, ["known.json"])
    revision = store.experiment(eid)["revision"]
    artifacts.register(store, eid, "evaluate", root, ["known.json"])
    assert store.experiment(eid)["revision"] == revision
    (root / "known.json").write_text('{"changed":true}')
    with pytest.raises(ValueError, match="immutable"):
        artifacts.register(store, eid, "evaluate", root, ["known.json"])
    assert store.experiment(eid)["revision"] == revision
    (root / "known.json").write_text("{}")
    uploads, sent = [], []

    async def handler(request):
        if request.method == "PUT":
            identity = request.url.path.rsplit("/", 1)[1]
            row = store.one("SELECT * FROM artifacts WHERE id=?", (identity,))
            uploads.append(row["name"])
            return httpx.Response(
                200,
                json={
                    "schema_version": 1,
                    "artifact_id": identity,
                    "available": True,
                    "sha256": row["sha256"],
                    "size": row["size"],
                },
            )
        body = json.loads(request.content)
        items = body.get("messages", body.get("events", []))
        sent.extend(body.get("messages", []))
        # Progress can commit while the gateway acknowledges the older projection.
        store.update(eid, error="new progress")
        return httpx.Response(200, json={"schema_version": 1, "accepted": [x["event_id"] for x in items]})

    client = publication_client(config, store, tmp_path, handler)
    await client.publish_outbox()
    if legacy_receipt:
        from experimenttracker.store import encode

        with store.tx() as db:
            db.execute("DELETE FROM artifact_registrations")
            db.execute("UPDATE outbox SET payload=? WHERE id=?", (encode(sent[0]), sent[0]["event_id"]))
    (root / "new.json").write_text("{}")
    artifacts.register(store, eid, "evaluate", root, ["new.json"])
    assert prune_batch(store)["pruned"] >= 0
    # Registration receipts survive a publisher restart.
    await client.close()
    client = publication_client(config, store, tmp_path, handler)
    await client.upload_artifacts()
    assert uploads == ["known.json"]
    await client.publish_outbox()
    await client.upload_artifacts()
    assert uploads == ["known.json", "new.json"]
    await client.close()


async def test_reference_outbox_stays_small_and_publishes_latest(config, runtime, tmp_path, monkeypatch):
    from experimenttracker.retention import prune_batch

    store, *_ = runtime
    eid = publication_experiment(store)
    before = store.one("SELECT count(*) AS n, sum(length(payload)) AS size FROM outbox")
    projection = store.projection
    with monkeypatch.context() as patch:

        def unexpected_projection(eid):
            raise AssertionError("emit must not build projections")

        patch.setattr(store, "projection", unexpected_projection)
        for i in range(100):
            store.update(eid, error=str(i))
    after = store.one("SELECT count(*) AS n, sum(length(payload)) AS size FROM outbox")
    assert after == {"n": before["n"] + 100, "size": before["size"] + 200}
    assert all(row["payload"] == "{}" for row in store.rows("SELECT payload FROM outbox"))
    latest = store.one("SELECT id FROM events ORDER BY seq DESC LIMIT 1")["id"]
    published, replayed = [], []

    def checked_projection(eid):
        assert store.db.in_transaction
        return projection(eid)

    monkeypatch.setattr(store, "projection", checked_projection)

    async def handler(request):
        body = json.loads(request.content)
        if request.url.path.endswith("/publish"):
            published.extend(body["messages"])
            items = body["messages"]
        else:
            replayed.extend(body["events"])
            items = body["events"]
        return httpx.Response(200, json={"schema_version": 1, "accepted": [x["event_id"] for x in items]})

    client = publication_client(config, store, tmp_path, handler)
    try:
        await client.publish_outbox()
        assert len(published) == 1
        assert published[0]["event_id"] == latest
        assert published[0]["experiment"]["error"] == "99"
        assert published[0]["experiment"]["revision"] == store.experiment(eid)["revision"]
        assert len(replayed) == after["n"] - 1
        assert all(set(e) == {"event_id", "experiment_id", "revision"} for e in replayed)
        assert not store.one("SELECT 1 FROM outbox WHERE delivered_at IS NULL")
        assert prune_batch(store)["pruned"] == 0
    finally:
        await client.close()


async def test_reference_outbox_skips_revision_changed_after_selection(
    config, runtime, tmp_path, monkeypatch
):
    store, *_ = runtime
    eid = publication_experiment(store)
    original_rows = store.rows
    changed = False

    def race(sql, args=()):
        nonlocal changed
        rows = original_rows(sql, args)
        if sql.startswith("SELECT o.* FROM experiments") and not changed:
            changed = True
            store.update(eid, error="newer revision")
        return rows

    monkeypatch.setattr(store, "rows", race)
    published = []

    async def handler(request):
        body = json.loads(request.content)
        items = body.get("messages", body.get("events", []))
        published.extend(body.get("messages", []))
        return httpx.Response(200, json={"schema_version": 1, "accepted": [x["event_id"] for x in items]})

    client = publication_client(config, store, tmp_path, handler)
    try:
        await client.publish_outbox()
        assert not published
        assert not store.one("SELECT 1 FROM outbox WHERE delivered_at IS NOT NULL")
        await client.publish_outbox()
        assert len(published) == 1
        assert published[0]["experiment"]["error"] == "newer revision"
        assert not store.one("SELECT 1 FROM outbox WHERE delivered_at IS NULL")
    finally:
        await client.close()


def test_reference_outbox_migration_preserves_saved_history(runtime):
    from experimenttracker.store import Store

    store, *_ = runtime
    eid = publication_experiment(store)
    with store.tx() as db:
        db.execute("DROP TABLE obligations")
        db.execute("DROP TABLE artifact_registrations")
        db.execute("DELETE FROM schema_version WHERE version>=23")
    before = {name: store.rows(f"SELECT * FROM {name}") for name in ("experiments", "events", "outbox")}
    migrated = Store(store.path)
    try:
        assert migrated.one("SELECT MAX(version) AS version FROM schema_version")["version"] == 24
        assert migrated.rows("SELECT * FROM artifact_registrations") == []
        assert migrated.rows("SELECT * FROM obligations") == []
        assert {name: migrated.rows(f"SELECT * FROM {name}") for name in before} == before
        reader = Store.read_only(store.path)
        try:
            assert reader.projection(eid) == migrated.projection(eid)
        finally:
            reader.close()
    finally:
        migrated.close()
