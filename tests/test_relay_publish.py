import hashlib
import json
import shutil
import subprocess
from uuid import uuid4

import httpx
import pytest
from test_gateway_client import publication_client, publication_experiment

from experimenttracker import artifacts, clock
from experimenttracker.publish.projections import TOMBSTONE_DIGEST, wire, wire_digest
from experimenttracker.store import encode

SAMPLES = [
    {"b": 1.0, "a": [1e-7, 1.5e-5, 0.1, 1e21, 123456789012345680000, -2.5, 0, 2**53], "z": None},
    {"10": "x", "2": "y", "01": "z", "k": True, "4294967295": 1, "é": 'ünï   \x01 "q" \\ \u007f 😀'},
    {
        "nested": {"20": [{"b": 2, "a": 1}], "3": {}},
        "empty": [],
        "float": 3.14159,
        "big": 1.7976931348623157e308,
    },
]


def test_wire_equals_javascript_json_stringify_of_the_parsed_body():
    assert wire({"b": 1.0, "a": 1e-7}) == '{"a":1e-7,"b":1}'
    assert wire([1e21, 1.5e-5, 123456789012345680000.0]) == "[1e+21,0.000015,123456789012345680000]"
    assert wire({"b": 0, "10": 0, "2": 0}) == '{"2":0,"10":0,"b":0}'
    node = shutil.which("node")
    if not node:
        pytest.skip("node is required for the byte-identity check")
    script = (
        "const t=require('fs').readFileSync(0,'utf8').split('\\n').filter(Boolean);"
        "console.log(JSON.stringify(t.map(s=>JSON.stringify(JSON.parse(s))===s)))"
    )
    lines = "\n".join(wire(sample) for sample in SAMPLES)
    result = subprocess.run([node, "-e", script], input=lines, capture_output=True, text=True, check=True)
    assert json.loads(result.stdout) == [True] * len(SAMPLES)
    assert "\n" not in lines.replace("\n", "", len(SAMPLES) - 1)


class Worker:
    """A scripted Worker relay: revisions, idempotent projections, immutable blobs."""

    def __init__(self):
        self.projections, self.blobs, self.calls = {}, {}, []
        self.lose_key = None
        self.status = None  # force one response for projection PUTs

    def manifest(self):
        return {k: [v["revision"], v["digest"]] for k, v in self.projections.items()}

    def handler(self, request):
        path = request.url.path
        self.calls.append((request.method, path.removeprefix("/api/executor/relay/")))
        assert request.headers["x-executor-id"] == "test-executor"
        if self.status == 404:
            return httpx.Response(404, json={"detail": "Unknown endpoint"})
        if path.endswith("/revisions"):
            return httpx.Response(200, json={"schema_version": 1, "revisions": self.manifest()})
        if "/blobs/" in path:
            blob = request.url.path.rsplit("/", 1)[1]
            assert hashlib.sha256(request.content).hexdigest() == request.headers["x-sha256"]
            self.blobs[blob] = request.headers["x-owner-key"]
            sha, size = request.headers["x-sha256"], int(request.headers["x-size"])
            return httpx.Response(200, json={"schema_version": 1, "id": blob, "sha256": sha, "size": size})
        body = json.loads(request.content)
        assert request.content.decode() == wire(body), "the body is the deterministic serialization"
        key, revision = body["key"], body["revision"]
        document = body.get("document")
        digest = TOMBSTONE_DIGEST if document is None else wire_digest(document)
        prior = self.projections.get(key)
        if self.status:
            return httpx.Response(self.status, json={"detail": "stale revision"})
        if prior and (
            revision < prior["revision"] or (revision == prior["revision"] and digest != prior["digest"])
        ):
            return httpx.Response(409, json={"detail": "stale revision"})
        applied = not prior or revision > prior["revision"]
        if applied:
            self.projections[key] = {
                "revision": revision,
                "digest": digest,
                "document": document,
                "body": body,
            }
        if self.lose_key == key and applied:
            self.lose_key = None
            raise httpx.ReadTimeout("response lost after the Worker stored the revision")
        return httpx.Response(
            200, json={"schema_version": 1, "key": key, "revision": revision, "applied": applied}
        )


def relay_client(config, store, tmp_path, worker):
    client = publication_client(config, store, tmp_path, worker.handler)
    config.executor_id = "test-executor"
    return client


def with_status(client, config):
    state = str(config.state_dir)
    client.status_provider = lambda: {
        "agent_ready": True,
        "projects": [{"id": "p", "checkout": "/secret/checkout", "name": "P"}],
        "maintenance": {
            "current_commit": "abc",
            "operation": {"error": f"could not write {state}/maintenance/x.lock or /opt/other/place"},
            "observed_at": clock.now(),
        },
    }


async def test_backfill_blob_first_scrubbed_status_and_unchanged_views_stay_quiet(config, runtime, tmp_path):
    store, _, _, workflow = runtime
    eid = publication_experiment(store)
    root = workflow.root(eid)
    root.mkdir(parents=True)
    (root / "result.json").write_text('{"x": 1.0}')
    artifacts.register(store, eid, "prepare", root, ["result.json"])
    store.update(eid, error="Registration pending")
    worker = Worker()
    client = relay_client(config, store, tmp_path, worker)
    with_status(client, config)
    outbox = store.rows("SELECT id,delivered_at,attempts,error FROM outbox ORDER BY id")

    await client.publish_relay()
    assert set(worker.projections) == {
        "status",
        "projects",
        "inbox",
        f"experiment:{eid}",
        *(f"knowledge:{pid}" for pid in store.projects),
    }
    artifact_id = store.one("SELECT id FROM artifacts")["id"]
    assert worker.blobs == {artifact_id: f"experiment:{eid}"}
    order = [call[1] for call in worker.calls]
    assert order.index(f"blobs/{artifact_id}") < order.index(f"projections/experiment:{eid}")
    assert worker.projections[f"experiment:{eid}"]["body"]["notify"]["fingerprint"]
    status = json.dumps(worker.projections["status"]["document"])
    assert str(config.state_dir) not in status and "/opt/other" not in status
    assert "/secret/checkout" not in json.dumps(worker.projections["projects"]["document"])
    assert "<local>" in status
    assert store.rows("SELECT id,delivered_at,attempts,error FROM outbox ORDER BY id") == outbox

    # Nothing changed: no further PUT, even when the aggregate window opens again.
    before = list(worker.calls)
    client.relay_aggregates_at = 0
    client.status_provider = lambda: worker.projections["status"]["document"]
    await client.publish_relay()
    assert [c for c in worker.calls[len(before) :] if c[0] == "PUT"] == []
    await client.close()


async def test_lost_response_then_replay_is_idempotent(config, runtime, tmp_path):
    store, _, _, _ = runtime
    eid = publication_experiment(store)
    key = f"experiment:{eid}"
    worker = Worker()
    client = relay_client(config, store, tmp_path, worker)
    worker.lose_key = key
    await client.publish_relay()
    assert key in worker.projections and "relay:" + key in client.operation_errors
    assert list(client.operation_errors) == ["relay:" + key]

    # A replay of the exact same revision and bytes is accepted, not 409.
    body = worker.projections[key]["body"]
    response = await client.relay_request(
        "PUT", "/api/executor/relay/projections/" + key, content=wire(body).encode()
    )
    assert response["applied"] is False
    # The manifest already covers the revision, so the next tick sends nothing more.
    client.relay_key_retry_at.clear()
    puts = len([c for c in worker.calls if c[0] == "PUT"])
    await client.publish_relay()
    assert len([c for c in worker.calls if c[0] == "PUT"]) == puts
    await client.close()


async def test_stale_409_keeps_the_outbox_row_records_an_error_and_backs_off(config, runtime, tmp_path):
    store, _, _, _ = runtime
    eid = publication_experiment(store)
    key = f"experiment:{eid}"
    worker = Worker()
    client = relay_client(config, store, tmp_path, worker)
    await client.publish_relay()
    # The Worker holds a different document at this revision, but its manifest is stale.
    worker.projections[key]["digest"] = "other"
    worker.manifest = lambda: {}
    client.relay_key_retry_at.clear()
    puts = lambda: len([c for c in worker.calls if c == ("PUT", "projections/" + key)])  # noqa: E731
    before = puts()
    pending = store.one("SELECT COUNT(*) AS n FROM outbox WHERE delivered_at IS NULL")["n"]
    await client.publish_relay()
    assert puts() == before + 1 and "409" in client.operation_errors["relay:" + key]
    await client.publish_relay()
    assert puts() == before + 1, "backed off, not retried in a tight loop"
    assert store.one("SELECT COUNT(*) AS n FROM outbox WHERE delivered_at IS NULL")["n"] == pending
    await client.close()


async def test_a_404_means_relay_unsupported_quietly_and_the_old_path_is_unchanged(config, runtime, tmp_path):
    store, _, _, _ = runtime
    eid = publication_experiment(store)
    worker = Worker()
    worker.status = 404
    old = []

    def handler(request):
        if "/relay/" in request.url.path:
            return worker.handler(request)
        old.append(request.url.path)
        body = json.loads(request.content)
        return httpx.Response(
            200, json={"schema_version": 1, "accepted": [m["event_id"] for m in body["messages"]]}
        )

    client = publication_client(config, store, tmp_path, handler)
    config.executor_id = "test-executor"
    await client.publish_outbox()
    assert old == ["/api/executor/publish"]
    assert store.one("SELECT COUNT(*) AS n FROM outbox WHERE delivered_at IS NULL")["n"] == 0
    await client.isolated("relay", client.publish_relay)
    assert not client.operation_errors
    calls = len(worker.calls)
    await client.isolated("relay", client.publish_relay)
    assert len(worker.calls) == calls, "backs off instead of polling every tick"
    # The relay appears later: publication starts after the back-off.
    worker.status = None
    client.relay_retry_at = 0
    await client.publish_relay()
    assert f"experiment:{eid}" in worker.projections
    await client.close()


async def test_deleted_experiment_is_tombstoned_once(config, runtime, tmp_path):
    store, _, _, _ = runtime
    eid = publication_experiment(store)
    key = f"experiment:{eid}"
    worker = Worker()
    client = relay_client(config, store, tmp_path, worker)
    await client.publish_relay()
    with store.tx() as db:
        db.execute("UPDATE experiments SET deleted_at=? WHERE id=?", (clock.now(), eid))
    await client.publish_relay()
    assert (
        worker.projections[key]["digest"] == TOMBSTONE_DIGEST and worker.projections[key]["document"] is None
    )
    calls = len(worker.calls)
    await client.publish_relay()
    assert [c for c in worker.calls[calls:] if c[0] == "PUT"] == []
    await client.close()


async def test_blobs_follow_the_old_upload_gating_and_oversize_is_recorded(config, runtime, tmp_path):
    store, _, _, workflow = runtime
    eid = publication_experiment(store)
    contract = {**store.experiment(eid)["contract"], "execution_contract": 2}
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
    root = workflow.root(eid)
    root.mkdir(parents=True)
    names = ["ok.json", "old.json", "huge.json"]
    for name in names:
        (root / name).write_text("{}")
    artifacts.register(store, eid, "prepare", root, names)
    ids = {r["name"]: r["id"] for r in store.rows("SELECT id,name FROM artifacts")}
    with store.tx() as db:
        # Contracts 3 and 4 keep every revision; contract 2 withholds retired evidence.
        db.execute("UPDATE artifacts SET evidence_revision=-1 WHERE name='old.json'")
        db.execute("UPDATE artifacts SET size=? WHERE name='huge.json'", (11 * 1024 * 1024,))
        store.emit(db, eid, "experiment_updated")
    cancelled = publication_experiment(store)
    croot = workflow.root(cancelled)
    croot.mkdir(parents=True)
    (croot / "c.json").write_text("{}")
    artifacts.register(store, cancelled, "prepare", croot, ["c.json"])
    cancelled_id = store.one("SELECT id FROM artifacts WHERE name='c.json'")["id"]
    with store.tx() as db:
        db.execute("UPDATE experiments SET phase='cancelled' WHERE id=?", (cancelled,))
        store.emit(db, cancelled, "experiment_updated")
    worker = Worker()
    client = relay_client(config, store, tmp_path, worker)
    client.relay_status = lambda: _async({"projects": []})
    # Old-evidence rows are only withheld when the contract is not an all-revisions one.
    await client.publish_relay()
    assert ids["ok.json"] in worker.blobs
    assert cancelled_id not in worker.blobs and ids["huge.json"] not in worker.blobs
    assert "relay:blob:" + ids["huge.json"] in client.operation_errors
    assert f"experiment:{cancelled}" in worker.projections
    # A document that lists a retired-evidence artifact still gets no blob for it.
    old = store.one("SELECT * FROM artifacts WHERE name='old.json'")
    await client.relay_upload_blobs(
        f"experiment:{eid}", {"artifacts": [{"id": old["id"], "sha256": old["sha256"]}]}
    )
    assert old["id"] not in worker.blobs
    await client.close()


async def _async(value):
    return value


async def test_conversation_knowledge_reports_and_maintenance_publish_by_their_own_revision(
    config, runtime, tmp_path
):
    store, _, _, _ = runtime
    eid = publication_experiment(store)
    with store.tx() as db:
        db.execute("UPDATE experiments SET phase='complete' WHERE id=?", (eid,))
    store.command(uuid4(), "discuss", {"text": "Explain the result"}, eid=eid)
    key = f"conversation:{eid}:{store.conversation(eid)[0]['seq']:010d}"
    worker = Worker()
    client = relay_client(config, store, tmp_path, worker)
    with_status(client, config)
    client.maintenance_provider = lambda: type(
        "M", (), {"status": lambda self, w: {"observed_at": clock.now()}}
    )()
    pid = next(iter(store.projects))
    await client.publish_relay()
    assert {key, f"knowledge:{pid}", "maintenance"} <= set(worker.projections)
    assert worker.projections[key]["document"]["text"] == "Explain the result"
    assert "notify" not in worker.projections[key]["body"]
    first = {k: v["revision"] for k, v in worker.projections.items()}

    # Unchanged revisions are not sent again; a changed message and a new report version are.
    client.relay_aggregates_at = 0
    puts = len([c for c in worker.calls if c[0] == "PUT"])
    await client.publish_relay()
    sent = [c[1] for c in worker.calls[-5:] if c[0] == "PUT"]
    assert key not in sent and f"knowledge:{pid}" not in sent
    later = clock.now() + 5
    with store.tx() as db:
        db.execute("UPDATE discussions SET updated_at=? WHERE experiment_id=?", (later, eid))
        db.execute(
            "INSERT INTO project_reports VALUES(?,?,?,?,?,?)", (pid, 1, str(uuid4()), "{}", "{}", later)
        )
        db.execute(
            "INSERT OR IGNORE INTO project_knowledge(project_id,next_due,updated_at) VALUES(?,?,?)",
            (pid, later, later),
        )
        db.execute("UPDATE project_knowledge SET revision=revision+1 WHERE project_id=?", (pid,))
    await client.publish_relay()
    assert worker.projections[key]["revision"] > first[key]
    assert worker.projections[f"knowledge:{pid}"]["revision"] > first[f"knowledge:{pid}"]
    assert worker.projections[f"knowledge-report:{pid}:1"]["document"]["report"]["version"] == 1
    assert len([c for c in worker.calls if c[0] == "PUT"]) > puts

    # A deleted experiment takes its messages with it.
    with store.tx() as db:
        db.execute("UPDATE experiments SET deleted_at=? WHERE id=?", (clock.now(), eid))
    await client.publish_relay()
    assert worker.projections[key]["digest"] == TOMBSTONE_DIGEST
    await client.close()
