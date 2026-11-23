import json
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from experimenttracker import clock, creation_policy
from experimenttracker.api import create_app
from experimenttracker.config import ProjectConfig
from experimenttracker.engine.runner import Runner
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
    # The routes are under test, not the engine: a running loop advances the experiment
    # (revision, branch checkout) between the reads the tests compare.
    monkeypatch.setattr(Runner, "periods", lambda self: {})
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
    listed = client.get("/api/projections?limit=100", headers=HEADERS).json()["items"]
    assert sorted(p["key"] for p in listed) == sorted(
        ["projects", "status", "inbox", "maintenance", f"experiment:{eid}"]
        + [f"knowledge:{pid}" for pid in store.projects]
    )
    assert [p["key"] for p in listed] == sorted((p["key"] for p in listed), reverse=True)
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
    assert client.get("/api/projections?prefix=experiment:&limit=1", headers=HEADERS).json()["items"] == [
        built
    ]
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
    request_id = str(uuid4())
    deleted = {
        "request_id": request_id,
        "kind": "delete",
        "target": f"experiment:{eid}",
        "payload": {"schema_version": 1, "request_id": request_id},
    }
    assert client.post("/api/commands", json=deleted, headers=HEADERS).json()["state"] == "applied"
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


def test_conversation_knowledge_report_autopilot_and_maintenance_documents(served, monkeypatch):
    client, store, tmp_path = served
    eid = create(client)
    with store.tx() as db:
        db.execute("UPDATE experiments SET phase='complete' WHERE id=?", (eid,))
    first = store.command(uuid4(), "discuss", {"text": "Explain the setup"}, eid=eid)
    second = store.command(uuid4(), "discuss", {"text": "And the budget?"}, eid=eid)
    messages = store.conversation(eid)
    keys = [f"conversation:{eid}:{m['seq']:010d}" for m in messages]
    assert len(messages) == 2 and first["request_id"] != second["request_id"]
    projections = Projections(store, maintenance=lambda: {"available": False, "observed_at": 1.0})
    for key, message in zip(keys, messages, strict=True):
        built = projections.build(key)
        assert built["document"] == {"experiment_id": eid, **message}
        assert parse_key(key) == "conversation"
    # Paging is the hosted list: newest key first, resumed with the last key seen.
    page = client.get(f"/api/projections?prefix=conversation:{eid}:&limit=1", headers=HEADERS).json()["items"]
    assert [p["key"] for p in page] == [keys[1]]
    rest = client.get(
        f"/api/projections?prefix=conversation:{eid}:&before={keys[1]}", headers=HEADERS
    ).json()["items"]
    assert [p["key"] for p in rest] == [keys[0]]
    assert projections.build(f"conversation:{eid}:{99:010d}") is None
    # Knowledge keeps the old view and its own revision; report versions are immutable.
    knowledge = projections.build("knowledge:research")
    assert knowledge["document"]["project_id"] == "research" and knowledge["revision"] == 0
    assert projections.build("knowledge:unknown") is None
    assert projections.build("knowledge-report:research:1") is None
    with store.tx() as db:
        db.execute(
            "INSERT INTO project_reports VALUES(?,?,?,?,?,?)",
            ("research", 1, "{}", "{}", 1.0, "turn"),
        )
    report = projections.build("knowledge-report:research:1")
    assert report["revision"] == 1 and report["document"]["report"]["version"] == 1
    assert projections.revisions()["knowledge-report:research:1"] == 1
    # Maintenance is an aggregate; without an updater there is no document.
    assert projections.build("maintenance")["document"]["available"] is False
    assert Projections(store).build("maintenance") is None
    monkeypatch.setattr(Projections, "autopilot_view", lambda self, pid: {"policy": {}, "pid": pid})
    assert Projections(store, autopilot=lambda: None).build("autopilot:research")["document"]["pid"]
    for key in ("conversation:x:1", "knowledge-report:p:v", "autopilot:a b", "knowledge:"):
        assert parse_key(key) is None, key


def test_autopilot_notifications_name_the_newest_unresolved_attention_notice():
    view = {
        "policy": {"notifications": True},
        "experiments": [{"experiment_id": "e1"}],
        "notices": [
            {"id": "n2", "kind": "handoff", "text": "Paper ready", "resolved_at": None},
            {"id": "n1", "kind": "budget", "text": "Low", "resolved_at": None},
        ],
    }
    sent = envelope({"key": "autopilot:p-q", "revision": 1, "document": view})
    assert sent["notify"]["targets"] == ["experiment:e1"] and "n2" in sent["notify"]["fingerprint"]
    assert sent["notify"]["url"] == "/?project=p-q&view=memory" and sent["notify"]["alert"] is True
    assert "notify" not in envelope({"key": "autopilot:p", "revision": 1, "document": {**view, "policy": {}}})
    view["notices"][0]["resolved_at"] = 5
    assert "notify" not in envelope({"key": "autopilot:p", "revision": 1, "document": view})


def test_local_blob_alias_serves_autopilot_downloads(served):
    client, store, tmp_path = served
    pdf = tmp_path / "paper.pdf"
    pdf.write_bytes(b"%PDF synthetic")
    now = clock.now()
    run = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO autopilot_runs(id,project_id,status,created_at,updated_at) VALUES(?,?,?,?,?)",
            (run, "research", "active", now, now),
        )
        db.execute(
            "INSERT INTO autopilot_documents VALUES(?,?,?,?,?,?,?,?,?,?)",
            (run, "notes.md", 1, str(uuid4()), "autopilot", None, 1, "hello", "[]", now),
        )
        db.execute(
            "INSERT INTO autopilot_files VALUES(?,?,?,?,?,?,?)",
            (str(uuid4()), run, str(uuid4()), "paper.pdf", str(pdf), pdf.stat().st_size, now),
        )
    items = Projections(store, autopilot=lambda: object()).autopilot_items("research")
    by_kind = {i["kind"]: i for i in items.values()}
    doc = client.get("/api/blobs/" + by_kind["document"]["blob_id"], headers=HEADERS)
    assert doc.status_code == 200 and doc.text == "hello"
    assert client.get("/api/blobs/" + by_kind["file"]["blob_id"], headers=HEADERS).content == pdf.read_bytes()
    assert client.get("/api/blobs/" + by_kind["handoff"]["blob_id"], headers=HEADERS).content[:2] == b"PK"
    assert client.get("/api/blobs/" + str(uuid4()), headers=HEADERS).status_code == 404


def test_autopilot_notification_targets_fit_the_worker_limit():
    from experimenttracker.publish.notifications import NOTIFY_TARGETS_MAX, autopilot_notification

    view = {
        "policy": {"notifications": True},
        "notices": [{"id": "n1", "kind": "handoff", "text": "Ready", "resolved_at": None}],
        "experiments": [{"experiment_id": f"00000000-0000-0000-0000-{i:012d}"} for i in range(80)],
    }
    notify = autopilot_notification("p", view)
    assert len(notify["targets"]) == NOTIFY_TARGETS_MAX
