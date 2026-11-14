import json
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import ValidationError

from experimenttracker.config import Config, ProjectConfig
from experimenttracker.execution_settings import ExecutionSettings
from experimenttracker.models import CreateExperiment
from experimenttracker.store import Conflict, Store


def register(config, store, tmp_path):
    checkout = tmp_path / "research"
    checkout.mkdir()
    project = ProjectConfig(id="anyup-rs", name="anyup-rs", checkout=checkout)
    config.projects.append(project)
    store.projects[project.id] = project
    return checkout


def create(store, **extra):
    payload = {"brief": "Measure a project-specific method.", "recipe_id": "agent_experiment_v1", **extra}
    return store.command(uuid4(), "create", payload)["experiment_id"]


def test_project_identity_survives_restart_and_receipts(config, runtime, tmp_path):
    store, _, _, _ = runtime
    register(config, store, tmp_path)
    request = str(uuid4())
    payload = {
        "brief": "Measure a project-specific method.",
        "recipe_id": "agent_experiment_v1",
        "project_id": "anyup-rs",
    }
    receipt = store.command(request, "create", payload)
    eid = receipt["experiment_id"]
    store.close()
    reopened = Store(config.state_dir / "workflow.sqlite")
    try:
        # A receipt can be replayed even if its checkout is no longer configured.
        assert reopened.command(request, "create", payload) == receipt
        assert reopened.projection(eid)["project_id"] == "anyup-rs"
        assert reopened.experiment(eid)["contract"]["project"]["name"] == "anyup-rs"
        with pytest.raises(Conflict, match="different command"):
            reopened.command(request, "create", {**payload, "project_id": "test-project"})
        assert len(reopened.rows("SELECT * FROM experiments")) == 1
    finally:
        reopened.close()


def test_cross_project_references_and_missing_checkouts_are_atomic(config, runtime, tmp_path):
    store, _, _, _ = runtime
    checkout = register(config, store, tmp_path)
    parent = create(store)
    store.update(parent, phase="complete", snapshot={"snapshot_hash": "test"})
    before = {
        table: len(store.rows(f"SELECT * FROM {table}"))
        for table in ("experiments", "commands", "events", "outbox")
    }
    for extra in (
        {"project_id": "anyup-rs", "reference_experiment_id": parent},
        {"project_id": "unknown"},
    ):
        with pytest.raises(Conflict):
            create(store, **extra)
    checkout.rmdir()
    with pytest.raises(Conflict, match="unavailable"):
        create(store, project_id="anyup-rs")
    assert before == {table: len(store.rows(f"SELECT * FROM {table}")) for table in before}


@pytest.mark.current_contract
def test_project_prompt_and_public_catalog(config, runtime, tmp_path):
    store, _, _, workflow = runtime
    checkout = register(config, store, tmp_path)
    workflow.projects.enqueue_settings(
        "anyup-rs",
        {
            "schema_version": 1,
            "request_id": str(uuid4()),
            "instructions": "Choose any capable available GPU before waiting for a busy A6000.",
        },
    )
    workflow.projects.settings_tick()
    eid = create(store, project_id="anyup-rs")
    current = store.experiment(eid)
    assert current["contract"]["execution_contract"] == 3
    prompt = workflow.prompt(current, "prepare")
    payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
    assert payload["project_context"]["checkout"] == str(checkout)
    assert payload["project_context"]["access"] == "read-write"
    assert (
        payload["project_instructions"] == "Choose any capable available GPU before waiting for a busy A6000."
    )
    assert "Implement the actual research brief without a model-family whitelist" in prompt
    assert next(p for p in workflow.status(None)["projects"] if p["id"] == "anyup-rs")["folder"] == str(
        checkout
    )
    assert "api_token_file" not in json.dumps(workflow.status(None))
    assert str(checkout) not in json.dumps(store.projection(eid))
    assert "project_id" not in CreateExperiment(
        request_id=uuid4(), brief="Legacy pending command"
    ).model_dump(mode="json", exclude_none=True)
    legacy = create(store)
    # A saved historical contract without project metadata still belongs to the test
    # project. It is static legacy state: no execution_contract, and it is never advanced.
    exp = store.experiment(legacy)
    exp["contract"].pop("project")
    exp["contract"].pop("execution_contract")
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (json.dumps(exp["contract"]), legacy))
    assert store.projection(legacy)["project_id"] == "test-project"


def test_project_config_requires_unique_ids_and_absolute_checkouts(config, tmp_path):
    with pytest.raises(ValidationError):
        ProjectConfig(id="anyup-rs", name="anyup-rs", checkout="relative")
    with pytest.raises(ValidationError):
        ProjectConfig(id="anyup-rs", name="anyup-rs")
    with pytest.raises(ValidationError):
        Config.model_validate({**config.model_dump(), "projects": [config.projects[0], config.projects[0]]})


def test_project_instructions_are_durable_revisioned_and_idempotent(config, runtime, tmp_path):
    store, _, _, workflow = runtime
    register(config, store, tmp_path)
    workflow.projects.reload()
    request_id = str(uuid4())
    payload = {
        "schema_version": 1,
        "request_id": request_id,
        "instructions": "Prefer capable available GPUs to reduce queue wait.",
    }
    waiting = workflow.projects.enqueue_settings("anyup-rs", payload)
    assert waiting["state"] == "waiting_for_executor"
    assert workflow.projects.instructions("anyup-rs") == ""
    workflow.projects.settings_tick()
    applied = workflow.projects.settings_receipt(request_id)
    assert applied["state"] == "applied"
    assert applied["settings"] == {
        "instructions": payload["instructions"],
        "revision": 1,
        "execution": ExecutionSettings().model_dump(),
    }
    assert workflow.projects.enqueue_settings("anyup-rs", payload) == applied
    workflow.projects.settings_tick()
    assert workflow.projects.catalog()[1]["revision"] == 1
    with pytest.raises(Conflict, match="different project settings"):
        workflow.projects.enqueue_settings("anyup-rs", {**payload, "instructions": "Changed"})
    next_payload = {**payload, "request_id": str(uuid4()), "instructions": ""}
    workflow.projects.enqueue_settings("anyup-rs", next_payload)
    workflow.projects.settings_tick()
    assert workflow.projects.settings("anyup-rs") == {
        "instructions": "",
        "revision": 2,
        "execution": ExecutionSettings().model_dump(),
    }


def project_request(workflow, *, name="Research", mode="new", folder=None, request_id=None):
    payload = {
        "schema_version": 1,
        "request_id": str(request_id or uuid4()),
        "name": name,
        "mode": mode,
        "allow_edits": True,
    }
    if folder:
        payload["folder"] = str(folder)
    return workflow.projects.enqueue(payload), payload


def test_new_project_intent_restart_and_exact_folder_recovery(config, runtime, tmp_path):
    from experimenttracker.projects import Projects

    store, _, _, workflow = runtime
    config.projects_root = tmp_path / "new-projects"
    receipt, payload = project_request(workflow)
    assert receipt["state"] == "waiting_for_executor"
    assert not config.projects_root.exists()
    workflow.projects.tick()
    done = workflow.projects.receipt(payload["request_id"])
    folder = config.projects_root / "Research"
    assert done["state"] == "applied" and folder.is_dir()
    original = folder.stat().st_ino
    # Saved filesystem identity and creation result survive the owning process.
    store.close()
    restarted = Store(config.state_dir / "workflow.sqlite")
    try:
        projects = Projects(config, restarted)
        assert projects.enqueue(payload) == done
        projects.tick()
        assert folder.stat().st_ino == original
        assert done["project_id"] in restarted.projects
        assert len(restarted.rows("SELECT * FROM projects")) == 1
    finally:
        restarted.close()


def test_existing_nonempty_symlink_folder_and_replacement_identity(config, runtime, tmp_path):
    store, _, _, workflow = runtime
    folder = tmp_path / "existing"
    folder.mkdir()
    source = folder / "module.py"
    source.write_text("original = True\n")
    alias = tmp_path / "alias"
    alias.symlink_to(folder, target_is_directory=True)
    receipt, payload = project_request(workflow, name="Existing", mode="existing", folder=alias)
    workflow.projects.tick()
    done = workflow.projects.receipt(payload["request_id"])
    assert done["state"] == "applied" and done["project"]["non_empty"]
    assert source.read_text() == "original = True\n"
    eid = create(store, project_id=receipt["project_id"])
    assert workflow.root(eid) == folder / ".experimenttracker" / "experiments" / eid
    assert workflow.project_context(store.experiment(eid))["access"] == "read-write"
    duplicate, duplicate_payload = project_request(workflow, name="Duplicate", mode="existing", folder=folder)
    workflow.projects.tick()
    assert workflow.projects.receipt(duplicate_payload["request_id"])["state"] == "rejected"
    assert len(store.rows("SELECT * FROM projects")) == 1
    # A new directory at the same pathname is not the saved project identity.
    folder.rename(tmp_path / "moved-existing")
    folder.mkdir()
    assert not next(p for p in workflow.projects.catalog() if p["id"] == receipt["project_id"])["available"]
    with pytest.raises(ValueError, match="identity changed"):
        workflow.root(eid)
    with pytest.raises(Conflict, match="identity"):
        create(store, project_id=receipt["project_id"])


def test_project_creation_never_overwrites_or_retries_an_uncertain_folder(config, runtime, tmp_path):
    store, _, _, workflow = runtime
    config.projects_root = tmp_path / "projects"
    folder = config.projects_root / "Research"
    folder.mkdir(parents=True)
    (folder / "existing.txt").write_text("preserve")
    _, payload = project_request(workflow)
    workflow.projects.tick()
    assert workflow.projects.receipt(payload["request_id"])["state"] == "rejected"
    assert (folder / "existing.txt").read_text() == "preserve"
    _, uncertain = project_request(workflow, name="Uncertain")
    target = config.projects_root / "Uncertain"
    with store.tx() as db:
        db.execute(
            "UPDATE project_requests SET state='creating',folder=? WHERE request_id=?",
            (str(target), uncertain["request_id"]),
        )
    workflow.projects.tick()
    assert workflow.projects.receipt(uncertain["request_id"])["state"] == "rejected"
    assert not target.exists()
    _, denied = project_request(workflow, name="Executor state", mode="existing", folder=config.state_dir)
    workflow.projects.tick()
    assert workflow.projects.receipt(denied["request_id"])["state"] == "rejected"


@pytest.mark.current_contract
async def test_agent_edits_selected_folder_and_freezes_project_run_evidence(config, runtime, tmp_path):
    from helpers import IterationAgent, approve_plan, until

    from experimenttracker.audit import verify_experiment
    from experimenttracker.codex_client import DemoAgent
    from experimenttracker.workflow import Workflow

    store, scheduler, _, old = runtime
    folder = tmp_path / "working-project"
    folder.mkdir()
    source = folder / "method.py"
    source.write_text("version = 1\n")
    receipt, payload = project_request(old, mode="existing", folder=folder)
    old.projects.tick()
    assert old.projects.receipt(payload["request_id"])["state"] == "applied"
    scratch = folder / "discussion-scratch.txt"

    class EditingAgent(IterationAgent):
        async def session(self, thread_id, workspace, *, writable=False, **kwargs):
            self.cwd, self.writable = workspace, writable
            return await DemoAgent.session(self, thread_id, workspace, writable=writable)

        async def begin(self, thread_id, prompt, schema, *, workspace=None, project_workspace=None, **kwargs):
            turn_payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
            phase = turn_payload["phase"]
            if phase == "plan":
                assert not self.writable
            elif phase == "prepare":
                assert self.writable and self.cwd == project_workspace == folder
                assert turn_payload["project_context"]["access"] == "read-write"
                # The actual file write proves this is a working folder, not context alone.
                source.write_text("version = 2\n")
            elif phase == "discuss":
                assert self.writable and self.cwd == project_workspace == folder
                assert not scratch.exists()
                scratch.write_text("Discussion notes for the saved synthetic measurement.\n")
                return await DemoAgent.begin(
                    self,
                    thread_id,
                    prompt,
                    schema,
                    workspace=workspace,
                    project_workspace=project_workspace,
                    **kwargs,
                )
            turn = await super().begin(
                thread_id, prompt, schema, workspace=workspace, project_workspace=project_workspace, **kwargs
            )
            if phase == "prepare":
                # The edited project source is declared, so each attempt freezes a bounded copy.
                self.results[(thread_id, turn)]["project_files"] = ["method.py"]
            return turn

    agent = EditingAgent()
    workflow = Workflow(config, store, scheduler, agent)
    eid = store.command(
        uuid4(),
        "create",
        {
            "brief": "Test an entirely new method and show its frontier.",
            "recipe_id": "agent_experiment_v1",
            "project_id": receipt["project_id"],
        },
    )["experiment_id"]
    try:
        assert store.experiment(eid)["contract"]["execution_contract"] == 3
        await approve_plan(workflow, store, eid)
        await until(workflow, store, eid, lambda exp: exp["phase"] == "complete")
        assert source.read_text() == "version = 2\n"
        assert workflow.root(eid).is_relative_to(folder)
        jobs = store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
        assert jobs
        for job in jobs:
            attempt = Path(job["execution_root"])
            assert attempt.is_relative_to(folder)
            assert (attempt / "snapshot/recipe.py").is_file()
            assert (attempt / "snapshot/project/method.py").read_text() == "version = 2\n"
        audit = verify_experiment(store, eid, workflow.root(eid))
        assert audit["artifacts_verified"] > 0
        assert audit["scope"] == (
            "small published object transport checks only; scientific data and reproducibility are not verified"
        )
        assert "scientific_checks" not in audit
        assert not (config.job_output_root / eid).exists()
        submissions = scheduler.submissions
        job_rows = store.rows(
            "SELECT intent_id,state FROM jobs WHERE experiment_id=? ORDER BY intent_id", (eid,)
        )
        artifact_rows = store.rows(
            "SELECT id,sha256,size FROM artifacts WHERE experiment_id=? ORDER BY id", (eid,)
        )
        request = store.command(
            uuid4(), "discuss", {"text": "Summarize the saved synthetic measurement."}, eid=eid
        )["request_id"]
        await until(
            workflow,
            store,
            eid,
            lambda exp: (
                store.one("SELECT state FROM discussions WHERE id=?", (request,))["state"] == "completed"
            ),
        )
        # The agent wrote the new scratch file; no jobs or published objects changed.
        assert scratch.read_text() == "Discussion notes for the saved synthetic measurement.\n"
        assert source.read_text() == "version = 2\n"
        assert scheduler.submissions == submissions
        assert (
            store.rows("SELECT intent_id,state FROM jobs WHERE experiment_id=? ORDER BY intent_id", (eid,))
            == job_rows
        )
        assert (
            store.rows("SELECT id,sha256,size FROM artifacts WHERE experiment_id=? ORDER BY id", (eid,))
            == artifact_rows
        )
        again = verify_experiment(store, eid, workflow.root(eid))
        assert again["artifacts_verified"] == audit["artifacts_verified"]
        assert "scientific_checks" not in again
    finally:
        await workflow.close()


@pytest.mark.current_contract
async def test_changed_project_folder_holds_without_turn_or_job(runtime, tmp_path):
    store, scheduler, agent, workflow = runtime
    folder = tmp_path / "held-project"
    folder.mkdir()
    receipt, payload = project_request(workflow, mode="existing", folder=folder)
    workflow.projects.tick()
    eid = create(store, project_id=receipt["project_id"])
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    saved = tmp_path / "saved-project"
    folder.rename(saved)
    try:
        await workflow.tick()
        exp = store.experiment(eid)
        assert exp["phase"] == "preparing" and exp["hold_reason"] == "project_unavailable"
        assert agent.calls == 0 and scheduler.submissions == 0
        saved.rename(folder)
        assert workflow.root(eid).is_relative_to(folder)
    finally:
        await workflow.close()


def test_delete_project_is_durable_and_preserves_files(config, runtime, tmp_path):
    from experimenttracker.projects import Projects

    store, _, _, workflow = runtime
    checkout = register(config, store, tmp_path)
    workflow.projects.reload()
    payload = {"request_id": str(uuid4()), "instructions": "", "delete": True}
    workflow.projects.enqueue_settings("anyup-rs", payload)
    workflow.projects.settings_tick()
    receipt = workflow.projects.settings_receipt(payload["request_id"])
    assert receipt["state"] == "applied"
    assert receipt["settings"]["deleted"] is True
    assert checkout.is_dir()
    assert "anyup-rs" not in store.projects
    Projects(config, store)
    assert "anyup-rs" not in store.projects
    assert workflow.projects.enqueue_settings("anyup-rs", payload) == receipt
    with pytest.raises(Conflict):
        create(store, project_id="anyup-rs")


def test_delete_project_rejects_visible_experiments_and_pending_agent_work(config, runtime):
    store, _, _, workflow = runtime
    create(store)
    payload = {"request_id": str(uuid4()), "instructions": "", "delete": True}
    workflow.projects.enqueue_settings("test-project", payload)
    workflow.projects.settings_tick()
    receipt = workflow.projects.settings_receipt(payload["request_id"])
    assert receipt["state"] == "rejected"
    assert "experiments" in receipt["error"]
    assert "test-project" in store.projects


def test_delete_project_blocks_uncertain_project_turn(config, runtime):
    store, _, _, workflow = runtime
    with store.tx() as db:
        db.execute(
            "INSERT INTO project_turns(id,project_id,payload,origin,state,created_at,updated_at) "
            "VALUES(?,?,'{}','local','uncertain',0,0)",
            (str(uuid4()), "test-project"),
        )
    payload = {"request_id": str(uuid4()), "instructions": "", "delete": True}
    workflow.projects.enqueue_settings("test-project", payload)
    workflow.projects.settings_tick()
    receipt = workflow.projects.settings_receipt(payload["request_id"])
    assert receipt["state"] == "rejected"
    assert "agent work" in receipt["error"]
    assert "test-project" in store.projects
