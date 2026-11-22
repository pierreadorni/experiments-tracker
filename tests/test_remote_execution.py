"""Separate local/remote trees and real rsync/job scripts, with a fake SSH/Slurm boundary.

No tests in this file submit actual cluster jobs or use managed inference.
"""

import hashlib
import json
import shlex
import sys
import time
from pathlib import Path
from uuid import uuid4

import pytest
from helpers import seed_saved_contract3, seed_saved_contract4
from pydantic import ValidationError

from experimenttracker import artifacts, creation_policy, iteration, remote
from experimenttracker.config import JobResources, ProjectConfig
from experimenttracker.execution_settings import ExecutionSettings
from experimenttracker.models import Implementation
from experimenttracker.remote import RemoteSlurmScheduler, sbatch_directives
from experimenttracker.slurm import command, wrapper
from experimenttracker.store import Store
from experimenttracker.workflow import Workflow

UP = "rsync -az --exclude=.experimenttracker/ --exclude=.git/ {local_project}/ {host}:{remote_project}/"
DOWN = "rsync -az --include=*/ --include=*.tfevents* --exclude=* {host}:{remote_run}/ {local_run}/"
SCRIPT = """#!/bin/bash
#SBATCH -c 1
#SBATCH --time 00:05:00
#SBATCH --ntasks=1
set -eu
mkdir -p "$EXPERIMENT_ROOT/checkpoint" "$EXPERIMENT_ROOT/tensorboard/measure"
printf checkpoint > "$EXPERIMENT_ROOT/checkpoint/weights.bin"
printf event > "$EXPERIMENT_ROOT/tensorboard/measure/events.out.tfevents.synthetic"
printf 'ET_PROGRESS {"stage":"measure","completed":1,"total":1,"timestamp":1000}\\n'
python3 - <<'INNER'
import json, os
from pathlib import Path
r = Path(os.environ["EXPERIMENT_ROOT"])
(r / "results.json").write_text(json.dumps({"schema_version":1,"protocol":"Transport acceptance","sample_count":1,"metrics":[{"name":"value","variant":"synthetic","value":42,"unit":""}]}))
INNER
"""


def settings(workflow, project, **values):
    request = str(uuid4())
    payload = {
        "request_id": request,
        "instructions": "Preserve held-out evaluation.",
        "execution": ExecutionSettings(**values).model_dump(),
    }
    workflow.projects.enqueue_settings(project, payload)
    workflow.projects.settings_tick()
    return payload


def prepared(runtime, tmp_path):
    store, _, _, wf = runtime
    wf.config.mode = "live"
    project = tmp_path / "source"
    project.mkdir(exist_ok=True)
    cfg = ProjectConfig(id="remote-project", name="Remote project", checkout=project)
    wf.config.projects.append(cfg)
    wf.projects.reload()
    values = settings(
        wf, cfg.id, context="jean-zay", remote_project=str(tmp_path / "remote"), rsync_up=UP, rsync_down=DOWN
    )
    eid = seed_saved_contract3(store, "Synthetic remote transport acceptance.", project_id=cfg.id)
    exp = store.experiment(eid)
    contract = {**exp["contract"], "execution": values["execution"]}
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (json.dumps(contract), eid))
    exp = store.experiment(eid)
    assert exp["contract"]["execution_contract"] == 3
    work = wf.working_directory(exp)
    work.mkdir(parents=True)
    (work / "train.sbatch").write_text(SCRIPT)
    root = wf.root(eid)
    root.mkdir(parents=True, exist_ok=True)
    plan = Implementation(
        schema_version=3,
        summary="Transport check",
        protocol="Transport acceptance",
        synthetic_demo=True,
        parameters_json="{}",
        files=["train.sbatch"],
        stages=[
            {
                "id": "measure",
                "title": "Measurement",
                "script": "train.sbatch",
                "outputs": ["results.json"],
                "local_outputs": ["checkpoint"],
                "needs": [],
                "resources": JobResources(partition="cpu_p1").model_dump(),
                "result_file": "results.json",
                "expected_metrics": ["value"],
                "sample_count": 1,
                "tensorboard": {"log_dir": "tensorboard/measure", "scalar_tag": "loss"},
            }
        ],
        figures=[],
    )
    manifest = artifacts.freeze_implementation(
        root, plan, python=Path(sys.executable), workspace=work, remote_execution=True
    )
    assert manifest["plan"]["schema_version"] == 3 and manifest["remote_execution"] is True
    store.resolve_implementation(eid, manifest)
    for stage in ("plan", "prepare"):
        store.stage(eid, stage, "complete")
    return wf, store.experiment(eid), values


class Machines:
    def __init__(self, monkeypatch, store):
        self.jobs = {}
        self.lost_submit = False
        self.fail_download = False
        self.calls = []
        self.store = store
        monkeypatch.setattr(remote, "command", self.run)

    async def run(self, *args, **kwargs):
        self.calls.append(args)
        if args[0] == "rsync":
            # Real local rsync between separate trees replaces only the transport.
            if self.fail_download and any(":/" in arg for arg in args[-2:-1]):
                raise RuntimeError("download unavailable")
            real = list(args)
            position = real.index("-e")
            del real[position : position + 2]
            real = [arg.split(":", 1)[1] if arg.startswith("jz:/") else arg for arg in real]
            return await command(*real, **kwargs)
        assert args[0] == "ssh"
        script = shlex.split(args[-1])[2]
        if "; done; " in script:
            script = script.split("; done; ", 1)[1]
        parts = shlex.split(script)
        if "sbatch" in parts:
            sbatch_script = script.split("sbatch", 1)[1]
            sent = shlex.split("sbatch" + sbatch_script)
            if "--test-only" not in sent:
                comment = sent[sent.index("--comment") + 1]
                if comment not in self.jobs.values():
                    self.jobs[str(101 + len(self.jobs))] = comment
            assert "--mem" not in parts
            if "--test-only" in parts:
                return ""
            intent = parts[parts.index("--comment") + 1]
            # Verify the durable boundary before simulating Slurm acceptance.
            assert (
                self.store.one("SELECT state FROM jobs WHERE intent_id=?", (intent,))["state"] == "submitting"
            )
            assert (
                self.store.one("SELECT state FROM execution_transfers WHERE direction='up'")["state"]
                == "complete"
            )
            intent = parts[parts.index("--comment") + 1]
            job_id = next(
                (job for job, saved in self.jobs.items() if saved == intent), str(101 + len(self.jobs))
            )
            root = Path(parts[parts.index("--chdir") + 1])
            stdout = await command("/bin/bash", parts[-1])
            (root / f"et-{intent}-{job_id}.out").write_text(stdout)
            (root / f"et-{intent}-{job_id}.err").write_text("")
            self.jobs[job_id] = intent
            if self.lost_submit:
                raise TimeoutError("response lost after Slurm acceptance")
            return job_id
        if "squeue" in parts:
            return ""
        if "sacct" in parts:
            if "--starttime" in parts:
                return "\n".join(f"{job}|et-{intent}|{intent}" for job, intent in self.jobs.items())
            return "\n".join(
                f"{job}|COMPLETED|0:0|2026-10-16T10:00:00|2026-10-16T10:01:00" for job in self.jobs
            )
        return await command("/bin/bash", "-c", script, **kwargs)


@pytest.mark.parametrize("mode", ["legacy", "candidate"])
def test_remote_c3_fixture_preserves_its_saved_route_under_each_create_policy(
    runtime, tmp_path, monkeypatch, mode
):
    monkeypatch.setattr(creation_policy, "MODE", mode)
    wf, exp, values = prepared(runtime, tmp_path)

    assert creation_policy.MODE == mode
    assert exp["contract"]["execution_contract"] == 3
    assert exp["contract"]["execution"]["context"] == "jean-zay"
    assert exp["contract"]["execution"] == values["execution"]


def test_project_context_is_saved_and_old_experiments_stay_local(runtime, monkeypatch):
    monkeypatch.setattr(creation_policy, "MODE", "legacy")
    store, _, _, wf = runtime
    before = store.command(
        uuid4(),
        "create",
        {
            "brief": "A local synthetic check",
            "recipe_id": "agent_experiment_v1",
        },
    )["experiment_id"]
    payload = settings(
        wf, "test-project", context="jean-zay", remote_project="$WORK/example", rsync_up=UP, rsync_down=DOWN
    )
    after = store.command(
        uuid4(),
        "create",
        {
            "brief": "A remote synthetic check",
            "recipe_id": "agent_experiment_v1",
        },
    )["experiment_id"]
    wf.projects.enqueue_settings("test-project", payload)
    wf.projects.settings_tick()
    assert wf.projects.settings("test-project")["revision"] == 1
    settings(wf, "test-project", context="acacia")
    assert store.experiment(before)["contract"]["execution"]["context"] == "acacia"
    assert store.experiment(after)["contract"]["execution"]["context"] == "jean-zay"
    reopened = Store(store.path)
    try:
        assert reopened.experiment(after)["contract"]["execution"]["rsync_up"] == UP
        assert not reopened.rows("PRAGMA foreign_key_check")
    finally:
        reopened.close()


@pytest.mark.parametrize(
    "value",
    [
        "ssh jz sbatch x",
        "rsync -az a b ; echo broken",
        "rsync --delete a b",
        "rsync -e evil a b",
        "rsync --rsync-path=evil a b",
    ],
)
def test_transfer_commands_reject_shell_and_destructive_options(value):
    with pytest.raises(ValidationError):
        ExecutionSettings(rsync_up=value)


def test_sbatch_resource_match_and_no_extra_gpu_allocation(tmp_path):
    resources = JobResources(
        partition="gpu_p13",
        account="uel@v100",
        constraint="v100",
        cpus_per_job=12,
        walltime_minutes_per_job=720,
        gpus_per_job=1,
    )
    source = tmp_path / "train.sbatch"
    source.write_text(
        "#!/bin/bash\n#SBATCH -c 12\n#SBATCH --gres gpu:1\n#SBATCH --time 12:00:00\n#SBATCH --account=uel@v100\n#SBATCH -C v100\n"
    )
    assert all(not line.startswith("#SBATCH") for line in sbatch_directives(source, resources))
    source.write_text("#SBATCH --gpus=2\n")
    with pytest.raises(ValueError, match="differs"):
        sbatch_directives(source, resources)
    source.write_text("#SBATCH --array=1-8\n")
    with pytest.raises(ValueError, match="unsupported"):
        sbatch_directives(source, resources)


@pytest.mark.usefixtures("contract_three_fixture")
async def test_remote_roundtrip_seals_checkpoints_without_downloading_bytes(runtime, tmp_path, monkeypatch):
    wf, exp, _ = prepared(runtime, tmp_path)
    assert exp["contract"]["execution_contract"] == 3
    machines = Machines(monkeypatch, wf.store)
    result = await wf.submit_prepared_stage(exp, "measure")
    assert result["job_id"] == "101"
    await wf.observe_jobs()
    job = wf.store.one("SELECT * FROM jobs")
    assert job["state"] == "complete"
    attempt = Path(job["execution_root"])
    remote_run = json.loads(job["execution_manifest"])["execution"]["layout"]["remote_run"]
    # The declared checkpoint stays on the cluster; only small outputs return.
    assert not (attempt / "checkpoint/weights.bin").exists()
    assert not (wf.root(exp["id"]) / "checkpoint").exists()
    assert (Path(remote_run) / "checkpoint/weights.bin").is_file()
    assert list((attempt / "tensorboard/measure").glob("events.out.tfevents.*"))
    refs = {
        row["name"]: row
        for row in wf.store.rows("SELECT * FROM output_refs WHERE intent_id=?", (job["intent_id"],))
    }
    assert {(name, row["kind"]) for name, row in refs.items()} == {
        ("checkpoint", "directory"),
        ("results.json", "file"),
    }
    assert all(row["context"] == "jean-zay" and row["host"] == "jz" for row in refs.values())
    assert refs["checkpoint"]["path"] == remote_run + "/checkpoint"
    assert refs["results.json"]["path"] == remote_run + "/results.json"
    assert (
        wf.store.one(
            "SELECT intent_id FROM stage_producers WHERE experiment_id=? AND stage_id='measure'",
            (exp["id"],),
        )["intent_id"]
        == job["intent_id"]
    )
    partial = wf.store.projection(exp["id"])["partial_results"]["tasks"]["measure"]["result"]
    assert [metric["value"] for metric in partial["metrics"]] == [42]
    assert (wf.root(exp["id"]) / "results.json").is_file()
    assert len(machines.jobs) == 1
    # Saved routing wins even after the project selection changes.
    settings(wf, "remote-project", context="acacia")
    assert isinstance(wf.scheduler_for(wf.store.experiment(exp["id"]), job), RemoteSlurmScheduler)


@pytest.mark.parametrize("mode", ["legacy", "candidate"])
async def test_c4_remote_result_transfer_replays_and_recovers_interrupted_download(
    runtime, tmp_path, monkeypatch, mode
):
    monkeypatch.setattr(creation_policy, "MODE", mode)
    store, _, _, wf = runtime
    wf.config.mode = "live"
    values = settings(
        wf, "test-project", context="jean-zay", remote_project="$WORK/example", rsync_up=UP, rsync_down=DOWN
    )
    eid = seed_saved_contract4(store, "Synthetic remote result delivery.")
    exp = store.experiment(eid)
    contract = {**exp["contract"], "execution": values["execution"]}
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (json.dumps(contract), eid))
    exp = store.experiment(eid)
    machines = Machines(monkeypatch, store)
    intent = str(uuid4())
    base = Path(wf.projects.base_root(exp["id"]))
    root = base / "attempts" / intent
    root.mkdir(parents=True)
    stage = "research-" + intent
    with wf.store.tx() as db:
        db.execute(
            "INSERT INTO stages(experiment_id,id,updated_at) VALUES(?,?,?)", (exp["id"], stage, time.time())
        )
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,state,job_id,scheduler_state,exit_code,script_hash,resources,snapshot_hash,created_at,ended_at,execution_root,execution_manifest) "
            "VALUES(?,?,?,'complete','12345','COMPLETED','0:0','x','{}','x',?,?,?,?)",
            (
                intent,
                exp["id"],
                stage,
                time.time(),
                time.time(),
                str(root),
                json.dumps(
                    {
                        "executor": "slurm",
                        "research_request": {"result_file": "metrics/result.json"},
                        "execution": {
                            "settings": exp["contract"]["execution"],
                            "layout": {
                                "remote_base": str(tmp_path / "remote"),
                                "remote_project": str(tmp_path / "remote" / "project"),
                                "remote_run": str(tmp_path / "remote" / "run"),
                                "local_project": str(wf.projects.base_root(exp["id"])),
                                "local_run": str(root),
                                "host": "jz",
                            },
                        },
                    }
                ),
            ),
        )
    job = wf.store.one("SELECT * FROM jobs WHERE intent_id=?", (intent,))
    # Result delivery remains under a submitted job; only its exact terminal
    # accounting tuple authorizes the result transfer.
    wf.store.db.execute("UPDATE jobs SET state='submitted' WHERE intent_id=?", (intent,))
    scheduler = wf.scheduler_for(exp, job)
    wf.store.db.execute("UPDATE jobs SET scheduler_state=NULL WHERE intent_id=?", (intent,))
    with pytest.raises(ValueError, match="successful terminal accounting"):
        await scheduler.fetch_result(job)
    wf.store.db.execute(
        "UPDATE jobs SET scheduler_state='COMPLETED',exit_code='0:0',ended_at=? WHERE intent_id=?",
        (time.time(), intent),
    )
    job = wf.store.one("SELECT * FROM jobs WHERE intent_id=?", (intent,))
    layout = await scheduler.resolve()
    remote_result = Path(layout["remote_run"]) / "metrics/result.json"
    remote_result.parent.mkdir(parents=True, exist_ok=True)
    remote_result.write_text('{"metrics": []}')
    machines.fail_download = True
    with pytest.raises(remote.TransferPending):
        await scheduler.fetch_result(job)
    transfer_id = hashlib.sha256((exp["id"] + ":" + intent + ":c4-result").encode()).hexdigest()
    assert (
        wf.store.one("SELECT state FROM execution_transfers WHERE id=?", (transfer_id,))["state"] == "pending"
    )
    assert not (root / "metrics/result.json").exists()
    machines.fail_download = False
    # Resume from the persisted transfer using a newly constructed scheduler, as
    # after an owner restart; successful scheduler accounting remains saved locally.
    scheduler = wf.scheduler_for(exp, job)
    target = root / "metrics/result.json"
    target.parent.mkdir()
    target.write_text("different")
    with pytest.raises(remote.TransferPending, match="different contents"):
        await scheduler.fetch_result(job)
    assert target.read_text() == "different"
    # A prior owner may have installed these bytes before committing completion.
    target.write_bytes(remote_result.read_bytes())
    await scheduler.fetch_result(job)
    calls = len(machines.calls)
    assert target.read_text() == '{"metrics": []}'
    await scheduler.fetch_result(job)
    assert len(machines.calls) == calls
    assert (
        wf.store.one("SELECT state FROM execution_transfers WHERE id=?", (transfer_id,))["state"]
        == "complete"
    )


@pytest.mark.usefixtures("contract_three_fixture")
async def test_lost_submission_reconciles_exactly_once_across_restart(runtime, tmp_path, monkeypatch):
    wf, exp, _ = prepared(runtime, tmp_path)
    assert exp["contract"]["execution_contract"] == 3
    machines = Machines(monkeypatch, wf.store)
    machines.lost_submit = True
    outcome = await wf.submit_prepared_stage(exp, "measure")
    assert outcome["state"] == "uncertain"
    assert len(machines.jobs) == 1
    # A second request cannot pass the saved dispatch boundary.
    assert (await wf.submit_prepared_stage(wf.store.experiment(exp["id"]), "measure"))["state"] == "uncertain"
    restarted = Workflow(wf.config, wf.store, wf.scheduler, wf.agent)
    await restarted.observe_jobs()
    assert wf.store.one("SELECT state,job_id FROM jobs") == {"state": "complete", "job_id": "101"}
    assert len(machines.jobs) == 1


@pytest.mark.usefixtures("contract_three_fixture")
async def test_completed_job_waits_for_transfer_and_polling_is_persisted(runtime, tmp_path, monkeypatch):
    wf, exp, _ = prepared(runtime, tmp_path)
    assert exp["contract"]["execution_contract"] == 3
    machines = Machines(monkeypatch, wf.store)
    await wf.submit_prepared_stage(exp, "measure")
    machines.fail_download = True
    await wf.observe_jobs()
    job = wf.store.one("SELECT * FROM jobs")
    assert job["scheduler_state"] == "COMPLETED" and job["state"] == "submitted"
    assert wf.store.experiment(exp["id"])["hold_reason"] == "results_transfer_pending"
    calls = len(machines.calls)
    restarted = Workflow(wf.config, wf.store, wf.scheduler, wf.agent)
    await restarted.observe_jobs()
    assert len(machines.calls) == calls
    assert not wf.store.one("SELECT 1 FROM stages WHERE id='measure' AND state='complete'")
    machines.fail_download = False
    with wf.store.tx() as db:
        db.execute("UPDATE jobs SET remote_polled_at=?", (time.time() - 601,))
    await restarted.observe_jobs()
    assert wf.store.one("SELECT state FROM jobs")["state"] == "complete"
    assert len(machines.jobs) == 1


async def test_remote_manifest_identity_guard_refuses_tampered_attempt_manifest(
    runtime, tmp_path, monkeypatch
):
    wf, exp, _ = prepared(runtime, tmp_path)
    assert exp["contract"]["execution_contract"] == 3
    machines = Machines(monkeypatch, wf.store)
    intent = str(uuid4())
    root, manifest, _ = iteration.stage_attempt(wf, exp, "measure", intent)
    resources = JobResources(partition="cpu_p1")
    scheduler = wf.scheduler_for(
        {**exp, "snapshot": manifest, "_attempt_id": intent, "_attempt_root": str(root)}
    )
    script = wrapper(
        root,
        "measure",
        intent,
        resources=resources,
        script="train.sbatch",
        project=root / "snapshot/project",
        iteration_mode=True,
    )
    await scheduler.preflight(intent, script, root, resources)
    remote_run = Path(scheduler.layout["remote_run"])
    worker = remote_run / "remote-worker.py"
    # The submitted attempt identity is part of the control payload.
    with pytest.raises(RuntimeError, match="attempt identity mismatch"):
        await command("/usr/bin/python3", str(worker), "verify", str(remote_run), "measure", str(uuid4()))
    saved = remote_run / "snapshot/manifest.json"
    saved.chmod(0o644)  # noqa: ASYNC240
    tampered = json.loads(saved.read_text())
    tampered["source_bytes"] = tampered["source_bytes"] + 1
    saved.write_text(json.dumps(tampered))  # noqa: ASYNC240
    with pytest.raises(RuntimeError, match="remote manifest identity mismatch"):
        await command("/bin/bash", str(remote_run / script.name))
    assert not machines.jobs
    assert not list(remote_run.glob("remote-receipt-*"))  # noqa: ASYNC240


@pytest.mark.usefixtures("contract_three_fixture")
async def test_agent_transfer_tool_is_durable_and_scoped(runtime, tmp_path):
    wf, exp, _ = prepared(runtime, tmp_path)
    assert exp["contract"]["execution_contract"] == 3
    action_id = str(uuid4())
    with wf.store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at) VALUES(?,?,'prepare',1,'running','test',?,?)",
            (action_id, exp["id"], time.time(), time.time()),
        )
    args = {
        "request_id": str(uuid4()),
        "remote_project": str(tmp_path / "other"),
        "rsync_up": UP,
        "rsync_down": DOWN,
    }
    receipt = await wf.controller.call(exp["id"], "set_project_execution", args)
    assert receipt["state"] == "applied"
    assert await wf.controller.call(exp["id"], "set_project_execution", args) == receipt
    assert wf.store.experiment(exp["id"])["contract"]["execution"]["remote_project"] == args["remote_project"]
    with wf.store.tx() as db:
        db.execute("UPDATE agent_actions SET phase='plan' WHERE id=?", (action_id,))
    with pytest.raises(ValueError, match="implementation"):
        await wf.controller.call(exp["id"], "set_project_execution", args)


@pytest.mark.usefixtures("contract_three_fixture")
async def test_remote_reads_are_batched_and_transport_failures_keep_observations_stale(
    runtime, tmp_path, monkeypatch
):
    wf, exp, _ = prepared(runtime, tmp_path)
    assert exp["contract"]["execution_contract"] == 3
    machines = Machines(monkeypatch, wf.store)
    await wf.submit_prepared_stage(exp, "measure")
    scheduler = wf.scheduler_for(exp)
    # A host's repeated queue/accounting reads use one snapshot for the interval.
    await scheduler.observe("101")
    await scheduler.observe("101")
    queue_calls = [args for args in machines.calls if args[0] == "ssh" and "squeue" in args[-1]]
    accounting_calls = [args for args in machines.calls if args[0] == "ssh" and "sacct " in args[-1]]
    assert len(queue_calls) == len(accounting_calls) == 1
    wf.remote_reads.clear()

    async def unavailable(*args, **kwargs):
        raise TimeoutError("SSH unavailable")

    monkeypatch.setattr(remote, "command", unavailable)
    await wf.observe_jobs()
    job = wf.store.one("SELECT * FROM jobs")
    assert job["state"] == "submitted" and job["observed_at"] is None
    assert job["remote_polled_at"]
    assert wf.store.experiment(exp["id"])["hold_reason"] == "executor_unavailable"
    # The persisted polling clock suppresses another attempt after restart.
    restarted = Workflow(wf.config, wf.store, wf.scheduler, wf.agent)
    await restarted.observe_jobs()
    assert len(machines.jobs) == 1


@pytest.mark.usefixtures("contract_three_fixture")
async def test_remote_successful_checkpoint_reuse_stays_remote(runtime, tmp_path, monkeypatch):
    wf, exp, _ = prepared(runtime, tmp_path)
    assert exp["contract"]["execution_contract"] == 3
    machines = Machines(monkeypatch, wf.store)
    await wf.submit_prepared_stage(exp, "measure")
    await wf.observe_jobs()
    job = wf.store.one("SELECT * FROM jobs")
    assert job["state"] == "complete"
    attempt = Path(job["execution_root"])
    manifest = json.loads(job["execution_manifest"])
    remote_run = manifest["execution"]["layout"]["remote_run"]
    # Each attempt binds only its own declared source copies.
    assert attempt.name == job["intent_id"]
    assert manifest["intent_id"] == job["intent_id"]
    assert manifest["source_mode"] == "declared-copies"
    assert set(manifest["files"]) == {"train.sbatch"}
    assert manifest["source_bytes"] == (attempt / "snapshot/train.sbatch").stat().st_size
    plan = Implementation.model_validate(exp["snapshot"]["plan"]).model_copy(
        update={
            "reuse_stages": ["measure"],
            "reuse_rationale": "Keep the measured synthetic stage; nothing to recompute.",
        }
    )
    reused = iteration.reuse(wf, wf.store.experiment(exp["id"]), plan)
    record = reused["measure"]
    assert record["intent_id"] == job["intent_id"]
    assert record["outputs"] == ["results.json"]
    assert record["rationale"] == plan.reuse_rationale
    assert set(record["output_ids"]) == {
        row["id"]
        for row in wf.store.rows("SELECT id FROM output_refs WHERE intent_id=?", (job["intent_id"],))
    }
    # Reuse reads the saved remote evidence directly; nothing is downloaded again.
    assert (Path(remote_run) / "checkpoint/weights.bin").read_bytes() == b"checkpoint"
    assert not (wf.root(exp["id"]) / "checkpoint").exists()
    assert (wf.root(exp["id"]) / "results.json").is_file()
    assert not any(
        args[0] == "rsync" and "checkpoint" in " ".join(str(arg) for arg in args) for args in machines.calls
    )


def test_browser_creation_preserves_the_selected_execution_snapshot(runtime, monkeypatch):
    monkeypatch.setattr(creation_policy, "MODE", "legacy")
    store, _, _, wf = runtime
    values = settings(
        wf, "test-project", context="jean-zay", remote_project="$WORK/example", rsync_up=UP, rsync_down=DOWN
    )
    settings(wf, "test-project", context="acacia")
    identity = str(uuid4())
    payload = {
        "brief": "A queued remote experiment",
        "recipe_id": "agent_experiment_v1",
        "execution": values["execution"],
    }
    result = store.command(identity, "create", payload)
    assert store.experiment(result["experiment_id"])["contract"]["execution"]["context"] == "jean-zay"
    assert store.command(identity, "create", payload) == result


@pytest.mark.usefixtures("contract_three_fixture")
async def test_remote_association_limit_waits_without_replanning_or_fast_retry(
    runtime, tmp_path, monkeypatch
):
    wf, exp, _ = prepared(runtime, tmp_path)
    assert exp["contract"]["execution_contract"] == 3
    machines = Machines(monkeypatch, wf.store)
    run = machines.run

    async def limited(*args, **kwargs):
        if args[0] == "ssh" and "--test-only" in args[-1]:
            raise RuntimeError("sbatch: error: AssocGrpSubmitJobsLimit")
        return await run(*args, **kwargs)

    monkeypatch.setattr(remote, "command", limited)
    result = await wf.submit_prepared_stage(exp, "measure")
    assert result["reason"] == "scheduler_admission_pending"
    assert not wf.store.rows("SELECT * FROM jobs")
    calls = len(machines.calls)
    await wf.submit_prepared_stage(wf.store.experiment(exp["id"]), "measure")
    assert len(machines.calls) == calls
    await wf.repair_failed_experiments()
    assert not wf.store.rows("SELECT * FROM agent_actions")


@pytest.mark.usefixtures("contract_three_fixture")
async def test_tensorboard_events_return_without_custom_download_template(runtime, tmp_path, monkeypatch):
    from tensorboard.compat.proto.event_pb2 import Event
    from tensorboard.summary.writer.event_file_writer import EventFileWriter

    from experimenttracker.slurm import Observation

    wf, exp, _ = prepared(runtime, tmp_path)
    assert exp["contract"]["execution_contract"] == 3
    machines = Machines(monkeypatch, wf.store)
    await wf.submit_prepared_stage(exp, "measure")
    job = wf.store.one("SELECT * FROM jobs")
    scheduler = wf.scheduler_for(exp, job)
    layout = await scheduler.resolve()
    writer = EventFileWriter(str(Path(layout["remote_run"]) / "tensorboard/measure"))
    writer.add_event(Event(wall_time=time.time(), step=1))
    writer.close()
    # No custom download template: the built-in filtered preview transfer returns the events.
    scheduler.settings.rsync_down = ""
    await scheduler.fetch(job, Observation("RUNNING"))
    local = Path(layout["local_run"]) / "tensorboard/measure"
    assert list(local.glob("events.out.tfevents.*"))
    calls = [args for args in machines.calls if "--max-size=512m" in args]
    assert calls and "--safe-links" in calls[0]
    assert len(machines.jobs) == 1


@pytest.mark.parametrize("operation", ["cleanup", "unknown"])
def test_remote_worker_rejects_retired_or_unknown_operations(operation):
    from experimenttracker.remote_worker import iteration_operation

    with pytest.raises(ValueError, match="unsupported remote worker operation"):
        iteration_operation(operation, None, "stage", "intent", {"intent_id": "intent"})
