"""Separate local/remote trees and real rsync/job scripts, with a fake SSH/Slurm boundary.

No tests in this file submit actual cluster jobs or use managed inference.
"""

import json
import shlex
import sys
import time
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import ValidationError

from experimenttracker import artifacts, remote
from experimenttracker.config import JobResources, ProjectConfig
from experimenttracker.execution_settings import ExecutionSettings
from experimenttracker.models import Implementation
from experimenttracker.remote import RemoteSlurmScheduler, sbatch_directives
from experimenttracker.slurm import command, wrapper
from experimenttracker.store import Store
from experimenttracker.workflow import Workflow

UP = "rsync -az --exclude=.experimenttracker/ --exclude=.git/ {local_project}/ {host}:{remote_project}/"
DOWN = "rsync -az --include=*/ --include=*.tfevents* --exclude=* {host}:{remote_run}/ {local_run}/"


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
    eid = store.command(
        uuid4(),
        "create",
        {
            "brief": "Synthetic remote transport acceptance",
            "recipe_id": "agent_experiment_v1",
            "project_id": cfg.id,
        },
    )["experiment_id"]
    root = wf.root(eid)
    root.mkdir(parents=True)
    (project / "train.sbatch").write_text("""#!/bin/bash
#SBATCH -c 1
#SBATCH --time 00:05:00
#SBATCH --ntasks=1
set -eu
mkdir -p "$EXPERIMENT_ROOT/checkpoint" "$EXPERIMENT_ROOT/events"
printf checkpoint > "$EXPERIMENT_ROOT/checkpoint/weights.bin"
printf event > "$EXPERIMENT_ROOT/events/test.tfevents.test"
printf 'ET_PROGRESS {"stage":"measure","completed":1,"total":1,"timestamp":1000}\n'
python3 - <<'INNER'
import json, os
from pathlib import Path
r = Path(os.environ["EXPERIMENT_ROOT"])
(r / "results.json").write_text(json.dumps({"schema_version":1,"protocol":"Transport acceptance","metrics":[{"name":"value","variant":"synthetic","value":42,"unit":""}]}))
(r / "validation.json").write_text(json.dumps({"schema_version":1,"passed":True,"checks":[{"name":"test","passed":True,"detail":"Synthetic transport check"}]}))
INNER
""")
    plan = Implementation(
        schema_version=2,
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
                "outputs": ["results.json", "validation.json"],
                "local_outputs": ["checkpoint"],
                "needs": [],
                "resources": JobResources(partition="cpu_p1").model_dump(),
            }
        ],
        figures=[],
    )
    manifest = artifacts.freeze_implementation(
        root, plan, "", python=Path(sys.executable), workspace=project, remote_execution=True
    )
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
            assert "--mem" not in parts
            if "--test-only" in parts:
                assert not self.store.rows("SELECT * FROM jobs")
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
            job_id = str(101 + len(self.jobs))
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


def test_project_context_is_saved_and_old_experiments_stay_local(runtime):
    store, _, _, wf = runtime
    before = store.command(
        uuid4(), "create", {"brief": "A local synthetic check", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]
    payload = settings(
        wf, "test-project", context="jean-zay", remote_project="$WORK/example", rsync_up=UP, rsync_down=DOWN
    )
    after = store.command(
        uuid4(), "create", {"brief": "A remote synthetic check", "recipe_id": "agent_experiment_v1"}
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


async def test_remote_roundtrip_seals_checkpoints_without_downloading_bytes(runtime, tmp_path, monkeypatch):
    wf, exp, _ = prepared(runtime, tmp_path)
    machines = Machines(monkeypatch, wf.store)
    result = await wf.submit_prepared_stage(exp, "measure")
    assert result["job_id"] == "101"
    await wf.observe_jobs()
    job = wf.store.one("SELECT * FROM jobs")
    assert job["state"] == "complete"
    root = wf.root(exp["id"])
    assert not (root / "checkpoint/weights.bin").exists()
    assert (root / "events/test.tfevents.test").read_text() == "event"
    manifest = wf.store.experiment(exp["id"])["snapshot"]
    artifacts.validate_stage(root, "measure", manifest)
    provenance = artifacts.read_json(root / "local-output-measure.json", root)
    assert provenance["storage"] == "jean-zay" and provenance["outputs"][0]["files"][0]["size"] == 10
    assert (Path(provenance["root"]) / "checkpoint/weights.bin").is_file()
    assert len(machines.jobs) == 1
    # Saved routing wins even after the project selection changes.
    settings(wf, "remote-project", context="acacia")
    assert isinstance(wf.scheduler_for(wf.store.experiment(exp["id"]), job), RemoteSlurmScheduler)


async def test_lost_submission_reconciles_exactly_once_across_restart(runtime, tmp_path, monkeypatch):
    wf, exp, _ = prepared(runtime, tmp_path)
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


async def test_completed_job_waits_for_transfer_and_polling_is_persisted(runtime, tmp_path, monkeypatch):
    wf, exp, _ = prepared(runtime, tmp_path)
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


async def test_snapshot_tampering_is_detected_inside_job_allocation(runtime, tmp_path, monkeypatch):
    wf, exp, _ = prepared(runtime, tmp_path)
    machines = Machines(monkeypatch, wf.store)
    scheduler = wf.scheduler_for(exp)
    root = wf.root(exp["id"])
    script = wrapper(
        root, "measure", str(uuid4()), resources=JobResources(partition="cpu_p1"), script="train.sbatch"
    )
    await scheduler.preflight(str(uuid4()), script, root, JobResources(partition="cpu_p1"))
    remote_source = Path(scheduler.layout["remote_run"], "snapshot/train.sbatch")
    remote_source.chmod(0o644)  # noqa: ASYNC240
    remote_source.write_text("echo tampered")  # noqa: ASYNC240
    with pytest.raises(RuntimeError, match="remote source hash mismatch"):
        await command("/bin/bash", str(Path(scheduler.layout["remote_run"]) / script.name))
    assert not machines.jobs


async def test_agent_transfer_tool_is_durable_and_scoped(runtime, tmp_path):
    wf, exp, _ = prepared(runtime, tmp_path)
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


async def test_remote_reads_are_batched_and_transport_failures_keep_observations_stale(
    runtime, tmp_path, monkeypatch
):
    wf, exp, _ = prepared(runtime, tmp_path)
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


async def test_remote_successful_checkpoint_reuse_stays_remote_and_rechecks_bytes(
    runtime, tmp_path, monkeypatch
):
    wf, exp, _ = prepared(runtime, tmp_path)
    Machines(monkeypatch, wf.store)
    await wf.submit_prepared_stage(exp, "measure")
    await wf.observe_jobs()
    job = wf.store.one("SELECT * FROM jobs")
    previous = json.loads(job["execution_manifest"])["execution"]["layout"]["remote_run"]
    old = wf.store.experiment(exp["id"])["snapshot"]
    project = Path(wf.project_context(exp)["checkout"])
    next_root = tmp_path / "next-allocation"
    next_root.mkdir()
    plan = Implementation.model_validate(old["plan"])
    artifacts.freeze_implementation(
        next_root,
        plan,
        "",
        python=Path(sys.executable),
        workspace=project,
        remote_execution=True,
        reused={
            "measure": {
                "root": str(wf.root(exp["id"])),
                "remote_root": previous,
                "snapshot_hash": old["snapshot_hash"],
                "intent_id": job["intent_id"],
            }
        },
    )
    worker = Path(remote.__file__).with_name("remote_worker.py")
    await command(
        "env",
        "EXPERIMENT_PROJECT=" + str(project),
        sys.executable,
        str(worker),
        "verify",
        str(next_root),
        "measure",
        str(uuid4()),
    )
    assert (next_root / "checkpoint").is_symlink()
    assert (next_root / "checkpoint/weights.bin").read_bytes() == b"checkpoint"
    assert (Path(previous) / "checkpoint/weights.bin").read_bytes() == b"checkpoint"
    Path(previous, "checkpoint/weights.bin").write_text("altered")  # noqa: ASYNC240
    with pytest.raises(RuntimeError, match="reused remote evidence changed"):
        await command(
            "env",
            "EXPERIMENT_PROJECT=" + str(project),
            sys.executable,
            str(worker),
            "verify",
            str(next_root),
            "measure",
            str(uuid4()),
        )


def test_browser_creation_preserves_the_selected_execution_snapshot(runtime):
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


async def test_remote_association_limit_waits_without_replanning_or_fast_retry(
    runtime, tmp_path, monkeypatch
):
    wf, exp, _ = prepared(runtime, tmp_path)
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


async def test_tensorboard_events_return_without_custom_download_template(runtime, tmp_path, monkeypatch):
    from tensorboard.compat.proto.event_pb2 import Event
    from tensorboard.summary.writer.event_file_writer import EventFileWriter

    from experimenttracker.slurm import Observation

    wf, exp, _ = prepared(runtime, tmp_path)
    machines = Machines(monkeypatch, wf.store)
    await wf.submit_prepared_stage(exp, "measure")
    job = wf.store.one("SELECT * FROM jobs")
    manifest = json.loads(job["execution_manifest"])
    manifest["plan"]["stages"][0]["tensorboard"] = {"log_dir": "tensorboard/measure", "scalar_tag": "loss"}
    job["execution_manifest"] = json.dumps(manifest)
    scheduler = wf.scheduler_for(exp, job)
    layout = await scheduler.resolve()
    writer = EventFileWriter(str(Path(layout["remote_run"]) / "tensorboard/measure"))
    writer.add_event(Event(wall_time=time.time(), step=1))
    writer.close()
    scheduler.settings.rsync_down = ""
    await scheduler.fetch(job, Observation("RUNNING"))
    local = wf.root(exp["id"]) / "tensorboard/measure"
    assert list(local.glob("events.out.tfevents.*"))
    calls = [args for args in machines.calls if "--max-size=512m" in args]
    assert calls and "--safe-links" in calls[0]
    assert len(machines.jobs) == 1
