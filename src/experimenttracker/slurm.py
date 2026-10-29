import asyncio
import contextlib
import json
import os
import pwd
import shlex
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .config import JobResources, SlurmConfig


class SubmissionUncertain(RuntimeError):
    pass


class ResourceUnavailable(RuntimeError):
    """Read-only scheduler validation failed; no submission was attempted."""


@dataclass
class Observation:
    state: str
    exit_code: str | None = None
    started_at: float | None = None
    ended_at: float | None = None
    estimated_start_at: float | None = None
    pending_reason: str | None = None


def scheduler_time(value):
    try:
        stamp = datetime.fromisoformat(value.strip()).replace(tzinfo=UTC).timestamp()
        return stamp if stamp > 0 else None
    except (ValueError, TypeError):
        return None


async def command(*args, timeout=20, clean_slurm=False):
    process = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        **(
            {"env": {k: v for k, v in os.environ.items() if not k.startswith("SBATCH_")}}
            if clean_slurm
            else {}
        ),
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout)
    except (TimeoutError, asyncio.CancelledError):
        with contextlib.suppress(ProcessLookupError):
            process.kill()
        await process.wait()
        raise
    if process.returncode != 0:
        raise RuntimeError(
            f"{args[0]} failed ({process.returncode}): {stderr.decode(errors='replace')[:2000]}"
        )
    return stdout.decode().strip()


def wrapper(
    root: Path,
    stage: str,
    intent_id: str,
    python="/usr/bin/python3",
    *,
    resources=None,
    script=None,
    project=None,
    iteration_mode=False,
):
    path = root / f"{stage}-{intent_id}.sh"
    cpus = resources.cpus_per_job if resources else 1
    exports = {
        "OMP_NUM_THREADS": str(cpus),
        "OPENBLAS_NUM_THREADS": str(cpus),
        "MKL_NUM_THREADS": str(cpus),
        "EXPERIMENT_ROOT": str(root),
        "EXPERIMENT_SOURCE": str(root / "snapshot"),
        "PYTHONDONTWRITEBYTECODE": "1",
        "EXPERIMENT_PYTHON": str(python),
        "EXPERIMENT_STAGE": stage,
        "PYTHONUNBUFFERED": "1",
        "EXPERIMENT_PROJECT": str(project) if project else "",
    }
    if iteration_mode:
        exports["EXPERIMENT_INPUTS"] = str(root / "inputs.json")
        exports["PYTHONPATH"] = str(root / "snapshot") + ":" + str(project or root / "snapshot")
    invocation = (
        ["/bin/bash", str(root / "snapshot" / script)]
        if script
        else [str(python), str(root / "snapshot" / "recipe.py"), stage, "--output", str(root)]
    )
    directives = []
    if script and script.endswith(".sbatch"):
        from .remote import sbatch_directives

        directives = sbatch_directives(root / "snapshot" / script, resources)
    path.write_text(
        "#!/bin/bash\n"
        + "\n".join(directives)
        + "\nset -eu\n"
        + "".join(f"export {name}={shlex.quote(value)}\n" for name, value in exports.items())
        + "cd "
        + shlex.quote(
            str(project if script and script.endswith(".sbatch") and project else root / "snapshot")
        )
        + "\n"
        + "exec "
        + shlex.join(invocation)
        + "\n"
    )
    path.chmod(0o444)
    return path


class SlurmScheduler:
    def __init__(self, config: SlurmConfig):
        self.config = config
        self.user = str(os.getuid())

    async def command(self, *args, **kwargs):
        return await command(*args, **kwargs)

    async def discover(self):
        probes = {
            "nodes": ("sinfo", "-N", "--noheader", "--format", "%N|%P|%t|%c|%m|%G|%f"),
            "partitions": ("scontrol", "show", "partition"),
            "allocations": ("scontrol", "show", "nodes"),
            "associations": (
                "sacctmgr",
                "-nP",
                "show",
                "assoc",
                "where",
                f"user={getattr(self, 'association_user', None) or pwd.getpwuid(os.getuid()).pw_name}",
                "format=Account,Partition,QOS,DefaultQOS",
            ),
        }
        results = await asyncio.gather(
            *(self.command(*args) for args in probes.values()), return_exceptions=True
        )
        snapshot = {"observed_at": time.time(), "source": "live Slurm", "errors": {}}
        for name, result in zip(probes, results, strict=True):
            if isinstance(result, BaseException):
                snapshot["errors"][name] = str(result)
                snapshot[name] = ""
            else:
                snapshot[name] = result[:65536]
        if not snapshot["nodes"] or not snapshot["partitions"]:
            raise ResourceUnavailable("Slurm node/partition discovery is unavailable")
        return snapshot

    def submission_args(self, intent_id, root: Path, resources: JobResources):
        cfg = resources
        args = [
            "sbatch",
            "--parsable",
            "--job-name",
            f"et-{intent_id}",
            "--comment",
            intent_id,
            "--partition",
            cfg.partition,
            "--cpus-per-task",
            str(cfg.cpus_per_job),
            "--mem",
            str(cfg.memory_mb_per_job),
            "--time",
            str(cfg.walltime_minutes_per_job),
            "--nodes",
            str(cfg.nodes),
            "--ntasks-per-node",
            str(cfg.tasks_per_node),
            "--chdir",
            str(root),
            "--output",
            str(root / "%x-%j.out"),
            "--error",
            str(root / "%x-%j.err"),
        ]
        if cfg.account:
            args += ["--account", cfg.account]
        if cfg.qos:
            args += ["--qos", cfg.qos]
        if cfg.gpus_per_job:
            gpu = f"gpu:{cfg.gpu_type}:" if cfg.gpu_type else "gpu:"
            args += ["--gres", gpu + str(cfg.gpus_per_job // cfg.nodes)]
        if cfg.constraint:
            args += ["--constraint", cfg.constraint]
        if cfg.exclusive:
            args += ["--exclusive"]
        return args

    async def preflight(self, intent_id, script: Path, root: Path, resources: JobResources):
        self.config.limits.validate_request(resources)
        try:
            # Slurm validates account/QOS, hardware and partition rules without creating a job.
            await self.command(
                *self.submission_args(intent_id, root, resources),
                "--test-only",
                str(script),
                clean_slurm=True,
            )
        except (TimeoutError, OSError, RuntimeError) as exc:
            raise ResourceUnavailable(str(exc)) from exc

    async def submit(self, intent_id, script: Path, root: Path, *, resources=None):
        cfg = resources or self.config.defaults()
        self.config.limits.validate_request(cfg)
        args = self.submission_args(intent_id, root, cfg)
        try:
            output = await self.command(*args, str(script), clean_slurm=True)
        except (TimeoutError, OSError, RuntimeError) as exc:
            # Even a reported failure is held until reconciliation. Never blindly retry sbatch.
            raise SubmissionUncertain(str(exc)) from exc
        job_id = output.split(";")[0]
        if not job_id.isdecimal():
            raise SubmissionUncertain("unrecognized sbatch response")
        return job_id

    async def cancel(self, job_id, intent_id):
        """Cancel only the exact tracked job after checking its scheduler identity."""
        if not job_id.isdecimal():
            raise ValueError("Slurm cancellation needs a numeric saved job ID")
        queue = await self.command("squeue", "--jobs", job_id, "--noheader", "--format", "%i|%k")
        matches = [line.split("|", 1) for line in queue.splitlines() if "|" in line]
        matching = [fields for fields in matches if fields[0].strip() == job_id]
        if not matching:
            return False
        if len(matching) != 1 or matching[0][1].strip() != intent_id:
            raise ResourceUnavailable("saved job ID no longer matches its scheduler intent")
        await self.command("scancel", job_id, clean_slurm=True)
        return True

    async def reconcile(self, intent_id):
        queue = await self.command("squeue", "--me", "--noheader", "--format", "%i|%100j|%k")
        since = (datetime.now(UTC) - timedelta(days=7)).strftime("%Y-%m-%d")
        accounting = await self.command(
            "sacct",
            "--starttime",
            since,
            "--user",
            self.user,
            "--noheader",
            "--parsable2",
            "--format",
            "JobIDRaw,JobName%100,Comment",
        )
        matches = set()
        for line in (queue + "\n" + accounting).splitlines():
            fields = [f.strip() for f in line.split("|")]
            if len(fields) >= 3 and (fields[1] == f"et-{intent_id}" or fields[2] == intent_id):
                if fields[0].isdecimal():
                    matches.add(fields[0])
        if len(matches) > 1:
            raise SubmissionUncertain("multiple scheduler jobs match one submission intent")
        return next(iter(matches), None)

    async def observe(self, job_id):
        queue = await self.command("env", "TZ=UTC", "squeue", "--me", "--noheader", "--format", "%i|%T|%S|%R")
        for line in queue.splitlines():
            fields = line.split("|")
            if len(fields) >= 2 and fields[0].strip() == job_id:
                state = fields[1].strip()
                start = scheduler_time(fields[2]) if len(fields) > 2 else None
                pending = state in ("PENDING", "CONFIGURING")
                return Observation(
                    state,
                    started_at=start if not pending else None,
                    estimated_start_at=start
                    if pending and start and start > datetime.now(UTC).timestamp()
                    else None,
                    pending_reason=fields[3].strip()[:200] if pending and len(fields) > 3 else None,
                )
        accounting = await self.command(
            "env",
            "TZ=UTC",
            "sacct",
            "--jobs",
            job_id,
            "--noheader",
            "--parsable2",
            "--format",
            "JobIDRaw,State,ExitCode,Start,End",
        )
        for line in accounting.splitlines():
            fields = line.split("|")
            if len(fields) >= 3 and fields[0].strip() == job_id:
                return Observation(
                    fields[1].split()[0].rstrip("+"),
                    fields[2],
                    started_at=scheduler_time(fields[3]) if len(fields) > 3 else None,
                    ended_at=scheduler_time(fields[4]) if len(fields) > 4 else None,
                )
        return Observation("ACCOUNTING_PENDING")


class DemoScheduler:
    """Durable local simulator: executes the real CPU recipe, with no Slurm or GPU claims."""

    def __init__(self, directory: Path):
        self.directory = directory
        directory.mkdir(parents=True, exist_ok=True)
        self.submissions = 0

    async def submit(self, intent_id, script, root, *, resources=None):
        if resources and resources.gpus_per_job:
            raise ValueError("demo mode cannot execute or simulate GPU allocations")
        job_id = f"demo-{intent_id}"
        path = self.directory / f"{intent_id}.json"
        if path.exists():
            raise SubmissionUncertain("duplicate demo submission")
        self.submissions += 1
        path.write_text(json.dumps({"job_id": job_id, "state": "RUNNING"}))
        try:
            await command("/bin/sh", str(script))
            status = {"job_id": job_id, "state": "COMPLETED", "exit_code": "0:0"}
        except Exception as exc:
            status = {"job_id": job_id, "state": "FAILED", "exit_code": "1:0", "error": str(exc)}
        path.write_text(json.dumps(status))
        return job_id

    async def reconcile(self, intent_id):
        path = self.directory / f"{intent_id}.json"
        return json.loads(path.read_text())["job_id"] if path.exists() else None

    async def cancel(self, job_id, intent_id):
        if job_id != f"demo-{intent_id}":
            raise ValueError("demo job identity mismatch")
        path = self.directory / f"{intent_id}.json"
        if not path.exists():
            return False
        value = json.loads(path.read_text())
        if value["state"] in ("RUNNING", "PENDING"):
            path.write_text(json.dumps({"job_id": job_id, "state": "CANCELLED", "exit_code": "1:0"}))
            return True
        return False

    async def observe(self, job_id):
        path = self.directory / f"{job_id.removeprefix('demo-')}.json"
        if not path.exists():
            return Observation("ACCOUNTING_PENDING")
        value = json.loads(path.read_text())
        return Observation(value["state"], value.get("exit_code"))
