"""Short SSH commands and resumable transfers; workflow authority stays on acacia."""

import asyncio
import contextlib
import hashlib
import json
import re
import shlex
import shutil
import time
from pathlib import Path
from uuid import uuid4

from . import artifacts
from .execution_settings import ExecutionSettings
from .slurm import ResourceUnavailable, SlurmScheduler, command
from .store import encode


class AdmissionPending(ResourceUnavailable):
    """The cluster's current submission count/quota holds an otherwise saved request."""


class TransferCancelled(RuntimeError):
    """Local transfer work was stopped by an experiment cancellation."""


class TransferPending(RuntimeError):
    """A retryable transport failure, never evidence that scientific work failed."""


def ssh_args(host):
    # -F bypasses this host's broken system-wide proxy include, using the existing
    # researcher-owned alias, key and known_hosts. Never relax host verification.
    args = ["ssh"]
    config = Path.home() / ".ssh/config"
    if config.is_file():
        args += ["-F", str(config)]
    return args + ["-o", "BatchMode=yes", "-o", "ConnectTimeout=15", host]


def transfer_args(template, values):
    args = shlex.split(template)
    return [arg.format_map(values) for arg in args]


def sbatch_directives(path, resources=None):
    """Only literal supported directives; managed CLI flags override names/logs/resources."""
    allowed = {
        "-c",
        "--cpus-per-task",
        "--gres",
        "--gpus",
        "--gpus-per-node",
        "--ntasks",
        "-n",
        "--ntasks-per-node",
        "--nodes",
        "-N",
        "--constraint",
        "-C",
        "--time",
        "-t",
        "--account",
        "-A",
        "--qos",
        "-q",
        "--partition",
        "-p",
        "--mem",
        "--exclusive",
        "--output",
        "-o",
        "--error",
        "-e",
        "--job-name",
        "-J",
        "--comment",
        "--chdir",
        "-D",
    }
    directives = []
    for line in path.read_text().splitlines():
        if line.startswith("#SBATCH"):
            args = shlex.split(line[len("#SBATCH") :].strip())
            if not args or args[0].split("=", 1)[0] not in allowed:
                raise ValueError("unsupported sbatch directive; use declared stage resources: " + line[:200])
            if any("$" in arg or "`" in arg for arg in args):
                raise ValueError("sbatch directives must contain literal values")
            if len(args) > 2:
                raise ValueError("write one resource directive per SBATCH line")
            option, separator, value = args[0].partition("=")
            value = value if separator else args[1] if len(args) == 2 else ""
            if resources:
                compare_sbatch_resource(option, value, resources)
            # Resource and tracking flags are supplied by the executor CLI. Keep
            # the source directive as a comment, avoiding alternative Slurm GPU
            # flags (--gpus vs --gres) silently accumulating extra resources.
            directives.append(line.replace("#SBATCH", "# source SBATCH", 1))
    return directives


def compare_sbatch_resource(option, value, resources):
    direct = {
        "-c": "cpus_per_job",
        "--cpus-per-task": "cpus_per_job",
        "--nodes": "nodes",
        "-N": "nodes",
        "--ntasks-per-node": "tasks_per_node",
        "--constraint": "constraint",
        "-C": "constraint",
        "--account": "account",
        "-A": "account",
        "--qos": "qos",
        "-q": "qos",
        "--partition": "partition",
        "-p": "partition",
    }
    expected, actual = None, value
    if option in direct:
        expected = str(getattr(resources, direct[option]))
    elif option in ("--ntasks", "-n"):
        expected = str(resources.nodes * resources.tasks_per_node)
    elif option in ("--gpus", "--gpus-per-node", "--gres"):
        parts = value.split(":")
        count = int(parts[-1])
        gpu_type = parts[-2] if len(parts) > 2 or option != "--gres" and len(parts) > 1 else None
        if option == "--gres" and parts[0] != "gpu":
            raise ValueError("only GPU gres directives are supported")
        expected = str(
            resources.gpus_per_job if option == "--gpus" else resources.gpus_per_job // resources.nodes
        )
        actual = str(count)
        if gpu_type and gpu_type != resources.gpu_type:
            raise ValueError("SBATCH GPU type differs from structured resources")
    elif option == "--mem":
        match = re.fullmatch(r"([0-9]+)([KMGTP]?)", value.upper())
        if not match:
            raise ValueError("use an integer Slurm memory request")
        multiplier = {"": 1, "K": 1 / 1024, "M": 1, "G": 1024, "T": 1024**2, "P": 1024**3}[match[2]]
        expected, actual = resources.memory_mb_per_job, int(match[1]) * multiplier
    elif option in ("--time", "-t"):
        days, clock = value.split("-", 1) if "-" in value else ("0", value)
        parts = [int(part) for part in clock.split(":")]
        minutes = (
            parts[0]
            if len(parts) == 1
            else parts[0] + parts[1] / 60
            if len(parts) == 2
            else parts[0] * 60 + parts[1] + parts[2] / 60
        )
        if days != "0":
            minutes = int(days) * 1440 + (
                parts[0] * 60
                if len(parts) == 1
                else parts[0] * 60 + parts[1] + (parts[2] / 60 if len(parts) == 3 else 0)
            )
        expected, actual = resources.walltime_minutes_per_job, minutes
    elif option == "--exclusive":
        expected, actual = resources.exclusive, True
    if expected is not None and actual != expected:
        raise ValueError(f"SBATCH {option}={value} differs from declared stage resources ({expected})")


class RemoteSlurmScheduler(SlurmScheduler):
    def __init__(self, workflow, exp, settings=None, *, saved=None):
        super().__init__(workflow.config.slurm)
        self.workflow = workflow
        self.store = workflow.store
        self.exp = exp
        self.settings = ExecutionSettings.model_validate(settings or exp["contract"].get("execution", {}))
        self.saved = saved
        self.layout = saved.get("layout") if saved else None
        self.command = self.remote_command

    async def remote_command(self, *args, timeout=30, clean_slurm=False):
        args = list(args)
        # sacct defaults to the remote current user; the acacia numeric UID must
        # never filter Jean Zay accounting.
        if args[0] == "sacct" and "--user" in args:
            position = args.index("--user")
            del args[position : position + 2]
        cache_key = None
        cache = self.workflow.pool_owner.remote_reads
        if args[0] == "squeue" and "--me" in args:
            cache_key = (self.settings.ssh_host, "reconcile_queue")
        if args[0] == "sacct" and "--starttime" in args:
            cache_key = (self.settings.ssh_host, "reconcile_accounting")
        if args[0] == "env" and "squeue" in args:
            cache_key = (self.settings.ssh_host, "queue")
        if "sacct" in args and "--jobs" in args and "JobIDRaw,State,ExitCode,Start,End" in args:
            identities = {args[args.index("--jobs") + 1]}
            for job in self.store.rows(
                "SELECT job_id,execution_manifest FROM jobs WHERE state IN ('submitting','submitted') AND job_id IS NOT NULL"
            ):
                metadata = json.loads(job["execution_manifest"] or "{}").get("execution", {})
                if metadata.get("layout", {}).get("host") == self.settings.ssh_host:
                    identities.add(job["job_id"])
            args[args.index("--jobs") + 1] = ",".join(sorted(identities))
            cache_key = (self.settings.ssh_host, "accounting")
        if cache_key and time.time() - cache.get(cache_key, (0, ""))[0] < self.settings.poll_seconds:
            return cache[cache_key][1]
        invocation = shlex.join([str(arg) for arg in args])
        if clean_slurm:
            invocation = 'for name in ${!SBATCH_@}; do unset "$name"; done; ' + invocation
        output = await command(
            *ssh_args(self.settings.ssh_host), "bash -lc " + shlex.quote(invocation), timeout=timeout
        )
        if cache_key:
            cache[cache_key] = (time.time(), output)
        return output

    async def resolve(self):
        if self.layout:
            return self.layout
        settings = self.settings
        if not settings.remote_project:
            raise ResourceUnavailable(
                "Set the project's Jean Zay remote folder and rsync commands before compute"
            )
        # Shell expands only the three accepted directory variables; no arbitrary evaluation.
        expression = settings.remote_project
        if expression.startswith("$"):
            variable, suffix = expression[1:].split("/", 1)
            base = await self.remote_command("printenv", variable)
            if not base.startswith("/"):
                raise ResourceUnavailable(f"Jean Zay {variable} is unavailable in the login environment")
            expression = base.rstrip("/") + "/" + suffix
        if not expression.startswith("/"):
            raise ResourceUnavailable("Remote project must resolve to an absolute path")
        prefix = (
            expression.rstrip("/")
            + "/.experimenttracker/executions/"
            + self.exp["id"]
            + "/"
            + str(self.exp.get("_attempt_id", self.exp["evidence_revision"]))
        )
        self.layout = {
            "remote_base": expression,
            "remote_project": prefix + "/project",
            "remote_run": prefix + "/run",
            "local_project": str(
                self.workflow.project_context(self.exp)["checkout"]
                or self.workflow.working_directory(self.exp)
            ),
            "local_run": self.exp.get("_attempt_root", str(self.workflow.root(self.exp["id"]))),
            "host": settings.ssh_host,
        }
        return self.layout

    async def discover(self):
        user = await self.remote_command("id", "-un")
        self.user = self.association_user = user
        return {**await super().discover(), "execution_context": "jean-zay"}

    async def transfer(self, direction, operation, *, identity=None):
        if self.store.experiment(self.exp["id"])["phase"] == "cancelled":
            raise TransferCancelled("Experiment cancelled")
        layout = await self.resolve()
        if self.store.experiment(self.exp["id"])["phase"] == "cancelled":
            raise TransferCancelled("Experiment cancelled")
        payload = {"settings": self.settings.model_dump(), "layout": layout}
        identity = identity or str(uuid4())
        with self.store.tx() as db:
            prior = db.execute("SELECT payload FROM execution_transfers WHERE id=?", (identity,)).fetchone()
            if prior and prior["payload"] != encode(payload):
                raise ValueError("transfer identity changed")
            db.execute(
                "INSERT OR IGNORE INTO execution_transfers(id,experiment_id,direction,payload,state,updated_at) VALUES(?,?,?,?,'pending',?)",
                (identity, self.exp["id"], direction, encode(payload), time.time()),
            )
        try:
            task = asyncio.create_task(operation())
            try:
                while True:
                    done, _ = await asyncio.wait({task}, timeout=0.1)
                    if self.store.experiment(self.exp["id"])["phase"] == "cancelled":
                        raise TransferCancelled("Experiment cancelled")
                    if done:
                        result = task.result()
                        break
            finally:
                if not task.done():
                    task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        except TransferCancelled:
            raise
        except Exception as exc:
            with self.store.tx() as db:
                db.execute(
                    "UPDATE execution_transfers SET state='pending',result=?,updated_at=? WHERE id=? AND state!='cancelled'",
                    (str(exc)[:2000], time.time(), identity),
                )
            raise TransferPending(f"Jean Zay {direction} transfer pending: {exc}") from exc
        with self.store.tx() as db:
            db.execute(
                "UPDATE execution_transfers SET state='complete',result=NULL,updated_at=? WHERE id=? AND state!='cancelled'",
                (time.time(), identity),
            )
        return result

    async def rsync(self, *args):
        # Credentials and their local directories never enter a project transfer.
        ssh = shlex.join(ssh_args(self.settings.ssh_host)[:-1])
        await command(
            "rsync",
            "-e",
            ssh,
            "--exclude=.git/",
            "--exclude=.ssh/",
            "--exclude=.codex/",
            "--exclude=.aws/",
            "--exclude=.env",
            "--exclude=.local/",
            "--exclude=.venv/",
            "--exclude=auth.json",
            "--exclude=*.pem",
            *args,
            timeout=180,
        )

    async def custom_transfer(self, direction):
        template = getattr(self.settings, "rsync_" + direction)
        if template.strip():
            args = transfer_args(template, await self.resolve())
            protected = (
                ["--exclude=.experimenttracker/"]
                if direction == "up"
                else [
                    "--exclude=/snapshot/",
                    "--exclude=/work/",
                    "--exclude=/analysis/",
                    "--exclude=/agent-context/",
                    "--exclude=/job-source-*.json",
                    "--exclude=/local-output-*.json",
                ]
            )
            await self.rsync(*protected, *args[1:])

    def metadata(self):
        if not self.layout:
            raise ValueError("remote execution has not been staged")
        return {
            "context": "jean-zay",
            "settings": self.settings.model_dump(),
            "layout": self.layout,
            "memory_allocation": "cluster-assigned; --mem is not submitted; memory_mb_per_job is a planning estimate",
        }

    def submission_args(self, intent_id, root, resources):
        args = super().submission_args(intent_id, root, resources)
        # The IDRIS submit plugin rejects explicit memory options. Memory follows
        # the cluster's CPU/GPU allocation policy; never pretend --mem was applied.
        position = args.index("--mem")
        del args[position : position + 2]
        return args

    async def stage(self, intent_id, script, resources, stage):
        layout = await self.resolve()
        if self.config.limits.memory_mb_per_job is not None:
            raise ResourceUnavailable(
                "Jean Zay assigns memory from the allocation and rejects --mem; an executor memory ceiling cannot be enforced. Remove that optional ceiling or use acacia."
            )
        if self.exp["contract"].get("execution_contract") != 3 and (
            not self.settings.rsync_up.strip() or not self.settings.rsync_down.strip()
        ):
            raise ResourceUnavailable(
                "Set both project rsync commands; use the project execution tool or Project settings"
            )
        root = Path(layout["local_run"])
        remote_root = Path(layout["remote_run"])
        source = root / "snapshot" / stage["script"] if stage.get("script") else None
        directives = sbatch_directives(source, resources) if source and source.suffix == ".sbatch" else []
        exports = {
            "EXPERIMENT_ROOT": str(remote_root),
            "EXPERIMENT_SOURCE": str(remote_root / "snapshot"),
            "EXPERIMENT_PROJECT": layout["remote_project"],
            "EXPERIMENT_SHARED_PROJECT": layout["remote_base"],
            "EXPERIMENT_STAGE": stage["id"],
            "EXPERIMENT_SNAPSHOT_HASH": self.exp["snapshot"]["snapshot_hash"],
            "EXPERIMENT_PYTHON": "/usr/bin/python3",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONUNBUFFERED": "1",
            "OMP_NUM_THREADS": str(resources.cpus_per_job),
        }
        if self.exp["contract"].get("execution_contract") == 3:
            exports.update(
                EXPERIMENT_INPUTS=str(remote_root / "inputs.json"),
                EXPERIMENT_HOST=layout["host"],
                PYTHONPATH=str(remote_root / "snapshot") + ":" + layout["remote_project"],
            )
        invoke = (
            ["/bin/bash", str(remote_root / "snapshot" / stage["script"])]
            if source
            else [
                "/usr/bin/python3",
                str(remote_root / "snapshot/recipe.py"),
                stage["id"],
                "--output",
                str(remote_root),
            ]
        )
        helper = ["/usr/bin/python3", str(remote_root / "remote-worker.py")]
        helper_args = [str(remote_root), stage["id"], intent_id]
        script.chmod(0o600)
        script.write_text(
            "#!/bin/bash\n"
            + "\n".join(directives)
            + "\nset -euo pipefail\n"
            + "".join(f"export {key}={shlex.quote(value)}\n" for key, value in exports.items())
            + shlex.join(helper + ["verify"] + helper_args)
            + "\ncd "
            + shlex.quote(
                layout["remote_project"]
                if directives or source and source.suffix == ".sbatch"
                else str(remote_root / "snapshot")
            )
            + "\n"
            + shlex.join(invoke)
            + "\n"
            + shlex.join(helper + ["seal"] + helper_args)
            + "\n"
        )
        script.chmod(0o444)
        helper_path = root / "remote-worker.py"
        shutil.copyfile(Path(__file__).with_name("remote_worker.py"), helper_path)

        # Upload immutable sources through a temporary directory; never rsync down
        # into the source tree and never copy acacia's Python virtual environment.
        async def upload():
            await self.remote_command("mkdir", "-p", layout["remote_project"], layout["remote_run"])
            if self.exp["snapshot"].get("plan", {}).get("schema_version") != 3:
                await self.custom_transfer("up")
            pending = layout["remote_run"] + "/snapshot.pending"
            await self.remote_command("mkdir", "-p", pending)
            await self.rsync("-rlt", str(root / "snapshot") + "/", layout["host"] + ":" + pending + "/")
            await self.remote_command(
                "bash",
                "-c",
                'if test ! -d "$1/snapshot"; then mv "$1/snapshot.pending" "$1/snapshot"; fi',
                "stage",
                layout["remote_run"],
            )
            # Overlay only declared project sources from their frozen bytes.
            for name, frozen in self.exp["snapshot"].get("project_sources", {}).items():
                destination = Path(layout["remote_project"]) / name
                await self.remote_command("mkdir", "-p", str(destination.parent))
                await self.rsync(
                    "-lt", str(root / "snapshot" / frozen), layout["host"] + ":" + str(destination)
                )
            if self.exp["snapshot"].get("plan", {}).get("schema_version") == 3:
                await self.rsync(
                    "-lt", str(root / "inputs.json"), layout["host"] + ":" + layout["remote_run"] + "/"
                )
            await self.rsync(
                "-lt", str(script), str(helper_path), layout["host"] + ":" + layout["remote_run"] + "/"
            )

        transfer_id = hashlib.sha256(
            (self.exp["id"] + str(self.exp["evidence_revision"]) + intent_id + "up").encode()
        ).hexdigest()
        await self.transfer("up", upload, identity=transfer_id)
        try:
            await super().preflight(intent_id, remote_root / script.name, remote_root, resources)
        except ResourceUnavailable as exc:
            detail = str(exc)
            if any(
                marker in detail
                for marker in (
                    "AssocGrpSubmitJobsLimit",
                    "QOSMaxSubmitJobPerUserLimit",
                    "QOSGrpSubmitJobsLimit",
                    "AssocMaxSubmitJobLimit",
                    "QOSMaxSubmitJobPerAccountLimit",
                )
            ):
                raise AdmissionPending(detail) from exc
            if "ssh failed (255)" in detail or "timed out" in detail:
                raise TransferPending(detail) from exc
            raise

    async def preflight(self, intent_id, script, root, resources):
        stage_id = next(
            stage["id"]
            for stage in self.exp["snapshot"]["plan"]["stages"]
            if script.name.startswith(stage["id"] + "-")
        )
        stage = next(stage for stage in self.exp["snapshot"]["plan"]["stages"] if stage["id"] == stage_id)
        await self.stage(intent_id, script, resources, stage)

    async def submit(self, intent_id, script, root, *, resources=None):
        layout = await self.resolve()
        self.workflow.pool_owner.remote_reads.clear()
        return await super().submit(
            intent_id,
            Path(layout["remote_run"]) / script.name,
            Path(layout["remote_run"]),
            resources=resources,
        )

    def due(self, job, now):
        return now - (job.get("remote_polled_at") or 0) >= self.settings.poll_seconds

    async def observe(self, job_id):
        # A single queue/accounting snapshot per host per sweep is supplied by
        # the workflow. Reads still use the saved exact job ID.
        return await super().observe(job_id)

    async def fetch(self, job, observed):
        layout = await self.resolve()
        root = Path(layout["local_run"])
        terminal = observed.state in {
            "COMPLETED",
            "FAILED",
            "CANCELLED",
            "TIMEOUT",
            "OUT_OF_MEMORY",
            "NODE_FAIL",
            "PREEMPTED",
            "BOOT_FAIL",
            "DEADLINE",
        }

        async def download():
            downloads = self.workflow.pool_owner.remote_downloads
            key = (self.exp["id"], self.exp["evidence_revision"], self.settings.rsync_down)
            if terminal or time.time() - downloads.get(key, 0) >= self.settings.poll_seconds:
                if not self.exp["contract"].get("execution_contract") == 3:
                    await self.custom_transfer("down")
                downloads[key] = time.time()
            for suffix in ("out", "err"):
                name = f"et-{job['intent_id']}-{job['job_id']}.{suffix}"
                try:
                    await self.rsync(
                        "-lt", layout["host"] + ":" + layout["remote_run"] + "/" + name, str(root / name)
                    )
                except RuntimeError:
                    if terminal and observed.exit_code == "0:0":
                        raise
            # Optional event previews share the normal remote polling cadence.
            # Transfer failure must never retry compute or block checked outputs.
            from .tfevents import selection

            manifest = json.loads(job["execution_manifest"])
            display = selection(manifest, job["stage_id"])
            if display:
                destination = root / display["log_dir"]
                try:
                    resolved_root = await asyncio.to_thread(root.resolve)
                    if destination.resolve().is_relative_to(resolved_root) and not destination.is_symlink():
                        destination.mkdir(parents=True, exist_ok=True)
                        await self.rsync(
                            "-rlt",
                            "--safe-links",
                            "--max-size=512m",
                            "--include=/events.out.tfevents.*",
                            "--exclude=*",
                            layout["host"] + ":" + layout["remote_run"] + "/" + display["log_dir"] + "/",
                            str(destination) + "/",
                        )
                except (OSError, RuntimeError):
                    pass
            if observed.state != "COMPLETED" or observed.exit_code != "0:0":
                return
            receipt_name = f"remote-receipt-{job['stage_id']}-{job['intent_id']}.json"
            pending = root / (".transfer-" + job["intent_id"])
            pending.mkdir(exist_ok=True)
            await self.rsync(
                "-lt",
                layout["host"] + ":" + layout["remote_run"] + "/" + receipt_name,
                str(pending / receipt_name),
            )
            receipt = artifacts.read_json(pending / receipt_name, pending)
            manifest = json.loads(job["execution_manifest"])
            if (
                receipt.get("intent_id") != job["intent_id"]
                or receipt.get("stage") != job["stage_id"]
                or receipt.get("snapshot_hash") != job["snapshot_hash"]
            ):
                raise ValueError("remote output receipt identity mismatch")
            expected = manifest["expected_outputs"][job["stage_id"]]
            if [record["name"] for record in receipt["outputs"]] != expected:
                raise ValueError("remote output membership mismatch")
            if manifest.get("plan", {}).get("schema_version") == 3:
                spec = next(s for s in manifest["plan"]["stages"] if s["id"] == job["stage_id"])
                if [r.get("name") for r in receipt.get("local_outputs", [])] != spec.get("local_outputs", []):
                    raise ValueError("remote declared output references differ from this attempt")
                if any(r.get("kind") not in ("file", "directory") for r in receipt["local_outputs"]):
                    raise ValueError("invalid remote output reference kind")
            for record in receipt["outputs"]:
                if (
                    type(record.get("size")) is not int
                    or not 0 <= record["size"] <= artifacts.MAX_ARTIFACT_BYTES
                ):
                    raise ValueError("remote small output exceeds transfer limits")
                name = record["name"]
                await self.rsync(
                    "-lt",
                    "--max-size=10m",
                    layout["host"] + ":" + layout["remote_run"] + "/" + name,
                    str(pending / name),
                )
                path = artifacts.allowed_path(pending / name, pending)
                if artifacts.sha256(path) != record["sha256"] or path.stat().st_size != record["size"]:
                    raise ValueError("downloaded output hash mismatch: " + name)
            for record in receipt["outputs"]:
                (pending / record["name"]).replace(root / record["name"])
            value = {
                "schema_version": 1,
                "stage": job["stage_id"],
                "storage": "jean-zay",
                "host": layout["host"],
                "root": layout["remote_run"],
                "snapshot_hash": receipt["snapshot_hash"],
                "intent_id": job["intent_id"],
                "outputs": receipt["local_outputs"],
            }
            provenance = root / ("local-output-" + job["stage_id"] + ".json")
            if receipt["local_outputs"] and manifest.get("plan", {}).get("schema_version") != 3:
                if provenance.exists() and artifacts.read_json(provenance, root) != value:
                    raise ValueError("remote checkpoint identity changed")
                provenance.write_text(encode(value))
                provenance.chmod(0o444)
            (pending / receipt_name).replace(root / receipt_name)

        identity = (
            hashlib.sha256((job["intent_id"] + ":terminal-delivery").encode()).hexdigest()
            if terminal
            else None
        )
        await self.transfer("down", download, identity=identity)
