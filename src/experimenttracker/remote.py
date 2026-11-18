"""Short SSH commands and resumable transfers; workflow authority stays on acacia."""

import asyncio
import contextlib
import hashlib
import json
import os
import re
import shlex
import shutil
import tempfile
from pathlib import Path
from uuid import UUID, uuid4

from . import artifacts, clock
from .core import obligation_sources, transfer_obligations
from .execution_settings import ExecutionSettings
from .slurm import ResourceUnavailable, SlurmScheduler, command
from .store import encode


class AdmissionPending(ResourceUnavailable):
    """The cluster's current submission count/quota holds an otherwise saved request."""


class TransferCancelled(RuntimeError):
    """Local transfer work was stopped by an experiment cancellation."""


class TransferPending(RuntimeError):
    """A retryable transport failure, never evidence that scientific work failed."""


def exact_research_attempt(workflow, experiment_id, job):
    try:
        intent = str(UUID(str(job.get("intent_id"))))
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError("remote result requires a UUID job intent") from exc
    if intent != job.get("intent_id") or job.get("experiment_id") != experiment_id:
        raise ValueError("remote result requires this experiment's exact saved intent")
    root = Path(job.get("execution_root") or "")
    base = Path(workflow.projects.base_root(experiment_id)).resolve()
    expected = base / "attempts" / intent
    if (
        not root.is_absolute()
        or root.is_symlink()
        or root != expected
        or root.resolve() != expected
        or not root.is_dir()
    ):
        raise ValueError("remote result requires this intent's exact saved attempt")
    return root


def place_research_result(temporary, target, root):
    root = root.resolve()
    parent = target.parent
    relative = parent.relative_to(root)
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError("result destination parent must not contain symlinks")
        if not current.exists():
            current.mkdir()
        if not current.is_dir() or not current.resolve().is_relative_to(root):
            raise ValueError("result destination escapes the exact attempt")
    downloaded = artifacts.allowed_path(temporary, root)
    try:
        os.link(downloaded, target)
    except FileExistsError:
        # A crash can leave the file installed before transfer completion commits.
        existing = artifacts.allowed_path(target, root)
        if existing.read_bytes() != downloaded.read_bytes():
            raise ValueError("pending remote result destination has different contents") from None
    return artifacts.allowed_path(target, root)


def ssh_args(host):
    # -F bypasses this host's broken system-wide proxy include, using the existing
    # researcher-owned alias, key and known_hosts. Never relax host verification.
    args = ["ssh"]
    config = Path.home() / ".ssh/config"
    if config.is_file():
        args += ["-F", str(config)]
    return args + ["-o", "BatchMode=yes", "-o", "ConnectTimeout=15", host]


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
        self.autopilot_job = lambda job: bool(
            self.store.one("SELECT 1 FROM autopilot_allocations WHERE job_id=?", (job,))
        )
        self.command = self.remote_command

    async def remote_command(self, *args, timeout=30, clean_slurm=False):
        args = list(args)
        # sacct defaults to the remote current user; the acacia numeric UID must
        # never filter Jean Zay accounting.
        if args[0] == "sacct" and "--user" in args:
            position = args.index("--user")
            del args[position : position + 2]
        cache_key = None
        cache = self.workflow.remote_reads
        if args[0] == "squeue" and "--me" in args:
            cache_key = (self.settings.ssh_host, "reconcile_queue")
        if args[0] == "sacct" and "--starttime" in args:
            cache_key = (self.settings.ssh_host, "reconcile_accounting")
        if args[0] == "env" and "squeue" in args:
            cache_key = (self.settings.ssh_host, "queue")
        if (
            "sacct" in args
            and "--jobs" in args
            and any(str(arg).startswith("JobIDRaw,State,ExitCode,Start,End") for arg in args)
        ):
            identities = {args[args.index("--jobs") + 1]}
            for job in self.store.rows(
                "SELECT job_id,execution_manifest FROM jobs WHERE state IN ('submitting','submitted') AND job_id IS NOT NULL"
            ):
                metadata = json.loads(job["execution_manifest"] or "{}").get("execution", {})
                if metadata.get("layout", {}).get("host") == self.settings.ssh_host:
                    identities.add(job["job_id"])
            args[args.index("--jobs") + 1] = ",".join(sorted(identities))
            cache_key = (
                self.settings.ssh_host,
                "accounting-autopilot" if "--duplicates" in args else "accounting",
            )
        if cache_key and clock.now() - cache.get(cache_key, (0, ""))[0] < self.settings.poll_seconds:
            return cache[cache_key][1]
        invocation = shlex.join([str(arg) for arg in args])
        if clean_slurm:
            invocation = 'for name in ${!SBATCH_@}; do unset "$name"; done; ' + invocation
        output = await command(
            *ssh_args(self.settings.ssh_host), "bash -lc " + shlex.quote(invocation), timeout=timeout
        )
        if cache_key:
            cache[cache_key] = (clock.now(), output)
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

    async def transfer(
        self, direction, operation, *, identity=None, supersedes=None, superseded_by=None, result_path=None
    ):
        """Run one exact saved transfer episode under its caller's saved identity.

        A terminal delivery admission passes the same job's ``supersedes`` preview
        row, which is recorded cancelled in that same transaction because an
        obsolete optional refresh is abandoned, never completed. A preview
        admission passes that job's ``superseded_by`` delivery row and is refused
        once it exists.
        """
        if self.store.experiment(self.exp["id"])["phase"] == "cancelled":
            raise TransferCancelled("Experiment cancelled")
        layout = await self.resolve()
        if self.store.experiment(self.exp["id"])["phase"] == "cancelled":
            raise TransferCancelled("Experiment cancelled")
        payload = {"settings": self.settings.model_dump(), "layout": layout}
        if result_path is not None:
            payload["result_path"] = result_path
        identity = identity or str(uuid4())
        with self.store.tx() as db:
            before_row = db.execute("SELECT * FROM execution_transfers WHERE id=?", (identity,)).fetchone()
            before = dict(before_row) if before_row else None
            if before and (
                before["experiment_id"] != self.exp["id"]
                or before["direction"] != direction
                or before["payload"] != encode(payload)
            ):
                raise ValueError("transfer identity changed")
            parent = db.execute("SELECT phase FROM experiments WHERE id=?", (self.exp["id"],)).fetchone()
            if parent is None:
                raise ValueError(f"transfer experiment does not exist: {self.exp['id']}")
            if parent["phase"] == "cancelled":
                raise TransferCancelled("Experiment cancelled")
            if before and before["state"] == "cancelled":
                raise TransferCancelled("Transfer cancelled")
            if (
                superseded_by
                and db.execute("SELECT 1 FROM execution_transfers WHERE id=?", (superseded_by,)).fetchone()
            ):
                raise TransferCancelled("Terminal delivery admitted; event preview refused")
            now = clock.now()
            if before is None:
                db.execute(
                    "INSERT INTO execution_transfers"
                    "(id,experiment_id,direction,payload,state,result,updated_at) "
                    "VALUES(?,?,?,?,'pending',NULL,?)",
                    (identity, self.exp["id"], direction, encode(payload), now),
                )
            elif before["state"] == "complete":
                db.execute(
                    "UPDATE execution_transfers SET state='pending',result=NULL,updated_at=? "
                    "WHERE id=? AND state='complete'",
                    (now, identity),
                )
            elif before["state"] != "pending":
                raise ValueError(f"transfer cannot be retried from saved state {before['state']}")
            if supersedes:
                stale = db.execute(
                    "SELECT * FROM execution_transfers WHERE id=? AND state='pending'", (supersedes,)
                ).fetchone()
                if stale is not None:
                    db.execute(
                        "UPDATE execution_transfers SET state='cancelled',"
                        "result='Superseded by terminal delivery',updated_at=? "
                        "WHERE id=? AND state='pending'",
                        (now, supersedes),
                    )
                    transfer_obligations.sync_transfer(db, supersedes, before=stale)
            obligation = transfer_obligations.sync_transfer(db, identity, before=before)
            admitted_episode = obligation["id"]
            self.store.emit(
                db, self.exp["id"], "state_changed", {"transfer_id": identity, "transfer_state": "pending"}
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
            self._save_transfer_outcome(identity, admitted_episode, "pending", str(exc)[:2000])
            raise TransferPending(f"Jean Zay {direction} transfer pending: {exc}") from exc
        self._save_transfer_outcome(identity, admitted_episode, "complete")
        if self.store.experiment(self.exp["id"])["phase"] == "cancelled":
            raise TransferCancelled("Experiment cancelled")
        return result

    def _save_transfer_outcome(self, identity, episode, state, result=None):
        """Save one exact outcome for the admitted transfer episode.

        Refuses a stale callback: the saved row must still be the pending row this
        call admitted, and its open episode must still be the latest one. A failure
        keeps the row pending and that episode open, so a retry keeps its age.
        """
        with self.store.tx() as db:
            saved = db.execute("SELECT * FROM execution_transfers WHERE id=?", (identity,)).fetchone()
            if saved is None:
                raise RuntimeError(f"saved transfer disappeared before its outcome: {identity}")
            if saved["state"] == "cancelled":
                raise TransferCancelled("Transfer cancelled")
            latest = obligation_sources.latest_episode(db, "transfer", identity)
            if (
                saved["state"] != "pending"
                or latest is None
                or latest["id"] != episode
                or latest["state"] != "open"
            ):
                raise RuntimeError(f"transfer {identity} responsibility changed before its outcome was saved")
            before = dict(saved)
            db.execute(
                "UPDATE execution_transfers SET state=?,result=?,updated_at=? WHERE id=? AND state='pending'",
                (state, result, clock.now(), identity),
            )
            transfer_obligations.sync_transfer(db, identity, before=before)
            if state != "pending":
                self.store.emit(
                    db, self.exp["id"], "state_changed", {"transfer_id": identity, "transfer_state": state}
                )

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
        if (self.exp.get("contract") or {}).get("execution_contract") == 4 or self.workflow.store.one(
            "SELECT 1 FROM autopilot_allocations WHERE intent_id=?", (intent_id,)
        ):
            args += ["--no-requeue"]
        # The IDRIS submit plugin rejects explicit memory options. Memory follows
        # the cluster's CPU/GPU allocation policy; never pretend --mem was applied.
        position = args.index("--mem")
        del args[position : position + 2]
        return args

    async def stage(self, intent_id, script, resources, stage):
        layout = await self.resolve()
        if self.exp["snapshot"].get("plan", {}).get("schema_version") != 3:
            raise ResourceUnavailable("Jean Zay remote execution requires a schema-3 implementation")
        if self.config.limits.memory_mb_per_job is not None:
            raise ResourceUnavailable(
                "Jean Zay assigns memory from the allocation and rejects --mem; an executor memory ceiling cannot be enforced. Remove that optional ceiling or use acacia."
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

    async def stage_research_source(self, job):
        """Stage one saved C4 pinned export; this does not submit or preflight a job."""
        from .research.admission import SubmitJobRequest, preflight
        from .research.job_observe import validate_remote_route
        from .research.tools import require_live_research

        saved_job = self.store.one("SELECT * FROM jobs WHERE intent_id=?", (job.get("intent_id"),))
        if not saved_job or saved_job["experiment_id"] != self.exp["id"]:
            raise ValueError("remote staging requires this experiment's exact saved job intent")
        job = dict(saved_job)
        if job.get("state") != "submitting":
            raise ValueError("remote staging requires a submitting job intent")
        parent = self.store.one("SELECT phase,deleted_at FROM experiments WHERE id=?", (self.exp["id"],))
        if (
            parent is None
            or parent["deleted_at"] is not None
            or parent["phase"] in {"complete", "failed", "cancelled"}
        ):
            raise ValueError("remote staging requires a non-terminal parent experiment")
        manifest = json.loads(job.get("execution_manifest") or "{}")
        request = SubmitJobRequest.model_validate(manifest.get("research_request"))
        if str(request.request_id) != job["intent_id"] or manifest.get("commit") != job.get("snapshot_hash"):
            raise ValueError("remote staging metadata differs from this exact saved intent")
        action_id = manifest.get("research_action_id")
        action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,))
        if not action or action["experiment_id"] != self.exp["id"]:
            raise ValueError("remote staging requires the exact approved research action")
        require_live_research(self.store.experiment(self.exp["id"]), action)
        preflight(
            self.workflow,
            self.exp["id"],
            action,
            request,
            remote_source=True,
            existing_intent=job["intent_id"],
        )
        execution = manifest.get("execution")
        settings, layout = validate_remote_route(
            self.exp, job["intent_id"], execution, job.get("execution_root")
        )
        if settings.model_dump() != self.settings.model_dump():
            raise ValueError("scheduler settings differ from this job's saved execution route")
        commit = manifest.get("commit")
        if not isinstance(commit, str) or job.get("snapshot_hash") != commit:
            raise ValueError("remote staging requires the exact pinned commit")
        from .engine import gitworkspace
        from .workspaces import git

        branch = self.workflow.workspaces.branch(self.exp["id"])
        pin = (
            git(branch["repo"], "rev-parse", f"refs/et/jobs/{job['intent_id']}").decode().strip()
            if branch
            else None
        )
        if pin != commit:
            raise ValueError("remote staging requires the exact saved Git pin")
        self.layout = layout
        marker = json.dumps({"intent_id": job["intent_id"], "commit": commit}, sort_keys=True)
        remote_root = layout["remote_run"]
        final = remote_root + "/c4-source"
        exports = {
            "EXPERIMENT_ROOT": remote_root,
            "EXPERIMENT_SOURCE": final + "/source",
            "OMP_NUM_THREADS": str(request.resources.cpus_per_job),
            "OPENBLAS_NUM_THREADS": str(request.resources.cpus_per_job),
            "MKL_NUM_THREADS": str(request.resources.cpus_per_job),
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONUNBUFFERED": "1",
        }
        wrapper = (
            "#!/bin/sh\nset -eu\n"
            + "".join(f"export {name}={shlex.quote(value)}\n" for name, value in exports.items())
            + 'cd "$EXPERIMENT_SOURCE"\nexec '
            + shlex.join(request.command)
            + "\n"
        )
        identity = hashlib.sha256(
            (self.exp["id"] + ":" + job["intent_id"] + ":c4-source").encode()
        ).hexdigest()
        with tempfile.TemporaryDirectory(prefix="et-c4-source-") as temporary:
            bundle = Path(temporary)
            gitworkspace.export(
                branch["repo"],
                commit,
                bundle / "source",
                max_archive_bytes=32 * 1024 * 1024,
                max_source_bytes=16 * 1024 * 1024,
            )
            (bundle / "intent.json").write_text(marker + "\n")
            (bundle / "job.sh").write_text(wrapper)
            (bundle / "job.sh").chmod(0o444)

            async def upload():
                await self.remote_command("mkdir", "-p", remote_root)
                existing = await self.remote_command(
                    "bash",
                    "-lc",
                    'if test -e "$1"; then if test -d "$1" && test -f "$1/intent.json"; then cat "$1/intent.json"; else printf conflict; fi; else printf absent; fi',
                    "stage",
                    final,
                )
                if existing.strip() != marker:
                    if existing.strip() != "absent":
                        raise ValueError("conflicting remote C4 source already exists")
                    pending = remote_root + "/.c4-source-" + identity + "." + uuid4().hex + ".pending"
                    await self.rsync("-rlt", str(bundle) + "/", layout["host"] + ":" + pending + "/")
                    await self.remote_command(
                        "bash",
                        "-lc",
                        'mv -Tn -- "$2" "$1" && test -d "$1" && test -f "$1/intent.json" && test "$(cat "$1/intent.json")" = "$3"',
                        "stage",
                        final,
                        pending,
                        marker,
                    )

            await self.transfer("up", upload, identity=identity)
        return {"state": "staged", "intent_id": job["intent_id"], "remote_path": final}

    def _saved_research_send(self, job, receipt):
        from .research.admission import SubmitJobRequest, preflight
        from .research.tools import require_live_research

        intent = job.get("intent_id")
        saved = self.store.one("SELECT * FROM jobs WHERE intent_id=?", (intent,))
        if not saved or saved["experiment_id"] != self.exp["id"] or saved["state"] != "submitting":
            raise ValueError("remote send requires this exact submitting job intent")
        saved = dict(saved)
        manifest = json.loads(saved.get("execution_manifest") or "{}")
        request = SubmitJobRequest.model_validate(manifest.get("research_request"))
        if (
            str(request.request_id) != intent
            or manifest.get("execution", {}).get("context") != "jean-zay"
            or manifest.get("commit") != saved.get("snapshot_hash")
            or manifest.get("research_action_id") is None
        ):
            raise ValueError("remote send requires the exact saved research intent and route")
        saved_layout = manifest.get("execution", {}).get("layout")
        if not isinstance(saved_layout, dict) or not isinstance(saved_layout.get("remote_run"), str):
            raise ValueError("remote send requires the saved execution layout")
        expected_receipt = {
            "state": "staged",
            "intent_id": intent,
            "remote_path": saved_layout["remote_run"] + "/c4-source",
        }
        if receipt != expected_receipt:
            raise ValueError("remote send requires the exact pinned staging receipt")
        transfer_id = hashlib.sha256((self.exp["id"] + ":" + intent + ":c4-source").encode()).hexdigest()
        upload = self.store.one("SELECT * FROM execution_transfers WHERE id=?", (transfer_id,))
        expected_payload = {
            "settings": ExecutionSettings.model_validate(manifest["execution"]["settings"]).model_dump(),
            "layout": saved_layout,
        }
        if (
            not upload
            or upload["experiment_id"] != self.exp["id"]
            or upload["direction"] != "up"
            or upload["state"] != "complete"
            or upload["payload"] != encode(expected_payload)
        ):
            raise ValueError("remote send requires the exact completed saved source upload")
        action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (manifest["research_action_id"],))
        if not action or action["experiment_id"] != self.exp["id"]:
            raise ValueError("remote send requires the exact approved research action")
        require_live_research(self.store.experiment(self.exp["id"]), action)
        preflight(
            self.workflow,
            self.exp["id"],
            action,
            request,
            remote_source=True,
            existing_intent=intent,
        )
        from .research.job_observe import validate_remote_route

        settings, layout = validate_remote_route(
            self.exp, intent, manifest["execution"], saved.get("execution_root")
        )
        if settings.model_dump() != self.settings.model_dump() or layout != manifest["execution"]["layout"]:
            raise ValueError("scheduler settings differ from this job's saved execution route")
        self.layout = layout
        remote_script = Path(layout["remote_run"]) / "c4-source" / "job.sh"
        return intent, request, layout, remote_script

    async def _preflight_research_send(self, job, receipt):
        intent, request, layout, script = self._saved_research_send(job, receipt)
        await super().preflight(intent, script, Path(layout["remote_run"]), request.resources)
        # Preflight awaits cluster reads. Re-read the saved intent, pause and
        # authority gates immediately before any subsequent submission effect.
        return self._saved_research_send(job, receipt)

    async def preflight_research_job(self, job, receipt):
        await self._preflight_research_send(job, receipt)

    async def submit_research_job(self, job, receipt):
        """Send one preflighted exact intent; every error from this call is uncertain.

        Callers must not retry this effect without reconciling the saved intent.
        """
        intent, request, layout, script = self._saved_research_send(job, receipt)
        self.workflow.remote_reads.clear()
        return await super().submit(intent, script, Path(layout["remote_run"]), resources=request.resources)

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
        self.workflow.remote_reads.clear()
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

    async def fetch_result(self, job):
        """Deliver a declared C4 result after saved successful Slurm accounting."""
        if (self.exp.get("contract") or {}).get("execution_contract") != 4:
            raise ValueError("remote result delivery requires saved execution contract 4")
        intent = job.get("intent_id")
        saved_job = self.store.one("SELECT * FROM jobs WHERE intent_id=?", (intent,))
        # Delivery requires the exact durable successful-accounting tuple while the
        # workflow still owns the submitted job pending result accounting.
        if (
            not saved_job
            or saved_job["experiment_id"] != self.exp["id"]
            or saved_job["state"] != "submitted"
            or saved_job["scheduler_state"] != "COMPLETED"
            or saved_job["exit_code"] != "0:0"
            or saved_job["ended_at"] is None
            or not saved_job["job_id"]
            or saved_job["job_id"] != job.get("job_id")
        ):
            raise ValueError("remote result requires saved successful terminal accounting")
        job = dict(saved_job)
        manifest = json.loads(job.get("execution_manifest") or "{}")
        if manifest.get("executor") == "agent":
            raise ValueError("remote result requires a saved Slurm job")
        request = manifest.get("research_request") or {}
        from .research.result_path import validate_remote_result_path

        name = validate_remote_result_path(request.get("result_file"))
        root = await asyncio.to_thread(exact_research_attempt, self.workflow, self.exp["id"], job)
        execution = manifest.get("execution") or {}
        saved_settings = ExecutionSettings.model_validate(execution.get("settings"))
        saved_layout = execution.get("layout")
        if not isinstance(saved_layout, dict):
            raise ValueError("remote result requires this job's saved execution layout")
        if (
            saved_settings.context != "jean-zay"
            or saved_layout.get("host") != saved_settings.ssh_host
            or saved_layout.get("local_run") != str(root)
            or not Path(saved_layout.get("remote_run", "")).is_absolute()
            or not Path(saved_layout.get("remote_project", "")).is_absolute()
        ):
            raise ValueError("saved execution route differs from this job's exact attempt")
        if saved_settings.model_dump() != self.settings.model_dump():
            raise ValueError("scheduler settings differ from this job's saved execution route")
        self.layout = saved_layout
        identity = hashlib.sha256((self.exp["id"] + ":" + intent + ":c4-result").encode()).hexdigest()
        saved = self.store.one("SELECT * FROM execution_transfers WHERE id=?", (identity,))
        if saved:
            if saved["experiment_id"] != self.exp["id"] or saved["direction"] != "down":
                raise ValueError("transfer identity changed")
            payload = json.loads(saved["payload"])
            layout = payload.get("layout") or {}
            if (
                payload.get("settings") != saved_settings.model_dump()
                or payload.get("result_path") != name
                or layout != saved_layout
                or layout.get("local_run") != str(root)
                or layout.get("host") != self.settings.ssh_host
            ):
                raise ValueError("saved transfer route differs from this job or declared result")
            self.layout = layout
        else:
            layout = saved_layout
        remote = Path(layout["remote_run"]) / name
        target = root / name
        if saved and saved["state"] == "complete":
            return await asyncio.to_thread(artifacts.allowed_path, target, root)

        async def download():
            # A private fresh directory prevents collisions and rsync --max-size from
            # leaving a prior attempt's bytes at its deterministic destination.
            temporary_dir = Path(await asyncio.to_thread(tempfile.mkdtemp, prefix=".result-", dir=root))
            temporary = temporary_dir / "result"
            try:
                await self.rsync("-lt", "--max-size=10m", layout["host"] + ":" + str(remote), str(temporary))
                return await asyncio.to_thread(place_research_result, temporary, target, root)
            finally:
                await asyncio.to_thread(shutil.rmtree, temporary_dir, True)

        return await self.transfer("down", download, identity=identity, result_path=name)

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
            downloads = self.workflow.remote_downloads
            key = (self.exp["id"], self.exp["evidence_revision"], self.settings.rsync_down)
            if terminal or clock.now() - downloads.get(key, 0) >= self.settings.poll_seconds:
                downloads[key] = clock.now()
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
            (pending / receipt_name).replace(root / receipt_name)

        # A repeated preview refresh replays this job's own saved row instead of a
        # fresh UUID; a terminal delivery supersedes that exact preview row.
        preview = hashlib.sha256((job["intent_id"] + ":preview").encode()).hexdigest()
        delivery = hashlib.sha256((job["intent_id"] + ":terminal-delivery").encode()).hexdigest()
        await self.transfer(
            "down",
            download,
            identity=delivery if terminal else preview,
            supersedes=preview if terminal else None,
            superseded_by=None if terminal else delivery,
        )
