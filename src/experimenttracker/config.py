from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


SLURM_NAME = r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,99}$"


class JobResources(StrictModel):
    """One allocation. CPU count is per task, memory per node, GPU count per job."""

    partition: str = Field(default="shortrun", pattern=SLURM_NAME)
    account: str | None = Field(default=None, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.@-]{0,99}$")
    qos: str | None = Field(default=None, pattern=SLURM_NAME)
    cpus_per_job: int = Field(default=1, ge=1, strict=True)
    memory_mb_per_job: int = Field(default=1024, ge=1, strict=True)
    walltime_minutes_per_job: int = Field(default=5, ge=1, strict=True)
    gpus_per_job: int = Field(default=0, ge=0, strict=True)
    gpu_type: str | None = Field(default=None, pattern=SLURM_NAME)
    nodes: int = Field(default=1, ge=1, strict=True)
    tasks_per_node: int = Field(default=1, ge=1, strict=True)
    constraint: str | None = Field(default=None, pattern=r"^[a-zA-Z0-9_.,&|!*+()\[\]-]{1,200}$")
    exclusive: bool = False

    @model_validator(mode="after")
    def coherent_gpu_request(self):
        if self.gpu_type and not self.gpus_per_job:
            raise ValueError("GPU type requires a positive GPU count")
        if self.gpus_per_job and self.gpus_per_job % self.nodes:
            raise ValueError("GPU count must divide evenly across nodes")
        return self


class ResourceLimits(StrictModel):
    """Optional owner limits; None delegates the bound to the Slurm association."""

    partitions: list[str] | None = None
    accounts: list[str] | None = None
    qos: list[str] | None = None
    cpus_per_job: int | None = Field(default=None, ge=1, strict=True)
    memory_mb_per_job: int | None = Field(default=None, ge=1, strict=True)
    walltime_minutes_per_job: int | None = Field(default=None, ge=1, strict=True)
    gpus_per_job: int | None = Field(default=None, ge=0, strict=True)
    nodes: int | None = Field(default=None, ge=1, strict=True)
    tasks_per_node: int | None = Field(default=None, ge=1, strict=True)
    gpu_hours_per_experiment: float | None = Field(default=None, gt=0, allow_inf_nan=False)

    def validate_request(self, resources: JobResources):
        for name in ("partition", "account", "qos"):
            allowed = getattr(self, {"partition": "partitions", "account": "accounts", "qos": "qos"}[name])
            if allowed is not None and getattr(resources, name) not in allowed:
                raise ValueError(f"requested {name} is outside the executor policy")
        for name in (
            "cpus_per_job",
            "memory_mb_per_job",
            "walltime_minutes_per_job",
            "gpus_per_job",
            "nodes",
            "tasks_per_node",
        ):
            limit = getattr(self, name)
            if limit is not None and getattr(resources, name) > limit:
                raise ValueError(f"requested {name} exceeds the executor policy ({limit})")


class SlurmConfig(JobResources):
    # These values are fallbacks for legacy plans; they are not limits on agent requests.
    max_jobs_per_experiment: int | None = Field(default=None, ge=1, strict=True)
    max_concurrent_jobs: int | None = Field(default=1, ge=1, strict=True)
    max_repairs_per_experiment: int | None = Field(default=None, ge=0, strict=True)
    limits: ResourceLimits = ResourceLimits()

    def defaults(self):
        return JobResources.model_validate(self.model_dump(include=set(JobResources.model_fields)))


class CodexConfig(StrictModel):
    executable: str = "codex"
    expected_version: str = "0.156.1"
    model: str | None = None
    # This is an inactivity window, refreshed by actual agent/tool progress.
    turn_timeout_seconds: int = Field(default=1800, ge=10)
    turn_max_seconds: int | None = Field(default=None, ge=10)
    max_agent_turns: int | None = Field(default=None, ge=1)
    max_sessions: int = Field(default=1, ge=1, le=8)
    identical_error_limit: int = Field(default=3, ge=2, le=20)
    monitor_interval_seconds: int = Field(default=900, ge=60, le=86400)
    monitor_stall_seconds: int = Field(default=900, ge=60)
    monitor_max_turns_per_job: int = Field(default=2, ge=0)
    interrupt_grace_seconds: int = Field(default=30, ge=1, le=30)
    rpc_timeout_seconds: int = Field(default=30, ge=1, le=120)
    session_timeout_seconds: int = Field(default=120, ge=1, le=600)


class QuotaConfig(StrictModel):
    five_hour_min_remaining: float = Field(default=25, ge=0, le=100)
    weekly_min_remaining: float = Field(default=10, ge=0, le=100)
    recovery_remaining: tuple[float, float] = (30, 15)
    snapshot_max_age_seconds: int = Field(default=60, ge=1, le=60)
    bucket: str = "codex"

    @model_validator(mode="after")
    def valid_recovery(self):
        thresholds = (self.five_hour_min_remaining, self.weekly_min_remaining)
        if any(
            not threshold <= recovery <= 100
            for threshold, recovery in zip(thresholds, self.recovery_remaining, strict=True)
        ):
            raise ValueError("quota recovery thresholds must be between the admission reserve and 100")
        return self


class GatewayConfig(StrictModel):
    url: str | None = None
    credential_file: Path | None = None
    sites_credential_file: Path | None = None
    poll_seconds: float = Field(default=5, ge=1)

    @model_validator(mode="after")
    def valid_origin_and_credentials(self):
        if self.url:
            parsed = urlsplit(self.url)
            if (
                parsed.scheme != "https"
                or not parsed.hostname
                or parsed.username
                or parsed.password
                or parsed.query
                or parsed.fragment
                or parsed.path not in ("", "/")
            ):
                raise ValueError(
                    "gateway URL must be an HTTPS origin without credentials, path, query or fragment"
                )
            if not self.credential_file:
                raise ValueError("gateway credential file is required")
        for file in (self.credential_file, self.sites_credential_file):
            if file and not file.is_absolute():
                raise ValueError("gateway credential paths must be absolute")
        return self


class ProjectConfig(StrictModel):
    id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    name: str = Field(min_length=1, max_length=100)
    checkout: Path | None = None
    writable: bool = True

    @model_validator(mode="after")
    def valid_checkout(self):
        if self.checkout and not self.checkout.is_absolute():
            raise ValueError("project checkout must be absolute")
        if self.id != "test-project" and not self.checkout:
            raise ValueError("research projects need a checkout")
        return self

    def public(self):
        return {
            "id": self.id,
            "name": self.name,
            "available": self.checkout is None or self.checkout.is_dir(),
            "writable": bool(self.checkout and self.writable),
            **({"folder": str(self.checkout.resolve())} if self.checkout else {}),
        }


class Config(StrictModel):
    schema_version: Literal[1] = 1
    executor_id: str = "acacia"
    mode: Literal["live", "demo"] = "demo"
    state_dir: Path
    job_output_root: Path
    backup_dir: Path
    recipe_dir: Path
    web_dir: Path
    host: Literal["127.0.0.1"] = "127.0.0.1"
    port: int = Field(default=8765, ge=1024, le=65535)
    api_token_file: Path
    scheduler_seconds: float = Field(default=30, ge=0.1)
    projects_root: Path | None = None
    projects: list[ProjectConfig] = Field(
        default_factory=lambda: [ProjectConfig(id="test-project", name="Test project")],
        min_length=1,
        max_length=50,
    )
    slurm: SlurmConfig = SlurmConfig()
    codex: CodexConfig = CodexConfig()
    quota: QuotaConfig = QuotaConfig()
    gateway: GatewayConfig = GatewayConfig()

    @model_validator(mode="after")
    def absolute_paths(self):
        for key in ("state_dir", "job_output_root", "backup_dir", "recipe_dir", "web_dir", "api_token_file"):
            if not getattr(self, key).is_absolute():
                raise ValueError(f"{key} must be absolute")
        if self.projects_root and not self.projects_root.is_absolute():
            raise ValueError("projects_root must be absolute")
        ids = [project.id for project in self.projects]
        if len(set(ids)) != len(ids) or "test-project" not in ids:
            raise ValueError("projects must have unique IDs and include test-project for legacy experiments")
        return self


def load_config(path: Path) -> Config:
    return Config.model_validate(yaml.safe_load(path.read_text()))
