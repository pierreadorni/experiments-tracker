"""Project execution preferences, copied into experiments and submission records."""

import re
import shlex

from pydantic import Field, model_validator

from .config import StrictModel


class ExecutionSettings(StrictModel):
    context: str = Field(default="acacia", pattern=r"^(acacia|jean-zay)$")
    ssh_host: str = Field(default="jz", pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,99}$")
    remote_project: str = Field(default="", max_length=1000)
    rsync_up: str = Field(default="", max_length=8000)
    rsync_down: str = Field(default="", max_length=8000)
    poll_seconds: int = Field(default=600, ge=600, le=86400, strict=True)

    @model_validator(mode="after")
    def commands(self):
        # No shell evaluation: templates expand one argument at a time. In particular,
        # neither a pasted pipeline nor an rsync remote-shell override is executable.
        for value in (self.rsync_up, self.rsync_down):
            if not value.strip():
                continue
            args = shlex.split(value)
            if not args or args[0] != "rsync":
                raise ValueError("transfer commands must start with rsync")
            if any(
                arg in (";", "&&", "||", "|", ">", "<", "&", "-e")
                or arg.startswith(("--rsh", "--rsync-path", "--delete", "--remove-source-files"))
                or arg.startswith("-e")
                and not arg.startswith("--")
                for arg in args[1:]
            ):
                raise ValueError("rsync commands cannot use shell operators, shell overrides or deletion")
            for arg in args:
                for key in re.findall(r"\{([^{}]+)\}", arg):
                    if key not in {"local_project", "remote_project", "local_run", "remote_run", "host"}:
                        raise ValueError(f"unknown rsync placeholder: {key}")
        if self.remote_project and not re.fullmatch(
            r"(?:/|\$(?:WORK|SCRATCH|HOME)/)[a-zA-Z0-9_./-]+", self.remote_project
        ):
            raise ValueError(
                "remote project must be an absolute path or start with $WORK/, $SCRATCH/ or $HOME/"
            )
        if ".." in self.remote_project.split("/"):
            raise ValueError("remote project cannot contain parent traversal")
        return self
