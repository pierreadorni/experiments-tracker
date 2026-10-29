"""Read-only startup diagnostics; no ownership, scheduler dispatch or agent session."""

import os
import shutil
import stat
import subprocess


def diagnose(config):
    checks = []

    def record(name, ok, detail):
        checks.append({"name": name, "ok": bool(ok), "detail": detail})

    for name, path in (
        ("recipe", config.recipe_dir / "recipe.py"),
        ("built_frontend", config.web_dir / "index.html"),
    ):
        record(name, path.is_file(), "present" if path.is_file() else "missing; follow setup in the runbook")
    files = [("api_credential", config.api_token_file)]
    if config.gateway.url:
        files.append(("executor_credential", config.gateway.credential_file))
        if config.gateway.sites_credential_file:
            files.append(("sites_credential", config.gateway.sites_credential_file))
    for name, path in files:
        try:
            info = path.lstat()
            safe = (
                stat.S_ISREG(info.st_mode)
                and info.st_uid == os.geteuid()
                and info.st_mode & stat.S_IRUSR
                and not info.st_mode & 0o077
            )
            record(
                name,
                safe and info.st_size > 0,
                "private nonempty file"
                if safe and info.st_size
                else "requires a nonempty regular file owned by you with mode 600",
            )
        except OSError:
            record(name, False, "missing or unreadable; credential contents are never inspected")
    storage = config.state_dir
    while not storage.exists() and storage != storage.parent:
        storage = storage.parent
    try:
        filesystem = subprocess.run(
            ["stat", "-f", "-c", "%T", str(storage)], check=True, capture_output=True, text=True, timeout=5
        ).stdout.strip()
        record("state_storage", filesystem not in ("nfs", "nfs4", "cifs", "smb2"), filesystem)
    except (OSError, subprocess.SubprocessError):
        record("state_storage", False, "filesystem could not be established")
    if config.mode == "live":
        for name in ("sbatch", "squeue", "sacct", "sinfo", "scontrol", "sacctmgr"):
            record(
                name, shutil.which(name) is not None, "on PATH" if shutil.which(name) else "missing from PATH"
            )
        try:
            version = subprocess.run(
                [config.codex.executable, "--version"], check=True, capture_output=True, text=True, timeout=5
            ).stdout.strip()
            record("codex_version", version == f"codex-cli {config.codex.expected_version}", version)
        except (OSError, subprocess.SubprocessError):
            record("codex_version", False, "Codex executable unavailable")
    return {
        "ok": all(check["ok"] for check in checks),
        "mode": config.mode,
        "new_execution_contract": 3,
        "execution_scope": "future brief-driven experiments use bounded source copies and direct outputs; saved contracts keep their semantics",
        "checks": checks,
        "scope": "read-only prerequisites; account/quota, partition authorization, service policy and phone sign-in are not verified",
    }
