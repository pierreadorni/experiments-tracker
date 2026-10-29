"""Durable project registration. Save the filesystem intent before creating a folder."""

import json
import os
import re
import time
from pathlib import Path
from uuid import UUID

from .config import ProjectConfig
from .execution_settings import ExecutionSettings
from .models import CreateProject, UpdateProjectSettings
from .store import Conflict, encode


class ProjectUnavailable(ValueError):
    pass


class Projects:
    def __init__(self, config, store):
        self.config, self.store = config, store
        self.reload()

    def reload(self):
        self.store.projects = {p.id: p for p in self.config.projects}
        for row in self.store.rows("SELECT * FROM projects ORDER BY created_at"):
            self.store.projects[row["id"]] = ProjectConfig(
                id=row["id"], name=row["name"], checkout=row["folder"]
            )

        for row in self.store.rows(
            "SELECT project_id,result FROM project_settings_requests WHERE state='applied'"
        ):
            if json.loads(row["result"]).get("deleted"):
                self.store.projects.pop(row["project_id"], None)

    def catalog(self):
        from .workspaces import git_available

        result = []
        for project in tuple(self.store.projects.values()):
            item = project.public()
            row = self.store.one("SELECT * FROM projects WHERE id=?", (project.id,))
            if row:
                item["available"] = self.matches(Path(row["folder"]), row)
                item["non_empty"] = bool(row["non_empty"])
            item["supports_worktrees"] = git_available(project.checkout) if item["available"] else False
            item.update(self.settings(project.id))
            result.append(item)
        return result

    def settings(self, project_id):
        row = self.store.one(
            "SELECT instructions,revision,execution FROM project_preferences WHERE project_id=?",
            (project_id,),
        )
        return (
            {
                "instructions": row["instructions"],
                "revision": row["revision"],
                "execution": ExecutionSettings.model_validate_json(row["execution"]).model_dump(),
            }
            if row
            else {"instructions": "", "revision": 0, "execution": ExecutionSettings().model_dump()}
        )

    def instructions(self, project_id):
        return self.settings(project_id)["instructions"]

    def settings_receipt(self, request_id):
        row = self.store.one("SELECT * FROM project_settings_requests WHERE request_id=?", (str(request_id),))
        if not row:
            raise KeyError(request_id)
        return {
            "schema_version": 1,
            "request_id": row["request_id"],
            "project_id": row["project_id"],
            "state": row["state"] if row["state"] in ("applied", "rejected") else "waiting_for_executor",
            "error": row["error"],
            "settings": json.loads(row["result"]) if row["result"] else None,
        }

    def enqueue_settings(self, project_id, payload, *, origin="local"):
        payload = UpdateProjectSettings.model_validate(payload).model_dump(mode="json")
        if not payload["delete"]:
            payload.pop("delete")
        identity, wire = payload["request_id"], encode(payload)
        with self.store.tx() as db:
            prior = db.execute(
                "SELECT project_id,payload,origin FROM project_settings_requests WHERE request_id=?",
                (identity,),
            ).fetchone()
            if prior:
                if prior["project_id"] != project_id or prior["payload"] != wire or prior["origin"] != origin:
                    raise Conflict("request_id was already used for different project settings")
            else:
                if (
                    db.execute("SELECT 1 FROM project_turns WHERE id=?", (identity,)).fetchone()
                    or db.execute("SELECT 1 FROM commands WHERE request_id=?", (identity,)).fetchone()
                    or db.execute("SELECT 1 FROM project_requests WHERE request_id=?", (identity,)).fetchone()
                ):
                    raise Conflict("request_id was already used for another command")
                if project_id not in self.store.projects:
                    raise Conflict("project is unknown")
                db.execute(
                    "INSERT INTO project_settings_requests(request_id,project_id,payload,created_at,origin) "
                    "VALUES(?,?,?,?,?)",
                    (identity, project_id, wire, time.time(), origin),
                )
        return self.settings_receipt(identity)

    def settings_tick(self):
        for row in self.store.rows(
            "SELECT * FROM project_settings_requests WHERE state='pending' ORDER BY created_at,request_id LIMIT 20"
        ):
            try:
                if row["project_id"] not in self.store.projects:
                    raise ValueError("project is unknown")
                payload = json.loads(row["payload"])
                instructions = payload["instructions"]
                execution = payload.get("execution") or self.settings(row["project_id"])["execution"]
                with self.store.tx() as db:
                    if payload.get("delete"):
                        for exp in db.execute("SELECT contract FROM experiments WHERE deleted_at IS NULL"):
                            if (
                                json.loads(exp["contract"]).get("project", {}).get("id", "test-project")
                                == row["project_id"]
                            ):
                                raise ValueError(
                                    "Delete this project's experiments before deleting the project."
                                )
                        if db.execute(
                            "SELECT 1 FROM project_turns WHERE project_id=? AND state NOT IN ('completed','failed')",
                            (row["project_id"],),
                        ).fetchone():
                            raise ValueError(
                                "Wait for project agent work to finish before deleting the project."
                            )
                    current = db.execute(
                        "SELECT revision FROM project_preferences WHERE project_id=?", (row["project_id"],)
                    ).fetchone()
                    revision = current["revision"] + 1 if current else 1
                    result = {"instructions": instructions, "revision": revision, "execution": execution}
                    if payload.get("delete"):
                        result["deleted"] = True
                    db.execute(
                        "INSERT INTO project_preferences(project_id,instructions,revision,updated_at,execution) "
                        "VALUES(?,?,?,?,?) ON CONFLICT(project_id) DO UPDATE SET "
                        "instructions=excluded.instructions,revision=excluded.revision,updated_at=excluded.updated_at,execution=excluded.execution",
                        (row["project_id"], instructions, revision, time.time(), encode(execution)),
                    )
                    db.execute(
                        "UPDATE project_settings_requests SET state='applied',result=? WHERE request_id=?",
                        (encode(result), row["request_id"]),
                    )
                self.reload()
            except (ValueError, KeyError) as exc:
                with self.store.tx() as db:
                    db.execute(
                        "UPDATE project_settings_requests SET state='rejected',error=? WHERE request_id=?",
                        (str(exc)[:1000], row["request_id"]),
                    )

    @staticmethod
    def matches(folder, saved):
        try:
            info = folder.stat()
            return (
                folder.is_dir()
                and not folder.is_symlink()
                and (info.st_dev, info.st_ino) == (saved["device"], saved["inode"])
            )
        except OSError:
            return False

    def receipt(self, request_id):
        row = self.store.one("SELECT * FROM project_requests WHERE request_id=?", (str(request_id),))
        if not row:
            raise KeyError(request_id)
        return {
            "schema_version": 1,
            "request_id": row["request_id"],
            "project_id": row["project_id"],
            "state": row["state"] if row["state"] in ("applied", "rejected") else "waiting_for_executor",
            "error": row["error"],
            "project": json.loads(row["result"]) if row["result"] else None,
        }

    def enqueue(self, payload, *, origin="local"):
        payload = CreateProject.model_validate(payload).model_dump(mode="json", exclude_none=True)
        identity, wire = payload["request_id"], encode(payload)
        with self.store.tx() as db:
            prior = db.execute(
                "SELECT payload,origin FROM project_requests WHERE request_id=?", (identity,)
            ).fetchone()
            if prior:
                if prior["payload"] != wire or prior["origin"] != origin:
                    raise Conflict("request_id was already used for a different project")
            else:
                # Preserve one request namespace across project and experiment commands.
                if (
                    db.execute("SELECT 1 FROM project_turns WHERE id=?", (identity,)).fetchone()
                    or db.execute("SELECT 1 FROM commands WHERE request_id=?", (identity,)).fetchone()
                ):
                    raise Conflict("request_id was already used for another command")
                if db.execute(
                    "SELECT 1 FROM project_settings_requests WHERE request_id=?", (identity,)
                ).fetchone():
                    raise Conflict("request_id was already used for another command")
                pending = db.execute(
                    "SELECT COUNT(*) FROM project_requests WHERE state IN ('pending','creating')"
                ).fetchone()[0]
                if len(self.store.projects) + pending >= 50:
                    raise Conflict("at most 50 projects are supported")
                db.execute(
                    "INSERT INTO project_requests(request_id,project_id,payload,created_at,origin) VALUES(?,?,?,?,?)",
                    (identity, "project-" + UUID(identity).hex, wire, time.time(), origin),
                )
        return self.receipt(identity)

    def protected(self, folder):
        # A project must not make the whole host/home or executor state writable.
        if folder == Path.home().resolve() or len(folder.parts) < 3:
            raise ValueError("select a project folder, rather than the home or system directory")
        for path in (self.config.state_dir, self.config.api_token_file, self.config.backup_dir):
            path = path.resolve()
            if path.is_relative_to(folder) or folder.is_relative_to(path):
                raise ValueError("the selected folder overlaps protected executor files")
        if folder.name in (".ssh", ".aws", ".codex", ".experimenttracker"):
            raise ValueError("select a project folder")

    def apply(self, row):
        payload = json.loads(row["payload"])
        if row["folder"]:
            folder = Path(row["folder"])
        elif payload["mode"] == "existing":
            folder = Path(payload["folder"]).expanduser()
            if not folder.is_absolute():
                raise ValueError("existing folder must be absolute or start with ~/")
            folder = folder.resolve(strict=True)
        else:
            parent = (self.config.projects_root or self.config.job_output_root.parent / "projects").resolve()
            folder = parent / re.sub(r"[^a-zA-Z0-9._-]+", "-", payload["name"].strip())
        self.protected(folder)
        if any(p.checkout and p.checkout.resolve() == folder for p in self.store.projects.values()):
            raise ValueError("this folder already belongs to a project; select that project")
        if any(
            p.checkout
            and (folder.is_relative_to(p.checkout.resolve()) or p.checkout.resolve().is_relative_to(folder))
            for p in self.store.projects.values()
        ):
            raise ValueError("project folders must not overlap another registered project")
        with self.store.tx() as db:
            db.execute(
                "UPDATE project_requests SET folder=?,state='creating' WHERE request_id=?",
                (str(folder), row["request_id"]),
            )
        if payload["mode"] == "new":
            marker = folder / ".experimenttracker-project.json"
            if folder.exists():
                # Adopt only our exact saved creation identity, never an unrelated existing directory.
                if (
                    not marker.is_file()
                    or marker.is_symlink()
                    or json.loads(marker.read_text()) != {"request_id": row["request_id"]}
                ):
                    raise ValueError("new folder already exists; select it as an existing folder")
            else:
                if row["state"] == "creating":
                    raise ValueError(
                        "folder creation outcome is uncertain; inspect the saved project request"
                    )
                folder.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                folder.mkdir(mode=0o700)
                with marker.open("x") as stream:
                    stream.write(encode({"request_id": row["request_id"]}))
                    stream.flush()
                    os.fsync(stream.fileno())
                directory = os.open(folder, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
        if not folder.is_dir() or folder.is_symlink():
            raise ValueError("select an existing directory")
        if not os.access(folder, os.R_OK | os.W_OK | os.X_OK):
            raise ValueError("project folder must be readable and writable by the executor")
        if payload["mode"] == "new":
            from .workspaces import initialize_project

            initialize_project(folder)
        non_empty = any(
            p.name not in (".experimenttracker-project.json", ".git", ".gitignore") for p in folder.iterdir()
        )
        info = folder.stat()
        project = {
            "id": row["project_id"],
            "name": payload["name"],
            "available": True,
            "writable": True,
            "non_empty": non_empty,
            "folder": str(folder),
            "instructions": payload.get("instructions", ""),
            "revision": 1 if payload.get("instructions") else 0,
        }
        with self.store.tx() as db:
            db.execute(
                "INSERT INTO projects VALUES(?,?,?,?,?,?,?)",
                (
                    row["project_id"],
                    payload["name"],
                    str(folder),
                    info.st_dev,
                    info.st_ino,
                    int(non_empty),
                    time.time(),
                ),
            )
            db.execute(
                "UPDATE project_requests SET state='applied',error=NULL,result=? WHERE request_id=?",
                (encode(project), row["request_id"]),
            )
            if payload.get("instructions"):
                db.execute(
                    "INSERT INTO project_preferences(project_id,instructions,revision,updated_at) VALUES(?,?,1,?)",
                    (row["project_id"], payload["instructions"], time.time()),
                )
        self.reload()

    def tick(self):
        for row in self.store.rows(
            "SELECT * FROM project_requests WHERE state IN ('pending','creating') ORDER BY created_at LIMIT 5"
        ):
            try:
                self.apply(row)
            except (ValueError, OSError) as exc:
                with self.store.tx() as db:
                    db.execute(
                        "UPDATE project_requests SET state='rejected',error=? WHERE request_id=?",
                        (str(exc)[:1000], row["request_id"]),
                    )

    def root(self, eid):
        base = self.base_root(eid)
        repair = self.store.experiment(eid)["contract"].get("auto_repair")
        if not repair:
            return base
        contract = self.store.experiment(eid)["contract"]
        root = (
            base
            / ("records" if contract.get("execution_contract") == 3 else "repairs")
            / str(int(repair["revision"]))
        )
        if root.parent.is_symlink() or root.is_symlink():
            raise ProjectUnavailable("repair workspace cannot be a symlink")
        return root

    def base_root(self, eid):
        saved = self.workspace(eid)
        return Path(saved["root"]) if saved else self.config.job_output_root / eid

    def workspace(self, eid):
        row = self.store.one("SELECT * FROM experiment_workspaces WHERE experiment_id=?", (eid,))
        if not row:
            return None
        folder = Path(row["folder"])
        if not self.matches(folder, row):
            raise ProjectUnavailable("project folder is missing or its identity changed")
        root = Path(row["root"])
        for part in (folder / ".experimenttracker", root.parent, root):
            if part.is_symlink():
                raise ProjectUnavailable("experiment workspace cannot be a symlink")
        return row
