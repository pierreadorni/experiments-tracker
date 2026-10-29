import asyncio
import contextlib
import logging
import secrets
import time
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.trustedhost import TrustedHostMiddleware

from . import artifacts
from .cli import runtime
from .knowledge import KnowledgeCommand
from .models import (
    ActionCommand,
    AnswerCommand,
    CreateExperiment,
    CreateProject,
    DeleteCommand,
    DiscussCommand,
    NoteCommand,
    PlanReviewCommand,
    PresentCommand,
    ReserveCommand,
    SteerCommand,
    UpdateProjectSettings,
)
from .store import Conflict

log = logging.getLogger(__name__)


def create_app(config, *, components=None):
    token = config.api_token_file.read_text().strip()
    if len(token) < 32:
        raise ValueError("API token must contain at least 32 characters")

    @asynccontextmanager
    async def lifespan(app):
        lock, store, workflow = components or runtime(config)
        app.state.store = store
        app.state.workflow = workflow
        app.state.last_tick = None
        app.state.started_at = time.time()
        app.state.last_tick_error = None
        worker = bridge = bridge_task = None

        async def workflow_status():
            projects = await asyncio.to_thread(workflow.projects.catalog)
            return workflow.status(app.state.last_tick, app.state.last_tick_error, projects=projects)

        app.state.workflow_status = workflow_status

        async def loop():
            while True:
                try:
                    await workflow.tick()
                    app.state.last_tick = time.time()
                    app.state.last_tick_error = None
                except Exception:
                    app.state.last_tick_error = "executor loop failed; inspect daemon logs"
                    log.exception("executor loop failed")
                await asyncio.sleep(min(config.scheduler_seconds, 1))

        try:
            await workflow.start()
            worker = asyncio.create_task(loop())
            if config.gateway.url:
                from .gateway_client import GatewayClient

                bridge = GatewayClient(
                    config,
                    store,
                    status_provider=workflow_status,
                )
                bridge_task = asyncio.create_task(bridge.run())
            yield
        finally:
            try:
                for task in (worker, bridge_task):
                    if task:
                        task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await task
                if bridge:
                    await bridge.close()
            finally:
                try:
                    await workflow.close()
                finally:
                    store.close()
                    if lock:
                        lock.close()

    app = FastAPI(
        title="ExperimentTracker",
        version="0.1.0",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=["127.0.0.1", "localhost", "testserver"])

    async def authorize(request: Request):
        bearer = request.headers.get("authorization", "")
        supplied = (
            bearer.removeprefix("Bearer ")
            if bearer.startswith("Bearer ")
            else request.cookies.get("et_session", "")
        )
        if not secrets.compare_digest(supplied, token):
            raise HTTPException(401, "Sign in with your local API token")
        if request.method not in ("GET", "HEAD", "OPTIONS") and not bearer.startswith("Bearer "):
            origin = request.headers.get("origin")
            expected = f"{request.url.scheme}://{request.headers.get('host')}"
            if origin != expected:
                raise HTTPException(403, "Same-origin request required")

    def store():
        return app.state.store

    def experiment_id(eid: UUID, *, allow_deleted=False):
        try:
            exp = store().experiment(str(eid))
            if exp["deleted_at"] and not allow_deleted:
                raise HTTPException(410, "Experiment deleted")
            return exp
        except KeyError:
            raise HTTPException(404, "Unknown experiment") from None

    @app.middleware("http")
    async def headers(request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        if request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store"
        else:
            response.headers["Content-Security-Policy"] = (
                "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; "
                "connect-src 'self'; frame-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'"
            )
        return response

    @app.get("/healthz")
    async def health(response: Response):
        tick = app.state.last_tick
        fresh = (
            tick is None
            and time.time() - app.state.started_at < 60
            or (tick is not None and time.time() - tick < max(60, config.scheduler_seconds * 4))
        )
        ok = fresh and app.state.last_tick_error is None
        response.status_code = 200 if ok else 503
        return {"ok": ok}

    @app.post("/api/session", dependencies=[Depends(authorize)])
    async def login(response: Response):
        response.set_cookie(
            "et_session", token, httponly=True, samesite="strict", secure=False, max_age=43200
        )
        return {"schema_version": 1, "authenticated": True}

    @app.delete("/api/session", dependencies=[Depends(authorize)])
    async def logout(response: Response):
        response.delete_cookie("et_session")
        return {"authenticated": False}

    @app.get("/api/status", dependencies=[Depends(authorize)])
    async def status():
        return {
            **await app.state.workflow_status(),
            "schema_version": 1,
            "mode": config.mode,
            "executor_id": config.executor_id,
            "gateway_configured": bool(config.gateway.url),
        }

    @app.post("/api/projects", dependencies=[Depends(authorize)], status_code=202)
    async def create_project(body: CreateProject):
        try:
            return app.state.workflow.projects.enqueue(body.model_dump(mode="json", exclude_none=True))
        except Conflict as exc:
            raise HTTPException(409, str(exc)) from None

    @app.get("/api/projects/requests/{request_id}", dependencies=[Depends(authorize)])
    async def project_request(request_id: UUID):
        try:
            return app.state.workflow.projects.receipt(request_id)
        except KeyError:
            raise HTTPException(404, "Unknown project request") from None

    @app.patch("/api/projects/{project_id}/settings", dependencies=[Depends(authorize)], status_code=202)
    async def update_project_settings(project_id: str, body: UpdateProjectSettings):
        try:
            return app.state.workflow.projects.enqueue_settings(project_id, body.model_dump(mode="json"))
        except Conflict as exc:
            raise HTTPException(409, str(exc)) from None

    @app.get("/api/projects/settings/requests/{request_id}", dependencies=[Depends(authorize)])
    async def project_settings_request(request_id: UUID):
        try:
            return app.state.workflow.projects.settings_receipt(request_id)
        except KeyError:
            raise HTTPException(404, "Unknown project settings request") from None

    @app.get("/api/projects/{project_id}/knowledge", dependencies=[Depends(authorize)])
    async def project_knowledge(project_id: str):
        try:
            return app.state.workflow.supervisor.memory.view(project_id)
        except Conflict as exc:
            raise HTTPException(404, str(exc)) from None

    @app.get("/api/projects/{project_id}/knowledge/reports/{version}", dependencies=[Depends(authorize)])
    async def project_report(project_id: str, version: int):
        report = app.state.workflow.supervisor.memory.report(project_id, version)
        if not report:
            raise HTTPException(404, "Unknown working report version")
        return {"schema_version": 1, "report": report}

    @app.post("/api/projects/{project_id}/knowledge", dependencies=[Depends(authorize)], status_code=202)
    async def project_knowledge_command(project_id: str, body: KnowledgeCommand):
        try:
            return app.state.workflow.supervisor.memory.enqueue(project_id, body.model_dump(mode="json"))
        except Conflict as exc:
            raise HTTPException(409, str(exc)) from None

    @app.get("/api/experiments", dependencies=[Depends(authorize)])
    async def experiments():
        return {
            "schema_version": 1,
            "experiments": [
                store().projection(row["id"])
                for row in store().rows(
                    "SELECT id FROM experiments WHERE deleted_at IS NULL ORDER BY created_at DESC"
                )
            ],
        }

    @app.post("/api/experiments", dependencies=[Depends(authorize)], status_code=202)
    async def create(body: CreateExperiment):
        try:
            return store().command(body.request_id, "create", body.model_dump(mode="json", exclude_none=True))
        except Conflict as exc:
            raise HTTPException(409, str(exc)) from None

    @app.get("/api/experiments/{eid}", dependencies=[Depends(authorize)])
    async def detail(eid: UUID):
        experiment_id(eid)
        return store().projection(str(eid))

    def apply(body, eid, kind):
        experiment_id(eid, allow_deleted=kind == "delete")
        try:
            return store().command(body.request_id, kind, body.model_dump(mode="json"), eid=str(eid))
        except Conflict as exc:
            raise HTTPException(409, str(exc)) from None

    @app.post("/api/experiments/{eid}/reports", dependencies=[Depends(authorize)])
    async def present(eid: UUID, command: PresentCommand):
        return apply(command, eid, "present")

    @app.post("/api/experiments/{eid}/conversation", dependencies=[Depends(authorize)], status_code=202)
    async def discuss(eid: UUID, command: DiscussCommand):
        return apply(command, eid, "discuss")

    @app.get("/api/experiments/{eid}/conversation", dependencies=[Depends(authorize)])
    async def conversation(eid: UUID, before: int | None = Query(default=None, ge=1, le=9223372036854775807)):
        experiment_id(eid)
        messages = store().conversation(str(eid), before)
        more = bool(
            messages
            and store().one(
                "SELECT 1 FROM discussions WHERE experiment_id=? AND seq<?", (str(eid), messages[0]["seq"])
            )
        )
        return {"schema_version": 1, "messages": messages, "has_more": more}

    @app.post("/api/experiments/{eid}/delete", dependencies=[Depends(authorize)])
    async def delete(eid: UUID, body: DeleteCommand):
        return apply(body, eid, "delete")

    @app.post("/api/experiments/{eid}/notes", dependencies=[Depends(authorize)])
    async def note(eid: UUID, body: NoteCommand):
        return apply(body, eid, "note")

    @app.post("/api/experiments/{eid}/actions", dependencies=[Depends(authorize)])
    async def action(eid: UUID, body: ActionCommand):
        return apply(body, eid, "action")

    @app.post("/api/experiments/{eid}/answers", dependencies=[Depends(authorize)])
    async def answer(eid: UUID, body: AnswerCommand):
        return apply(body, eid, "answer")

    @app.post("/api/experiments/{eid}/plan-review", dependencies=[Depends(authorize)])
    async def plan_review(eid: UUID, body: PlanReviewCommand):
        return apply(body, eid, "plan_review")

    @app.post("/api/experiments/{eid}/steering", dependencies=[Depends(authorize)])
    async def steer(eid: UUID, body: SteerCommand):
        return apply(body, eid, "steer")

    @app.post("/api/quota/reserve", dependencies=[Depends(authorize)])
    async def reserve(body: ReserveCommand):
        try:
            return store().command(body.request_id, "reserve", body.model_dump(mode="json"))
        except Conflict as exc:
            raise HTTPException(409, str(exc)) from None

    @app.get("/api/inbox", dependencies=[Depends(authorize)])
    async def inbox():
        return {
            "schema_version": 1,
            "entries": store().rows(
                "SELECT i.* FROM inbox i JOIN experiments e ON e.id=i.experiment_id WHERE e.deleted_at IS NULL ORDER BY i.created_at DESC"
            ),
        }

    @app.get("/api/commands/{request_id}", dependencies=[Depends(authorize)])
    async def receipt(request_id: UUID):
        row = store().one(
            "SELECT request_id,experiment_id,kind,state,received_at FROM commands WHERE request_id=?",
            (str(request_id),),
        )
        if not row:
            row = store().one(
                "SELECT request_id,experiment_id,kind,error,received_at FROM rejected_gateway_commands WHERE request_id=?",
                (str(request_id),),
            )
            if row:
                row["state"] = "rejected"
        if not row:
            raise HTTPException(404, "Unknown command")
        return {"schema_version": 1, **row}

    @app.get("/api/artifacts/{artifact_id}", dependencies=[Depends(authorize)])
    async def artifact(artifact_id: UUID):
        row = store().one("SELECT * FROM artifacts WHERE id=? AND local_valid=1", (str(artifact_id),))
        if not row:
            raise HTTPException(404, "Unknown or unavailable artifact")
        experiment_id(row["experiment_id"])
        try:
            path = artifacts.allowed_path(
                Path(row["path"]), app.state.workflow.projects.base_root(row["experiment_id"])
            )
            if artifacts.sha256(path) != row["sha256"]:
                raise ValueError("artifact content changed")
        except (ValueError, OSError):
            raise HTTPException(409, "Artifact integrity check failed") from None
        security = {
            "Content-Security-Policy": "sandbox allow-same-origin; default-src 'none'; style-src 'unsafe-inline'; img-src 'self'; font-src 'self'; base-uri 'none'"
        }
        return FileResponse(
            path,
            media_type=row["media_type"],
            headers=security,
            content_disposition_type="inline",
            filename=row["name"],
        )

    if config.web_dir.is_dir():
        app.mount("/", StaticFiles(directory=config.web_dir, html=True), name="web")
    return app
