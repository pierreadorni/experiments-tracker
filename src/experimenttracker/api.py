import asyncio
import contextlib
import secrets
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field
from starlette.middleware.trustedhost import TrustedHostMiddleware

from . import artifacts, clock
from .cli import runtime
from .engine.runner import Runner
from .models import (
    ActionCommand,
    CreateExperiment,
    IntegrateCommand,
    PlanReviewCommand,
    PresentCommand,
)
from .publish.commands import Services, dispatch, saved_receipt
from .publish.commands import receipt as command_receipt
from .store import Conflict


class CommandRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: UUID
    kind: str = Field(pattern=r"^[a-z_]{1,40}$")
    target: str | None = Field(default=None, max_length=200)
    payload: dict = Field(default_factory=dict)


def create_app(config, *, components=None, config_path=None, current_release=None):
    token = config.api_token_file.read_text().strip()
    if len(token) < 32:
        raise ValueError("API token must contain at least 32 characters")

    @asynccontextmanager
    async def lifespan(app):
        lock, store, workflow = components or runtime(config)
        from .maintenance import Maintenance

        workflow.maintenance = Maintenance(config, store, config_path=config_path, current=current_release)
        app.state.store = store
        app.state.workflow = workflow
        app.state.started_at = clock.now()
        runner = Runner(workflow)
        app.state.runner = runner
        app.state.health = runner.health
        bridge = bridge_task = None

        async def workflow_status():
            last_tick, executor_error = app.state.health()
            projects = await asyncio.to_thread(workflow.projects.catalog)
            return workflow.status(last_tick, executor_error, projects=projects)

        app.state.workflow_status = workflow_status

        try:
            await runner.start()
            if config.gateway.url:
                from .gateway_client import GatewayClient

                bridge = GatewayClient(
                    config,
                    store,
                    status_provider=workflow_status,
                )
                bridge.maintenance_provider = lambda: workflow.maintenance
                bridge.maintenance_workflow = workflow
                bridge_task = asyncio.create_task(bridge.run())
            yield
        finally:
            try:
                # Stop scheduling and finish owned passes, then close the bridge, the
                # workflow and the store: no task may write after the store closes.
                await runner.stop()
                if bridge_task:
                    bridge_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await bridge_task
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
        tick, executor_error = app.state.health()
        fresh = (
            tick is None
            and clock.now() - app.state.started_at < 60
            or (tick is not None and clock.now() - tick < max(60, config.scheduler_seconds * 4))
        )
        ok = fresh and executor_error is None
        response.status_code = 200 if ok else 503
        return {"ok": ok}

    @app.post("/api/session", dependencies=[Depends(authorize)])
    async def login(response: Response):
        response.set_cookie(
            "et_session", token, httponly=True, samesite="strict", secure=False, max_age=43200
        )
        return {"schema_version": 1, "authenticated": True}

    @app.get("/api/session", dependencies=[Depends(authorize)])
    async def session():
        return {"schema_version": 1, "authenticated": True, "host": "local"}

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

    from .maintenance import InstallCommand, ReleaseUpdate, StepUpdate, blockers, identities

    @app.get("/api/maintenance", dependencies=[Depends(authorize)])
    async def maintenance_status():
        return {"schema_version": 1, **app.state.workflow.maintenance.status(app.state.workflow)}

    @app.post("/api/maintenance/install", dependencies=[Depends(authorize)], status_code=202)
    async def maintenance_install(body: InstallCommand):
        try:
            return app.state.workflow.maintenance.install(body)
        except Conflict as exc:
            raise HTTPException(409, str(exc)) from None

    @app.post("/api/maintenance/releases", dependencies=[Depends(authorize)])
    async def maintenance_release(body: ReleaseUpdate):
        try:
            await asyncio.to_thread(app.state.workflow.maintenance.register, body)
            return {"schema_version": 1, "accepted": body.commit_id}
        except (ValueError, OSError) as exc:
            raise HTTPException(
                409, "Release registration failed; inspect local release and test receipt"
            ) from exc

    @app.get("/api/maintenance/internal", dependencies=[Depends(authorize)])
    async def maintenance_internal():
        last_tick, executor_error = app.state.health()
        return {
            "schema_version": 1,
            "maintenance": app.state.workflow.maintenance.status(app.state.workflow),
            "boot_id": app.state.workflow.maintenance.boot_id,
            "last_tick": last_tick,
            "started_at": app.state.started_at,
            "executor_error": executor_error,
            "agent_ready": app.state.workflow.agent_ready,
            # Compatibility for a pre-T5.5 coordinator; T5.5e deletes it.
            "identities": identities(store()),
        }

    @app.post("/api/maintenance/step", dependencies=[Depends(authorize)])
    async def maintenance_step(body: StepUpdate):
        try:
            app.state.workflow.maintenance.step(body)
            return {"schema_version": 1, "accepted": str(body.request_id)}
        except Conflict as exc:
            raise HTTPException(409, str(exc)) from None

    @app.post("/api/maintenance/stop", dependencies=[Depends(authorize)])
    async def maintenance_stop(body: InstallCommand):
        op = store().one("SELECT * FROM maintenance WHERE request_id=?", (str(body.request_id),))
        if not op or op["release_id"] != body.release_id or op["state"] != "restarting":
            raise HTTPException(409, "No matching authorized restart")
        if blockers(app.state.workflow) or not getattr(app.state, "request_shutdown", None):
            raise HTTPException(409, "Executor is not at a safe shutdown boundary")
        app.state.request_shutdown()
        return {"schema_version": 1, "accepted": str(body.request_id)}

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

    @app.post("/api/experiments/{eid}/actions", dependencies=[Depends(authorize)])
    async def action(eid: UUID, body: ActionCommand):
        return apply(body, eid, "action")

    @app.post("/api/experiments/{eid}/plan-review", dependencies=[Depends(authorize)])
    async def plan_review(eid: UUID, body: PlanReviewCommand):
        return apply(body, eid, "plan_review")

    @app.post("/api/experiments/{eid}/integrate", dependencies=[Depends(authorize)])
    async def integrate(eid: UUID, body: IntegrateCommand):
        return apply(body, eid, "integrate")

    @app.get("/api/inbox", dependencies=[Depends(authorize)])
    async def inbox():
        return {
            "schema_version": 1,
            "entries": store().rows(
                "SELECT i.* FROM inbox i JOIN experiments e ON e.id=i.experiment_id WHERE e.deleted_at IS NULL ORDER BY i.created_at DESC"
            )
            + store().rows(
                "SELECT n.id,'' AS experiment_id,r.project_id,n.kind,n.text,n.created_at FROM autopilot_notices n JOIN autopilot_runs r ON r.id=n.run_id WHERE n.resolved_at IS NULL ORDER BY n.created_at DESC LIMIT 100"
            ),
        }

    @app.post("/api/commands", dependencies=[Depends(authorize)])
    async def post_command(body: CommandRequest):
        workflow = app.state.workflow
        services = Services(
            store(),
            projects=lambda: workflow.projects,
            memory=lambda: workflow.supervisor.memory,
            autopilot=lambda: workflow.research.service,
            maintenance=lambda: workflow.maintenance,
        )
        received = clock.now()
        outcome = dispatch(
            services, str(body.request_id), body.kind, body.target, body.payload, origin="local"
        )
        return command_receipt(str(body.request_id), body.kind, body.target, outcome, received)

    @app.get("/api/commands", dependencies=[Depends(authorize)])
    async def pending_commands(state: str = Query(pattern="^pending$")):
        # Local commands settle before POST returns, so nothing waits for an executor here.
        return {"schema_version": 1, "commands": []}

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
            saved = saved_receipt(store(), request_id)
            if not saved:
                raise HTTPException(404, "Unknown command")
            return saved
        return {"schema_version": 1, **row}

    async def projections():
        from .publish.projections import Projections

        workflow = app.state.workflow
        status = {
            **await app.state.workflow_status(),
            "schema_version": 1,
            "mode": config.mode,
            "executor_id": config.executor_id,
        }
        return Projections(
            store(),
            status=status,
            catalog=status["projects"],
            memory=lambda: workflow.supervisor.memory,
            autopilot=lambda: workflow.research.service,
            maintenance=lambda: workflow.maintenance.status(workflow),
        )

    @app.get("/api/projections", dependencies=[Depends(authorize)])
    async def projection_list(
        prefix: str = Query(default="", max_length=100),
        before: str = Query(default="\uffff", max_length=200),
        limit: int = Query(default=50, ge=1, le=100),
    ):
        # The hosted list: newest key first, each page resumed with the last key seen.
        built = await projections()
        keys = sorted((k for k in built.keys(prefix) if k < before), reverse=True)[:limit]
        return {"schema_version": 1, "items": [p for key in keys if (p := built.build(key))]}

    @app.get("/api/projections/{key}", dependencies=[Depends(authorize)])
    async def projection(key: str):
        result = (await projections()).build(key)
        if not result:
            raise HTTPException(404, "Unknown projection")
        return {"schema_version": 1, **result}

    @app.get("/api/artifacts/{artifact_id}", dependencies=[Depends(authorize)])
    @app.get("/api/blobs/{artifact_id}", dependencies=[Depends(authorize)])
    async def artifact(artifact_id: UUID):
        row = store().one("SELECT * FROM artifacts WHERE id=? AND local_valid=1", (str(artifact_id),))
        if not row:
            # Autopilot downloads share the blob id space with experiment artifacts.
            item = (await projections()).autopilot_blob(str(artifact_id))
            if not item:
                raise HTTPException(404, "Unknown or unavailable artifact")
            return Response(
                item["read"](),
                media_type=item["media_type"],
                headers={
                    "Content-Security-Policy": "sandbox allow-same-origin; default-src 'none'",
                    "Content-Disposition": f'inline; filename="{item["name"]}"',
                },
            )
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
