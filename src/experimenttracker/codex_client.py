import asyncio
import contextlib
import hashlib
import json
import time
from collections.abc import Callable
from pathlib import Path

from .config import CodexConfig
from .models import request_answer
from .slurm import command


class RpcError(RuntimeError):
    pass


class RpcRejected(RpcError):
    """An explicit JSON-RPC error response, rather than a lost or invalid response."""


class NeedsAttention(RuntimeError):
    pass


class SessionUnavailable(RpcError):
    """The runtime rejected session resume before any turn was dispatched."""


class TerminalTurnError(RpcError):
    """The exact turn is confirmed terminal and may be continued."""

    def __init__(self, message, *, reason=None):
        super().__init__(message)
        self.reason = reason


class TransportError(RpcError, NeedsAttention):
    """Connection loss cannot establish whether a previously dispatched turn completed."""


def strict_output_schema(schema):
    """Structured output requires every property, including fields with Pydantic defaults."""
    if isinstance(schema, dict):
        result = {key: strict_output_schema(value) for key, value in schema.items() if key != "default"}
        if result.get("type") == "object":
            result["required"] = list(result.get("properties", {}))
            result["additionalProperties"] = False
        return result
    if isinstance(schema, list):
        return [strict_output_schema(value) for value in schema]
    return schema


class AppServer:
    """Pinned JSONL transport. Read notifications independently of outstanding RPC responses."""

    def __init__(self, config: CodexConfig, log_dir: Path):
        self.config = config
        self.log_dir = log_dir
        self.process = None
        self.reader = None
        self.pending = {}
        self.next_id = 0
        self.events = asyncio.Queue()
        self.on_notification: Callable | None = None
        self.on_request: Callable | None = None
        self.stderr_file = None
        self.write_lock = asyncio.Lock()

    async def start(self):
        self.events = asyncio.Queue()
        version = await command(self.config.executable, "--version")
        if version != f"codex-cli {self.config.expected_version}":
            raise RpcError(f"adapter requires Codex {self.config.expected_version}; found {version}")
        self.log_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.stderr_file = (self.log_dir / "app-server.stderr.log").open("ab")
        (self.log_dir / "app-server.stderr.log").chmod(0o600)
        self.process = await asyncio.create_subprocess_exec(
            self.config.executable,
            "app-server",
            "--listen",
            "stdio://",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=self.stderr_file,
            limit=16 * 1024 * 1024,
        )
        self.reader = asyncio.create_task(self._read())
        await self.rpc(
            "initialize",
            {"clientInfo": {"name": "experimenttracker", "title": "ExperimentTracker", "version": "0.1.0"}},
        )
        await self.send({"method": "initialized"})

    async def send(self, message):
        if not self.process or self.process.returncode is not None:
            raise TransportError("App Server is not running")
        async with self.write_lock:
            try:
                self.process.stdin.write((json.dumps(message, allow_nan=False) + "\n").encode())
                await self.process.stdin.drain()
            except (BrokenPipeError, ConnectionResetError) as exc:
                raise TransportError("App Server input connection closed") from exc

    async def rpc(self, method, params, *, timeout=None):
        self.next_id += 1
        request_id = self.next_id
        future = asyncio.get_running_loop().create_future()
        self.pending[request_id] = future
        try:
            await self.send({"id": request_id, "method": method, "params": params})
            return await asyncio.wait_for(
                future,
                timeout
                or (
                    self.config.session_timeout_seconds
                    if method in ("thread/start", "thread/resume", "turn/start")
                    else self.config.rpc_timeout_seconds
                ),
            )
        except TimeoutError as exc:
            detail = (
                "; the turn may have started; reconcile its saved identity before retry"
                if method == "turn/start"
                else ""
            )
            raise TimeoutError(f"{method} response timed out{detail}") from exc
        finally:
            self.pending.pop(request_id, None)

    async def _read(self):
        error = TransportError("App Server exited")
        try:
            while line := await self.process.stdout.readline():
                message = json.loads(line)
                if not isinstance(message, dict):
                    raise RpcError("invalid protocol payload")
                if "method" in message:
                    if "id" in message:
                        if self.on_request:
                            await self.on_request(message)
                        else:
                            await self.send(
                                {
                                    "id": message["id"],
                                    "error": {"code": -32601, "message": "interactive request unavailable"},
                                }
                            )
                    else:
                        if self.on_notification:
                            await self.on_notification(message)
                        # Streaming deltas and quota notifications are not turn results. Keeping
                        # every notification would grow memory indefinitely between actions.
                        if message["method"] == "turn/completed" or (
                            message["method"] == "item/completed"
                            and message.get("params", {}).get("item", {}).get("type") == "agentMessage"
                        ):
                            await self.events.put(message)
                elif "id" in message:
                    future = self.pending.get(message["id"])
                    if future and not future.done():
                        if "error" in message:
                            future.set_exception(RpcRejected(str(message["error"])))
                        elif "result" in message:
                            future.set_result(message["result"])
                        else:
                            future.set_exception(RpcError("response has neither result nor error"))
        except Exception as exc:
            error = TransportError(f"App Server transport failed: {exc}")
        finally:
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(error)
            await self.events.put({"method": "transport/closed", "params": {"error": str(error)}})

    async def close(self):
        if self.process and self.process.returncode is None:
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), 5)
            except TimeoutError:
                self.process.kill()
                await self.process.wait()
        if self.reader:
            self.reader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.reader
        if self.stderr_file:
            self.stderr_file.close()

    @property
    def available(self):
        return bool(
            self.process and self.process.returncode is None and self.reader and not self.reader.done()
        )


class CodexAgent:
    is_demo = False

    def __init__(self, config, state_dir):
        self.config = config
        self.server = AppServer(config, state_dir / "logs")
        self.active = None
        self.request_handler = None
        self.quota_handler = None
        self.activity_handler = None
        self.last_progress = time.monotonic()
        self.notification_tasks = set()
        self.models = []
        self.models_observed_at = None

        self.resolved_model = config.model

    async def start(self):
        await self.server.start()
        account = await self.server.rpc("account/read", {"refreshToken": False})
        if (account.get("account") or {}).get("type") != "chatgpt":
            raise RpcError("a managed ChatGPT login is required; API billing is not supported")
        import time

        catalog = {"data": []}
        cursor = None
        for _ in range(10):
            page = await self.server.rpc(
                "model/list", {"includeHidden": False, **({"cursor": cursor} if cursor else {})}
            )
            catalog["data"].extend(page.get("data", []))
            cursor = page.get("nextCursor")
            if not cursor:
                break
        self.models = [
            {
                "id": model["model"],
                "name": model["displayName"],
                "reasoning_efforts": [
                    option["reasoningEffort"] for option in model["supportedReasoningEfforts"]
                ],
                "default_effort": model["defaultReasoningEffort"],
            }
            for model in catalog["data"]
            if not model.get("hidden")
        ]
        self.models_observed_at = time.time()
        if not self.resolved_model:
            defaults = [m["model"] for m in catalog.get("data", []) if m.get("isDefault")]
            if len(defaults) != 1:
                raise RpcError("model catalog has no unambiguous default; configure codex.model")
            self.resolved_model = defaults[0]
        self.server.on_notification = self._notification
        self.server.on_request = self._request

    @property
    def available(self):
        return self.server.available

    async def _notification(self, message):
        params = message.get("params", {})
        if (
            self.active
            and params.get("threadId") == self.active[0]
            and params.get("turnId", self.active[1]) == self.active[1]
        ):
            if message["method"].startswith(("item/", "turn/")):
                self.last_progress = time.monotonic()
        if (
            message["method"]
            in (
                "item/started",
                "item/completed",
                "thread/tokenUsage/updated",
                "item/reasoning/summaryTextDelta",
            )
            and self.activity_handler
        ):
            await self.activity_handler(message)
        if message["method"] == "account/rateLimits/updated" and self.quota_handler:
            # Never wait for an interrupt RPC from the protocol reader itself.
            task = asyncio.create_task(self.quota_handler(message["params"]))
            self.notification_tasks.add(task)
            task.add_done_callback(self.notification_tasks.discard)

    async def _request(self, message):
        supported = {
            "item/commandExecution/requestApproval",
            "item/fileChange/requestApproval",
            "item/tool/requestUserInput",
        }
        if self.active and message["method"] in supported and self.request_handler:
            await self.request_handler(message)
        else:
            await self.server.send(
                {
                    "id": message["id"],
                    "error": {"code": -32601, "message": "unsupported request; no automatic approval"},
                }
            )

    async def quota(self):
        return await self.server.rpc("account/rateLimits/read", {})

    async def session(
        self,
        thread_id,
        workspace,
        *,
        writable=False,
        model=None,
        runtime_config=None,
        approval_policy="on-request",
    ):
        params = {
            "cwd": str(workspace),
            "sandbox": "workspace-write" if writable else "read-only",
            "approvalPolicy": approval_policy,
        }
        if runtime_config:
            params["config"] = runtime_config
        if model or self.resolved_model:
            params["model"] = model or self.resolved_model
        if thread_id:
            try:
                result = await self.server.rpc("thread/resume", {"threadId": thread_id, **params})
            except RpcRejected as exc:
                raise SessionUnavailable(f"session resume rejected: {exc}") from exc
        else:
            result = await self.server.rpc("thread/start", params)
        return result["thread"]["id"], result.get("model")

    async def begin(self, thread_id, prompt, schema, *, workspace=None, effort="low", project_workspace=None):
        self.active = (thread_id, None)
        result = await self.server.rpc(
            "turn/start",
            {
                "threadId": thread_id,
                "input": [{"type": "text", "text": prompt}],
                "outputSchema": strict_output_schema(schema),
                "effort": effort,
                **({"cwd": str(project_workspace)} if project_workspace else {}),
                **(
                    {
                        "sandboxPolicy": {
                            "type": "workspaceWrite",
                            "writableRoots": [str(workspace)]
                            + ([str(project_workspace)] if project_workspace else []),
                            "networkAccess": True,
                            "excludeSlashTmp": True,
                            "excludeTmpdirEnvVar": True,
                        }
                    }
                    if workspace is not None
                    else {"sandboxPolicy": {"type": "readOnly"}}
                ),
            },
        )
        turn = result["turn"]
        if not isinstance(turn.get("id"), str):
            raise RpcError("invalid turn identity")
        self.active = (thread_id, turn["id"])
        return turn["id"]

    async def finish(self, thread_id, turn_id):
        last_message = None
        started = self.last_progress = time.monotonic()
        try:
            while True:
                idle = self.config.turn_timeout_seconds - (time.monotonic() - self.last_progress)
                ceiling = self.config.turn_max_seconds
                remaining = min(idle, ceiling - (time.monotonic() - started)) if ceiling else idle
                try:
                    message = await asyncio.wait_for(self.server.events.get(), max(0, remaining))
                except TimeoutError:
                    if time.monotonic() - self.last_progress < self.config.turn_timeout_seconds and (
                        ceiling is None or time.monotonic() - started < ceiling
                    ):
                        continue
                    raise
                method = message["method"]
                params = message.get("params", {})
                if method == "transport/closed":
                    raise TransportError(params["error"])
                if params.get("threadId") != thread_id:
                    continue
                if method == "item/completed" and params.get("turnId") == turn_id:
                    item = params.get("item", {})
                    if item.get("type") == "agentMessage":
                        last_message = item.get("text", "")
                if method == "turn/completed" and params.get("turn", {}).get("id") == turn_id:
                    turn = params["turn"]
                    if turn.get("status") != "completed":
                        raise TerminalTurnError(f"turn {turn.get('status')}: {turn.get('error')}")
                    if last_message is None:
                        raise TerminalTurnError(
                            "agent output validation: completed turn has no structured result"
                        )
                    try:
                        return json.loads(last_message)
                    except ValueError as exc:
                        raise TerminalTurnError(f"agent output validation: invalid JSON: {exc}") from exc
        except TimeoutError:
            await self.interrupt(thread_id, turn_id)
            try:
                async with asyncio.timeout(self.config.interrupt_grace_seconds):
                    while True:
                        notification = await self.server.events.get()
                        params = notification.get("params", {})
                        if notification["method"] == "transport/closed":
                            raise NeedsAttention("transport closed while confirming interruption")
                        if (
                            notification["method"] == "turn/completed"
                            and params.get("threadId") == thread_id
                            and params.get("turn", {}).get("id") == turn_id
                        ):
                            status = params["turn"].get("status")
                            if status == "completed":
                                # Completion can race the timeout/interrupt boundary. Reuse
                                # the exact saved result rather than spending a repair turn.
                                return await self.read_turn(thread_id, turn_id)
                            if status not in ("interrupted", "failed"):
                                raise NeedsAttention("interruption has no terminal saved outcome")
                            break
            except TimeoutError:
                raise NeedsAttention("interruption unconfirmed; reconcile saved turn before retry") from None
            raise TerminalTurnError(
                "turn timeout; interrupted",
                reason="owner_ceiling"
                if self.config.turn_max_seconds and time.monotonic() - started >= self.config.turn_max_seconds
                else "inactivity",
            ) from None
        finally:
            self.active = None

    async def read_turn(self, thread_id, turn_id, *, expected_prompt_hash=None):
        result = await self.server.rpc("thread/read", {"threadId": thread_id, "includeTurns": True})
        if expected_prompt_hash is not None and result["thread"].get("id") != thread_id:
            raise NeedsAttention("saved thread identity does not match")
        for turn in result["thread"].get("turns", []):
            if turn["id"] == turn_id:
                if expected_prompt_hash is not None:
                    messages = [item for item in turn.get("items", []) if item.get("type") == "userMessage"]
                    content = messages[0].get("content", []) if len(messages) == 1 else []
                    if (
                        len(content) != 1
                        or content[0].get("type") != "text"
                        or hashlib.sha256(content[0].get("text", "").encode()).hexdigest()
                        != expected_prompt_hash
                    ):
                        raise NeedsAttention("saved turn input does not match the exact dispatched prompt")
                if turn["status"] == "completed":
                    items = [item for item in turn.get("items", []) if item.get("type") == "agentMessage"]
                    if items:
                        try:
                            return json.loads(items[-1]["text"])
                        except ValueError as exc:
                            raise TerminalTurnError(f"agent output validation: invalid JSON: {exc}") from exc
                if turn["status"] in ("interrupted", "failed"):
                    raise TerminalTurnError(f"turn {turn['status']}: {turn.get('error')}")
                raise NeedsAttention(f"saved turn is {turn['status']}; operator reconciliation required")
        raise NeedsAttention("saved turn not found; operator reconciliation required")

    async def interrupt(self, thread_id, turn_id):
        with contextlib.suppress(RpcError, TimeoutError):
            await self.server.rpc("turn/interrupt", {"threadId": thread_id, "turnId": turn_id})

    async def reply(self, pending):
        result = request_answer(pending, pending["answer"])
        await self.server.send({"id": pending["rpc_id"], "result": result})

    async def close(self):
        if self.active:
            await self.interrupt(*self.active)
        for task in self.notification_tasks:
            task.cancel()
        if self.notification_tasks:
            await asyncio.gather(*self.notification_tasks, return_exceptions=True)
        self.notification_tasks.clear()
        await self.server.close()


class DemoAgent:
    is_demo = True
    available = True
    resolved_model = "deterministic-demo"
    models = [
        {
            "id": "deterministic-demo",
            "name": "Deterministic demo (no model usage)",
            "reasoning_efforts": ["low", "medium"],
            "default_effort": "medium",
        }
    ]
    models_observed_at = None

    def __init__(self):
        self.calls = 0
        self.request_handler = None
        self.quota_handler = None
        self.results = {}

    async def start(self):
        pass

    async def quota(self):
        return {
            "rateLimitsByLimitId": {
                "codex": {
                    "primary": {"usedPercent": 0, "windowDurationMins": 300},
                    "secondary": {"usedPercent": 0, "windowDurationMins": 10080},
                }
            },
            "ordinaryUsageAllowed": True,
        }

    async def session(
        self, thread_id, workspace, *, writable=False, model=None, approval_policy="on-request"
    ):
        return thread_id or f"demo-thread-{workspace.name}", "deterministic-demo"

    async def begin(self, thread_id, prompt, schema, *, workspace=None, effort="low", project_workspace=None):
        self.calls += 1
        turn_id = f"demo-turn-{self.calls}"
        payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
        if payload["phase"] in ("project_synthesize", "project_chat"):
            ids = list(payload["sources"])
            blocks = []
            for source in payload["experiments"]:
                metrics = source.get("metric_catalog", [])
                if metrics:
                    selected = metrics[: min(4, len(metrics))]
                    blocks.append(
                        {
                            "kind": "table",
                            "caption": "Registered measurements from this synthetic demonstration; protocols are not compared across experiments.",
                            "columns": ["Variant", "Metric", "Value"],
                            "rows": [
                                {
                                    "cells": [
                                        {"text": metric["variant"]},
                                        {"text": metric["name"]},
                                        {
                                            "metric": {
                                                "experiment_id": source["id"],
                                                "name": metric["name"],
                                                "variant": metric["variant"],
                                            }
                                        },
                                    ],
                                    "experiment_ids": [source["id"]],
                                }
                                for metric in selected
                            ],
                        }
                    )
                    break
            figure = next((f for f in payload["figures"] if f["name"] == "predictions.svg"), None)
            if figure:
                blocks.append(
                    {
                        "kind": "figure",
                        "caption": "Saved prediction figure for the synthetic demonstration.",
                        "alt": "Saved synthetic model predictions.",
                        "experiment_id": figure["experiment_id"],
                        "artifact_id": figure["id"],
                    }
                )
            result = {
                "reply": "Deterministic synthetic project memory demonstration; no research conclusions or model inference.",
                "experiment_ids": ids,
                "report": {
                    "title": "Synthetic project working report",
                    "overview": "This report aggregates synthetic demonstrations only.",
                    "sections": [
                        {
                            "title": "Synthetic comparisons",
                            "body": "These completed fixtures demonstrate the workflow. They do not establish scientific generalization.",
                            "experiment_ids": ids,
                            "blocks": blocks,
                        }
                    ],
                    "open_questions": ["Which real research question should be tested next?"],
                }
                if payload["phase"] == "project_synthesize"
                else None,
            }
        elif payload["phase"] == "prepare":
            brief = payload["brief"].lower()
            supported = (
                ("linear" in brief and "quadratic" in brief)
                or ("fixed" in brief and any(word in brief for word in ("models", "compare", "protocol")))
                or ("repeat" in brief and "protocol" in brief)
            ) and not any(word in brief for word in ("tree", "frontier", "forest", "neural", "classifier"))
            result = {
                "schema_version": 1,
                "recipe_id": "toy_compare_v1",
                "snapshot_hash": payload["manifest"]["snapshot_hash"],
                "parameters_hash": payload["manifest"]["parameters_hash"],
                "summary": "Deterministic demo preparation of fixed shared synthetic data and three CPU stages.",
                "scope_status": "supported" if supported else "unsupported",
                "scope_reason": (
                    "The deterministic demo recognizes the explicitly requested fixed linear/quadratic comparison."
                    if supported
                    else "The deterministic demo only supports an explicitly requested fixed linear/quadratic comparison; this brief was not recognized as that recipe."
                ),
            }
        elif payload["phase"] == "discuss":
            if "context_mode" in payload:
                contexts = getattr(self, "discussion_contexts", {})
                updates = payload["context"]
                if thread_id not in contexts:
                    updates = json.loads(
                        await asyncio.to_thread(Path(payload["context_index"]["path"]).read_text)
                    )
                context = contexts.setdefault(thread_id, {"history": []})
                for key, value in updates.items():
                    if key.endswith("_detail"):
                        context[key.removesuffix("_detail")] = json.loads(
                            await asyncio.to_thread(Path(value["path"]).read_text)
                        )
                    else:
                        context[key] = value
                if "history_detail" in payload:
                    context["history"] = json.loads(
                        await asyncio.to_thread(Path(payload["history_detail"]["path"]).read_text)
                    )
                self.discussion_contexts = contexts
                payload = {**context, **payload}
            ids = [a["id"] for a in payload["artifacts"] if a["name"] == "results.json"]
            result = {
                "schema_version": 1,
                "reply": "This is a deterministic demo reply. The saved held-out errors support the quadratic fit on this synthetic dataset; one seed cannot establish generalization. "
                + (
                    "Your earlier messages remain in this experiment's conversation. "
                    if payload["history"]
                    else ""
                )
                + "A useful next experiment would vary the noise and seeds while keeping the split comparable.",
                "evidence_ids": ids,
                "presentation": None,
            }
            if any(word in payload["message"].lower() for word in ("plot", "visualize", "report")):
                document = payload["current_report_document"] or {
                    "title": payload["analysis"]["title"],
                    "standfirst": payload["analysis"]["summary"],
                    "blocks": [
                        {
                            "kind": "paragraph",
                            "text": payload["analysis"]["observations"][0]["text"],
                            "evidence_ids": ids,
                        }
                    ],
                }
                document["blocks"].append(
                    {
                        "kind": "metric_plot",
                        "x_label": "Model index (1 linear, 2 quadratic)",
                        "y_label": "Held-out MSE (lower is better)",
                        "caption": "Saved synthetic held-out error; one fixed seed, no uncertainty estimate.",
                        "alt": "Linear and quadratic held-out errors from the saved demo.",
                        "evidence_ids": ids,
                        "series": [
                            {
                                "label": variant,
                                "points": [{"x": i, "metric": {"name": "mse", "variant": variant}}],
                            }
                            for i, variant in enumerate(("linear", "quadratic"), 1)
                        ],
                    }
                )
                result["presentation"] = document
                result["reply"] = (
                    "I prepared a report revision with a plot of the saved held-out MSE. This deterministic demo uses the original measurements and adds no training run."
                )
        else:
            ids = [
                a["id"]
                for a in payload["artifacts"]
                if a["name"] in ("results.json", "predictions.json", "predictions.svg")
            ]
            if not ids:
                ids = payload["evidence_ids"][:10]
            metrics = payload["results"]["metrics"]
            result = {
                "schema_version": 1,
                "title": "Quadratic regression · synthetic infrastructure demo",
                "summary": "The quadratic model fits this synthetic quadratic data better. Demo interpretation is deterministic, not a Codex turn.",
                "observations": [
                    {
                        "text": f"Held-out MSE: linear {metrics['linear']['mse']:.6g}; quadratic {metrics['quadratic']['mse']:.6g}.",
                        "evidence_ids": ids,
                    }
                ],
                "limitations": [
                    "One synthetic seed; this does not establish performance on real scientific data.",
                    "The target was generated from a quadratic function, favoring that model.",
                ],
                "next_ideas": [
                    "Repeat across seeds and noise levels after a separate researcher instruction."
                ],
                "scientific_outcome": "supported",
            }
        self.results[(thread_id, turn_id)] = result
        return turn_id

    async def finish(self, thread_id, turn_id):
        return self.results[(thread_id, turn_id)]

    async def read_turn(self, thread_id, turn_id):
        if (thread_id, turn_id) not in self.results:
            raise NeedsAttention("demo turn not retained; uncertain turn is held")
        return self.results[(thread_id, turn_id)]

    async def interrupt(self, *args):
        pass

    async def close(self):
        pass
