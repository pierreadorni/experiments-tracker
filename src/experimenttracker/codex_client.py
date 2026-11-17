import asyncio
import contextlib
import hashlib
import json
import sys
from collections.abc import Callable
from pathlib import Path

from . import clock
from .config import CodexConfig, JobResources
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


def turn_prompt_sha256(turn):
    """Exact hash of one saved turn's single text input, or None when it is not provable."""
    messages = [item for item in turn.get("items", []) if item.get("type") == "userMessage"]
    content = messages[0].get("content", []) if len(messages) == 1 else []
    if len(content) != 1 or content[0].get("type") != "text":
        return None
    return hashlib.sha256(content[0].get("text", "").encode()).hexdigest()


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
        self.last_progress = clock.monotonic()
        self.notification_tasks = set()
        self.models = []
        self.models_observed_at = None
        # Callbacks keep the count they arrived with, so a restart cannot credit them
        # to the process that replaced the one that asked.
        self.start_count = 0

        self.resolved_model = config.model

    async def start(self):
        self.start_count += 1
        await self.server.start()
        account = await self.server.rpc("account/read", {"refreshToken": False})
        if (account.get("account") or {}).get("type") != "chatgpt":
            raise RpcError("a managed ChatGPT login is required; API billing is not supported")
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
        self.models_observed_at = clock.now()
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
                self.last_progress = clock.monotonic()
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
            await self.activity_handler(message, self.start_count)
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
            await self.request_handler(message, self.start_count)
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

    async def begin(
        self,
        thread_id,
        prompt,
        schema,
        *,
        workspace=None,
        effort="low",
        project_workspace=None,
        additional_writable_roots=(),
    ):
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
                            + ([str(project_workspace)] if project_workspace else [])
                            + [str(root) for root in additional_writable_roots],
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
        started = self.last_progress = clock.monotonic()
        try:
            while True:
                idle = self.config.turn_timeout_seconds - (clock.monotonic() - self.last_progress)
                ceiling = self.config.turn_max_seconds
                remaining = min(idle, ceiling - (clock.monotonic() - started)) if ceiling else idle
                try:
                    message = await asyncio.wait_for(self.server.events.get(), max(0, remaining))
                except TimeoutError:
                    if clock.monotonic() - self.last_progress < self.config.turn_timeout_seconds and (
                        ceiling is None or clock.monotonic() - started < ceiling
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
                if self.config.turn_max_seconds
                and clock.monotonic() - started >= self.config.turn_max_seconds
                else "inactivity",
            ) from None
        finally:
            self.active = None

    async def read_thread(self, thread_id):
        """Saved turns of one thread; an unreadable thread raises instead of proving absence."""
        return [
            {"id": turn["id"], "status": turn["status"], "prompt_sha256": turn_prompt_sha256(turn)}
            for turn in await self._read_thread(thread_id)
        ]

    async def _read_thread(self, thread_id):
        result = await self.server.rpc("thread/read", {"threadId": thread_id, "includeTurns": True})
        if result["thread"].get("id") != thread_id:
            raise NeedsAttention("saved thread identity does not match")
        return result["thread"].get("turns", [])

    async def read_turn(self, thread_id, turn_id, *, expected_prompt_hash=None):
        for turn in await self._read_thread(thread_id):
            if turn["id"] == turn_id:
                if expected_prompt_hash is not None and turn_prompt_sha256(turn) != expected_prompt_hash:
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


DEMO_PROTOCOL = (
    "Synthetic demonstration only: fit an intercept+slope model and a quadratic least-squares model "
    "to a fixed 24-point generated grid, then report held-out mean squared error on 8 fixed points. "
    "Standard-library code on generated data; results say nothing about real measurements."
)

DEMO_AGENT_SOURCE = '''\
"""Standard-library synthetic demo: fixed linear and quadratic least-squares comparison."""

import argparse
import json
import math
import os
from pathlib import Path

PROTOCOL = __PROTOCOL__
TRAIN_X = [round(-1.5 + 0.1 * step, 4) for step in range(24)]
TEST_X = [round(-1.45 + 0.3 * step, 4) for step in range(8)]
GRID_X = [round(-1.6 + 0.1 * step, 4) for step in range(33)]


def target(x):
    return 2.0 * x * x + 0.5 + 0.1 * math.sin(4 * x)


def fit(features, targets):
    size = len(features[0])
    rows = [
        [sum(f[i] * f[j] for f in features) for j in range(size)]
        + [sum(f[i] * y for f, y in zip(features, targets))]
        for i in range(size)
    ]
    for column in range(size):
        pivot = max(range(column, size), key=lambda row: abs(rows[row][column]))
        rows[column], rows[pivot] = rows[pivot], rows[column]
        for row in range(size):
            if row != column:
                factor = rows[row][column] / rows[column][column]
                for index in range(column, size + 1):
                    rows[row][index] -= factor * rows[column][index]
    return [rows[i][size] / rows[i][i] for i in range(size)]


def predict(coefficients, features):
    return sum(c * f for c, f in zip(coefficients, features))


def measure(output):
    targets = [target(x) for x in TRAIN_X]
    linear = fit([(1.0, x) for x in TRAIN_X], targets)
    quadratic = fit([(1.0, x, x * x) for x in TRAIN_X], targets)
    cases = [
        (x, target(x), predict(linear, (1.0, x)), predict(quadratic, (1.0, x, x * x)))
        for x in TEST_X
    ]
    metrics = [
        {
            "name": "mse",
            "variant": variant,
            "value": math.fsum((case[index] - case[1]) ** 2 for case in cases) / len(cases),
            "unit": "target units squared",
        }
        for index, variant in ((2, "linear"), (3, "quadratic"))
    ]
    (output / "metrics.json").write_text(
        json.dumps(
            {"schema_version": 1, "protocol": PROTOCOL, "sample_count": len(cases), "metrics": metrics},
            indent=2,
        )
    )
    (output / "predictions.json").write_text(
        json.dumps(
            {
                "grid": [
                    [x, predict(linear, (1.0, x)), predict(quadratic, (1.0, x, x * x))] for x in GRID_X
                ],
                "observed": [[case[0], case[1]] for case in cases],
            },
            indent=2,
        )
    )


def figure(output):
    inputs = json.loads(Path(os.environ["EXPERIMENT_INPUTS"]).read_text())
    source = next(item["path"] for item in inputs if item["name"] == "predictions.json")
    data = json.loads(Path(source).read_text())
    width, height, pad = 360, 240, 24
    values = [value for row in data["grid"] for value in row[1:]] + [row[1] for row in data["observed"]]
    x_range = max(row[0] for row in data["grid"]) - min(row[0] for row in data["grid"])
    y_range = max(values) - min(values)

    def place(x, y):
        left = pad + (x - min(row[0] for row in data["grid"])) / x_range * (width - 2 * pad)
        top = height - pad - (y - min(values)) / y_range * (height - 2 * pad)
        return round(left, 2), round(top, 2)

    curves = "".join(
        '<polyline fill="none" stroke="{}" stroke-width="2" points="{}"/>'.format(
            colour,
            " ".join(f"{x},{y}" for x, y in (place(row[0], row[index]) for row in data["grid"])),
        )
        for index, colour in ((1, "#1f77b4"), (2, "#d62728"))
    )
    points = "".join(
        f'<circle cx="{x}" cy="{y}" r="3" fill="#333333"/>'
        for x, y in (place(*row) for row in data["observed"])
    )
    (output / "predictions.svg").write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}"><rect width="{width}" height="{height}" fill="white"/>'
        f'<text x="{pad}" y="16" font-family="sans-serif" font-size="11">'
        "Synthetic fits: blue linear, red quadratic; dots are held-out points</text>"
        f'<line x1="{pad}" y1="{height - pad}" x2="{width - pad}" y2="{height - pad}" '
        'stroke="#888888"/>'
        f"{curves}{points}</svg>"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("stage")
    parser.add_argument("--output", required=True)
    arguments = parser.parse_args()
    output = Path(arguments.output)
    if arguments.stage == "measure":
        measure(output)
    elif arguments.stage == "figure":
        figure(output)
    else:
        raise SystemExit(f"unknown stage {arguments.stage}")


if __name__ == "__main__":
    main()
'''.replace("__PROTOCOL__", repr(DEMO_PROTOCOL))

DEMO_RESOURCES = JobResources()


class DemoAgent:
    is_demo = True
    available = True
    resolved_model = "deterministic-demo"
    start_count = 0
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
        self.start_count += 1

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
        self,
        thread_id,
        workspace,
        *,
        writable=False,
        model=None,
        approval_policy="on-request",
        runtime_config=None,
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
        elif payload["phase"] == "plan":
            result = self.demo_plan(payload)
        elif payload["phase"] == "prepare" and "output_schema" in payload:
            result = self.demo_implementation(payload)
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
        elif isinstance(payload["results"]["metrics"], list):
            result = self.demo_analysis(payload)
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

    @staticmethod
    def demo_supported(brief):
        text = brief.lower()
        return ("linear" in text and "quadratic" in text) and not any(
            word in text for word in ("tree", "frontier", "forest", "neural", "classifier")
        )

    def demo_plan(self, payload):
        """Schema 3 plan for the current contract; questions replace work the demo cannot do."""
        if not self.demo_supported(payload["brief"]):
            return {
                "schema_version": 3,
                "title": "Synthetic regression demo: clarification needed",
                "summary": "The deterministic demo only runs its fixed synthetic comparison.",
                "protocol": DEMO_PROTOCOL,
                "questions": [
                    "The deterministic demo can only compare linear and quadratic least-squares fits on "
                    "its generated grid. Should this brief be narrowed to that comparison?"
                ],
                "stages": [],
            }
        return {
            "schema_version": 3,
            "title": "Synthetic linear and quadratic regression demo",
            "summary": "Two small CPU tasks on generated data: measure held-out errors, then plot the saved predictions.",
            "protocol": DEMO_PROTOCOL,
            "questions": [],
            "stages": [
                {
                    "id": "measure",
                    "title": "Measure the synthetic fits",
                    "summary": "Fit both models to the fixed generated grid and write bounded evaluation evidence.",
                    "executor": "slurm",
                    "resources": DEMO_RESOURCES.model_dump(),
                    "needs": [],
                },
                {
                    "id": "figure",
                    "title": "Plot the saved predictions",
                    "summary": "Draw the produced prediction data; no retraining and no new measurements.",
                    "executor": "slurm",
                    "resources": DEMO_RESOURCES.model_dump(),
                    "needs": ["measure"],
                },
            ],
        }

    def demo_implementation(self, payload):
        """Write the declared standard-library source and a work-confined interpreter symlink."""
        workspace = Path(payload["workspace"])
        workspace.mkdir(parents=True, exist_ok=True)
        python = workspace / "bin/python"
        python.parent.mkdir(parents=True, exist_ok=True)
        if not python.is_symlink():
            python.symlink_to(sys.executable)
        (workspace / "recipe.py").write_text(DEMO_AGENT_SOURCE)
        return {
            "schema_version": 3,
            "title": "Synthetic linear and quadratic regression demo",
            "summary": "A fixed generated comparison in one source file and two CPU stages.",
            "protocol": DEMO_PROTOCOL,
            "synthetic_demo": True,
            "parameters_json": json.dumps({"train_points": 24, "test_points": 8, "grid_points": 33}),
            "files": ["recipe.py"],
            "figures": ["predictions.svg"],
            "python_executable": str(python),
            "stages": [
                {
                    "id": "measure",
                    "title": "Measure the synthetic fits",
                    "needs": [],
                    "outputs": ["metrics.json", "predictions.json"],
                    "resources": DEMO_RESOURCES.model_dump(),
                    "result_file": "metrics.json",
                    "expected_metrics": ["mse"],
                    "sample_count": 8,
                },
                {
                    "id": "figure",
                    "title": "Plot the saved predictions",
                    "needs": ["measure"],
                    "outputs": ["predictions.svg"],
                    "resources": DEMO_RESOURCES.model_dump(),
                },
            ],
        }

    def demo_analysis(self, payload):
        """Interpret the backend's combined results and cite the registered figure."""
        metrics = {(m["name"], m["variant"]): m["value"] for m in payload["results"]["metrics"]}
        measured = next(record["id"] for record in payload["artifacts"] if record["name"] == "metrics.json")
        figure = next(record["id"] for record in payload["artifacts"] if record["name"] == "predictions.svg")
        return {
            "schema_version": 1,
            "title": "Synthetic regression demo · working report",
            "summary": "Both models fit the generated grid; the quadratic fit is closer on the fixed held-out points.",
            "observations": [
                {
                    "text": "Held-out MSE: linear {:.6g}; quadratic {:.6g}.".format(
                        metrics[("mse", "linear")], metrics[("mse", "quadratic")]
                    ),
                    "evidence_ids": [measured, figure],
                }
            ],
            "limitations": [
                "One generated dataset with a deterministic perturbation; no sampling or uncertainty analysis.",
                "The target was constructed from a quadratic function, which favors the quadratic fit.",
            ],
            "next_ideas": [
                "Repeat the comparison over several generated datasets after a separate researcher instruction."
            ],
            "scientific_outcome": "supported",
            "presentation": {
                "title": "Synthetic regression demo report",
                "standfirst": "A deterministic workflow demonstration on generated data, not scientific evidence.",
                "blocks": [
                    {
                        "kind": "table",
                        "caption": "Held-out MSE on the fixed synthetic test points; one dataset, no uncertainty estimate.",
                        "columns": ["Fit", "MSE"],
                        "rows": [
                            [{"text": variant}, {"metric": {"name": "mse", "variant": variant}}]
                            for variant in ("linear", "quadratic")
                        ],
                    },
                    {
                        "kind": "figure",
                        "caption": "Saved prediction curves over the generated grid.",
                        "alt": "Linear and quadratic fitted curves over the synthetic held-out points.",
                        "artifact_id": figure,
                    },
                    {
                        "kind": "paragraph",
                        "text": "Both fits are measured on the same generated points. The quadratic model was chosen "
                        "because its family contains the generating function.",
                    },
                ],
            },
        }

    async def finish(self, thread_id, turn_id):
        return self.results[(thread_id, turn_id)]

    async def read_thread(self, thread_id):
        """The deterministic demo keeps no turn inputs, so it can never prove absence."""
        raise NeedsAttention("demo threads retain no turn inputs")

    async def read_turn(self, thread_id, turn_id):
        if (thread_id, turn_id) not in self.results:
            raise NeedsAttention("demo turn not retained; uncertain turn is held")
        return self.results[(thread_id, turn_id)]

    async def interrupt(self, *args):
        pass

    async def close(self):
        pass
