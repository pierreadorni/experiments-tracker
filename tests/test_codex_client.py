import asyncio
import json
from types import SimpleNamespace

import pytest

from experimenttracker.codex_client import AppServer, CodexAgent, NeedsAttention, TransportError
from experimenttracker.config import CodexConfig


@pytest.mark.parametrize("method", ["thread/start", "thread/resume", "turn/start"])
async def test_startup_rpc_uses_startup_window_and_names_timeout(tmp_path, method):
    config = CodexConfig().model_copy(update={"rpc_timeout_seconds": 0.001, "session_timeout_seconds": 0.05})
    server = AppServer(config, tmp_path)

    async def send(message):
        asyncio.get_running_loop().call_later(
            0.005, server.pending[message["id"]].set_result, {"saved": True}
        )

    server.send = send
    assert await server.rpc(method, {}) == {"saved": True}

    async def lose_response(message):
        pass

    server.send = lose_response
    with pytest.raises(TimeoutError, match=method + " response timed out"):
        await server.rpc(method, {}, timeout=0.001)
    assert not server.pending


async def test_reader_filters_streaming_noise_and_fails_pending_rpc_on_disconnect(tmp_path):
    server = AppServer(CodexConfig(), tmp_path)
    stream = asyncio.StreamReader()
    server.process = SimpleNamespace(stdout=stream)
    pending = asyncio.get_running_loop().create_future()
    server.pending[1] = pending
    for _ in range(1000):
        stream.feed_data(
            (json.dumps({"method": "item/agentMessage/delta", "params": {"delta": "x"}}) + "\n").encode()
        )
    stream.feed_data(b'{"method":"item/completed","params":{"item":{"type":"agentMessage","text":"{}"}}}\n')
    stream.feed_eof()
    await server._read()
    with pytest.raises(TransportError):
        await pending
    assert server.events.qsize() == 2  # final message and explicit connection-close evidence
    assert (await server.events.get())["method"] == "item/completed"
    assert (await server.events.get())["method"] == "transport/closed"


async def test_turn_connection_loss_requires_reconciliation(tmp_path):
    agent = CodexAgent(CodexConfig(), tmp_path)
    agent.active = ("thread", "turn")
    await agent.server.events.put({"method": "transport/closed", "params": {"error": "connection lost"}})
    with pytest.raises(NeedsAttention):
        await agent.finish("thread", "turn")
    assert agent.active is None


async def test_completed_turn_racing_timeout_is_reused(tmp_path):
    config = CodexConfig().model_copy(update={"turn_timeout_seconds": 0.001})
    agent = CodexAgent(config, tmp_path)
    reads = 0

    async def interrupt(thread, turn):
        await agent.server.events.put(
            {
                "method": "turn/completed",
                "params": {"threadId": thread, "turn": {"id": turn, "status": "completed"}},
            }
        )

    async def read(thread, turn):
        nonlocal reads
        reads += 1
        assert (thread, turn) == ("thread", "turn")
        return {"saved": "completed result"}

    agent.interrupt = interrupt
    agent.read_turn = read
    assert await agent.finish("thread", "turn") == {"saved": "completed result"}
    assert reads == 1


async def test_model_and_effort_are_sent_to_managed_rpc(tmp_path):
    agent = CodexAgent(CodexConfig(), tmp_path)
    calls = []

    async def rpc(method, params):
        calls.append((method, params))
        if method.startswith("thread/"):
            return {"thread": {"id": "thread"}, "model": params["model"]}
        return {"turn": {"id": "turn"}}

    agent.server.rpc = rpc
    assert await agent.session(None, tmp_path, writable=True, model="gpt-6.1-sol") == (
        "thread",
        "gpt-6.1-sol",
    )
    await agent.begin(
        "thread", "Implement brief", {"type": "object", "properties": {}}, workspace=tmp_path, effort="medium"
    )
    assert calls[0][1]["model"] == "gpt-6.1-sol"
    assert calls[0][1]["approvalPolicy"] == "on-request"
    assert calls[1][1]["effort"] == "medium"
    assert calls[1][1]["sandboxPolicy"]["writableRoots"] == [str(tmp_path)]
    await agent.session("thread", tmp_path, model="gpt-6.1-sol")
    assert calls[2][0] == "thread/resume" and calls[2][1]["model"] == "gpt-6.1-sol"


async def test_project_turn_has_actual_working_directory_and_write_grant(tmp_path):
    from experimenttracker.codex_client import CodexAgent

    calls = []

    class Server:
        async def rpc(self, method, params):
            calls.append((method, params))
            return {"turn": {"id": "saved-project-turn"}}

    agent = CodexAgent.__new__(CodexAgent)
    agent.server = Server()
    outputs, project, work = tmp_path / "outputs", tmp_path / "project", tmp_path / "work"
    schema = {"type": "object", "properties": {"ok": {"type": "boolean"}}, "required": ["ok"]}
    assert (
        await agent.begin(
            "thread",
            "Implement the brief",
            schema,
            workspace=outputs,
            project_workspace=project,
            additional_writable_roots=[work],
        )
        == "saved-project-turn"
    )
    method, params = calls[0]
    assert method == "turn/start"
    assert params["cwd"] == str(project)
    assert params["sandboxPolicy"]["writableRoots"] == [str(outputs), str(project), str(work)]


@pytest.mark.parametrize("error_type", ["rejected", "transport", "invalid"])
async def test_session_rejection_is_retryable_only_before_turn_dispatch(tmp_path, error_type):
    from experimenttracker.codex_client import RpcError, RpcRejected, SessionUnavailable

    agent = CodexAgent(CodexConfig(), tmp_path)
    errors = {"rejected": RpcRejected, "transport": TransportError, "invalid": RpcError}
    calls = []

    async def rpc(method, params):
        calls.append(method)
        raise errors[error_type]("session archived")

    agent.server.rpc = rpc
    expected = SessionUnavailable if error_type == "rejected" else errors[error_type]
    with pytest.raises(expected):
        await agent.session("saved-thread", tmp_path)
    assert calls == ["thread/resume"]
    assert agent.active is None
