import io
import json
import socket
from concurrent.futures import ThreadPoolExecutor

import pytest

from experimenttracker.autopilot_client import main


@pytest.mark.parametrize("version", [None, 1])
def test_mcp_document_name_reaches_owner(tmp_path, monkeypatch, capsys, version):
    socket_path = tmp_path / "owner.sock"
    arguments = {"name": "protocol"}
    if version is not None:
        arguments["version"] = version
    document = {"name": "protocol", "version": 1, "content": "Saved foundation document"}
    request = {
        "jsonrpc": "2.0",
        "id": 7,
        "method": "tools/call",
        "params": {"name": "get_document", "arguments": arguments},
    }
    monkeypatch.setattr("sys.argv", ["project-mcp", "--socket", str(socket_path), "--action", "action-1"])
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(request) + "\n"))

    with socket.socket(socket.AF_UNIX) as server:
        server.bind(str(socket_path))
        server.listen(1)
        server.settimeout(5)

        def owner():
            with server.accept()[0] as connection:
                connection.settimeout(5)
                with connection.makefile("rb") as stream:
                    received = json.loads(stream.readline())
                connection.sendall(json.dumps({"ok": True, "result": document}).encode() + b"\n")
                return received

        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(owner)
            main()
            received = pending.result(timeout=10)

    assert received == {"project_action_id": "action-1", "name": "get_document", "arguments": arguments}
    response = json.loads(capsys.readouterr().out)
    assert response["id"] == 7
    assert "isError" not in response["result"]
    assert json.loads(response["result"]["content"][0]["text"]) == document
