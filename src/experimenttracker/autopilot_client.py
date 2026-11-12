"""Thin local SDK and stdio MCP adapter; the owner remains the only writer."""

import argparse
import json
import socket
import sys
from pathlib import Path

from .autopilot_tools import TOOLS
from .store import encode


class AutopilotClient:
    def __init__(self, socket_path: Path, action_id: str):
        self.socket_path, self.action_id = socket_path, action_id

    def call(self, name: str, **arguments) -> dict:
        with socket.socket(socket.AF_UNIX) as connection:
            connection.settimeout(120)
            connection.connect(str(self.socket_path))
            connection.sendall(
                (
                    encode({"project_action_id": self.action_id, "name": name, "arguments": arguments}) + "\n"
                ).encode()
            )
            with connection.makefile("rb") as stream:
                response = json.loads(stream.readline(1024 * 1024))
        if not response["ok"]:
            raise ValueError(response["error"])
        return response["result"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", type=Path, required=True)
    parser.add_argument("--action", required=True)
    args = parser.parse_args()
    client = AutopilotClient(args.socket, args.action)
    for line in sys.stdin:
        request = {}
        try:
            request = json.loads(line)
            if "id" not in request:
                continue
            method = request["method"]
            if method == "initialize":
                result = {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "experimenttracker-project", "version": "1"},
                }
            elif method == "tools/list":
                result = {"tools": TOOLS}
            elif method == "tools/call":
                value = client.call(request["params"]["name"], **request["params"].get("arguments", {}))
                result = {"content": [{"type": "text", "text": encode(value)}]}
            elif method == "ping":
                result = {}
            else:
                raise ValueError("unsupported MCP method")
            response = {"jsonrpc": "2.0", "id": request["id"], "result": result}
        except Exception as exc:
            response = {
                "jsonrpc": "2.0",
                "id": request.get("id"),
                "result": {"content": [{"type": "text", "text": str(exc)}], "isError": True},
            }
        print(encode(response), flush=True)


if __name__ == "__main__":
    main()
