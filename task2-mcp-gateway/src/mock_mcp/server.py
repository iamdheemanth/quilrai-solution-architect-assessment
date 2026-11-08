"""Mock downstream MCP server with per-instance call counters.

DNS-rebinding protection is left at the SDK default (on), so only Host headers of
127.0.0.1:*, localhost:* and [::1]:* are accepted; anything else gets HTTP 421.
"""

import os
import sys
from collections import Counter
from dataclasses import dataclass

import uvicorn
from mcp.server.mcpserver import MCPServer
from starlette.applications import Starlette

_TRUE = {"true", "1", "yes"}
_FALSE = {"false", "0", "no"}


@dataclass
class MockDownstream:
    server: MCPServer
    app: Starlette
    calls: Counter[str]


def build_mock(json_response: bool = False, stateless: bool = True) -> MockDownstream:
    server = MCPServer("mock-downstream")
    calls: Counter[str] = Counter()

    @server.tool()
    def get_status() -> dict:
        """Return the mock server status."""
        calls["get_status"] += 1
        return {"status": "ok"}

    @server.tool()
    def list_users() -> list[str]:
        """List the known users."""
        calls["list_users"] += 1
        return ["alice", "bob"]

    @server.tool()
    def admin_reset_key(user: str) -> dict:
        """Reset the API key of a user (admin only)."""
        calls["admin_reset_key"] += 1
        return {"user": user, "key_reset": True}

    app = server.streamable_http_app(json_response=json_response, stateless_http=stateless)
    return MockDownstream(server=server, app=app, calls=calls)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in _TRUE:
        return True
    if value in _FALSE:
        return False
    sys.stderr.write(f"{name} must be one of true/false/1/0/yes/no, got {raw!r}\n")
    raise SystemExit(2)


def main() -> None:
    host = os.environ.get("MOCK_HOST", "127.0.0.1")
    raw_port = os.environ.get("MOCK_PORT", "8001")
    try:
        port = int(raw_port)
        if not 0 < port < 65536:
            raise ValueError
    except ValueError:
        sys.stderr.write(f"MOCK_PORT must be an integer in 1..65535, got {raw_port!r}\n")
        raise SystemExit(2) from None
    json_response = _env_bool("MOCK_JSON_RESPONSE", False)
    stateless = _env_bool("MOCK_STATELESS", True)
    mock = build_mock(json_response=json_response, stateless=stateless)
    uvicorn.run(mock.app, host=host, port=port, log_level="info", server_header=False)
