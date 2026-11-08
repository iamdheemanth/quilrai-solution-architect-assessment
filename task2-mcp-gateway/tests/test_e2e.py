"""Layer 4: real uvicorn servers (mock + gateway) driven by the official SDK client."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import threading
import time

import httpx2
import pytest
import uvicorn
from mcp import Client
from mcp.client.streamable_http import streamable_http_client
from mcp.shared.exceptions import MCPError

from mcp_gateway.app import create_app
from mcp_gateway.config import load_settings
from mock_mcp.server import build_mock
from tests.helpers import TEST_SECRET, token

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start(app, started: list) -> int:
    """Run ``app`` under uvicorn in a thread; register it in ``started`` before waiting."""
    port = free_port()
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", lifespan="on")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    started.append((server, thread))
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() > deadline or not thread.is_alive():
            raise RuntimeError("uvicorn server did not start")
        time.sleep(0.05)
    return port


@pytest.fixture(scope="module")
def stack():
    started: list[tuple[uvicorn.Server, threading.Thread]] = []
    try:
        mock = build_mock(json_response=False)
        mock_port = start(mock.app, started)
        settings = load_settings(
            {
                "GATEWAY_JWT_SECRET": TEST_SECRET,
                "GATEWAY_DOWNSTREAM_URL": f"http://127.0.0.1:{mock_port}/mcp",
            }
        )
        gw_port = start(create_app(settings), started)
        yield f"http://127.0.0.1:{gw_port}/mcp", mock
    finally:
        for server, _ in started:
            server.should_exit = True
        hung = []
        for _server, thread in reversed(started):
            thread.join(timeout=10)
            if thread.is_alive():
                hung.append(thread.name)
        assert not hung, f"uvicorn thread(s) still alive after shutdown: {hung}"


def sdk_client(url: str, role: str, mode: str):
    http = httpx2.AsyncClient(headers={"Authorization": f"Bearer {token(role)}"})
    return Client(streamable_http_client(url, http_client=http), mode=mode), http


MODES = ["legacy", "2026-07-28"]


@pytest.mark.parametrize("mode", MODES)
async def test_admin_lists_and_calls_admin_tool(stack, mode):
    url, mock = stack
    before = mock.calls["admin_reset_key"]
    client, http = sdk_client(url, "admin", mode)
    try:
        async with client as c:
            tools = await c.list_tools()
            result = await c.call_tool("admin_reset_key", {"user": "alice"})
    finally:
        await http.aclose()
    assert len(tools.tools) == 3
    assert result.is_error is False
    assert mock.calls["admin_reset_key"] == before + 1


@pytest.mark.parametrize("mode", MODES)
async def test_viewer_allowed_tool_ok_and_admin_tool_denied(stack, mode):
    url, mock = stack
    before = mock.calls["admin_reset_key"]
    client, http = sdk_client(url, "viewer", mode)
    try:
        async with client as c:
            ok = await c.call_tool("get_status")
            with pytest.raises(MCPError) as exc:
                await c.call_tool("admin_reset_key", {"user": "alice"})
    finally:
        await http.aclose()
    assert ok.is_error is False
    assert exc.value.code == -32001
    assert mock.calls["admin_reset_key"] == before


def minimal_env(**extra: str) -> dict[str, str]:
    keep = ("PATH", "SYSTEMROOT", "TEMP", "TMP")
    return {k: os.environ[k] for k in keep if k in os.environ} | extra


def test_main_refuses_short_secret():
    proc = subprocess.run(
        [sys.executable, "-m", "mcp_gateway"],
        env=minimal_env(GATEWAY_JWT_SECRET="short"),
        capture_output=True,
        timeout=30,
    )
    assert proc.returncode == 2
    assert b"GATEWAY_JWT_SECRET" in proc.stderr
    assert proc.stdout == b""
