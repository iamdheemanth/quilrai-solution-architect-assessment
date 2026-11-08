"""Layer 2: gateway in front of the real SDK mock server, wired in-process."""

from __future__ import annotations

from contextlib import asynccontextmanager

import httpx2
import pytest

from mcp_gateway.app import create_app
from mcp_gateway.config import Settings
from mock_mcp.server import build_mock
from tests.helpers import TEST_SECRET, headers, rpc

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


@asynccontextmanager
async def stack():
    """Yield ``(client, mock)`` with the gateway proxying to a fresh in-process mock."""
    mock = build_mock(json_response=True)
    upstream = httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=mock.app), base_url="http://127.0.0.1:8001"
    )
    upstream.headers.clear()
    app = create_app(Settings(jwt_secret=TEST_SECRET), upstream_client=upstream)
    client = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app), base_url="http://gateway")
    try:
        async with mock.server.session_manager.run():
            yield client, mock
    finally:
        await client.aclose()
        await upstream.aclose()


def call(name, arguments=None, id=1):
    return rpc("tools/call", {"name": name, "arguments": arguments or {}}, id=id)


async def test_viewer_admin_call_denied_and_never_reaches_mock():
    async with stack() as (client, mock):
        resp = await client.post(
            "/mcp", content=call("admin_reset_key", {"user": "a"}), headers=headers("viewer")
        )
    assert resp.json()["error"]["code"] == -32001
    assert mock.calls["admin_reset_key"] == 0


async def test_admin_admin_call_reaches_mock():
    async with stack() as (client, mock):
        resp = await client.post(
            "/mcp", content=call("admin_reset_key", {"user": "a"}), headers=headers("admin")
        )
    body = resp.json()
    assert resp.status_code == 200
    assert "error" not in body
    assert body["result"]["isError"] is False
    assert mock.calls["admin_reset_key"] == 1


async def test_viewer_tools_list_is_transparent():
    async with stack() as (client, _mock):
        resp = await client.post("/mcp", content=rpc("tools/list"), headers=headers("viewer"))
    names = {t["name"] for t in resp.json()["result"]["tools"]}
    assert names == {"get_status", "list_users", "admin_reset_key"}


async def test_duplicate_key_body_rejected_and_nothing_called():
    body = (
        b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":'
        b'{"name":"get_status","name":"admin_reset_key","arguments":{}}}'
    )
    async with stack() as (client, mock):
        resp = await client.post("/mcp", content=body, headers=headers("viewer"))
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == -32700
    assert sum(mock.calls.values()) == 0
