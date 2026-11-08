import json

import httpx2
import pytest

from mock_mcp.server import build_mock

pytestmark = pytest.mark.anyio

HEADERS = {
    "content-type": "application/json",
    "accept": "application/json, text/event-stream",
}
INIT = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-11-25",
        "capabilities": {},
        "clientInfo": {"name": "t", "version": "0"},
    },
}


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _client(m, base_url="http://127.0.0.1:8001"):
    return httpx2.AsyncClient(transport=httpx2.ASGITransport(app=m.app), base_url=base_url)


async def _rpc(client, payload):
    return await client.post("/mcp", headers=HEADERS, content=json.dumps(payload))


async def test_tools_list_has_three_tools():
    m = build_mock(json_response=True)
    async with m.server.session_manager.run():
        async with _client(m) as c:
            init = await _rpc(c, INIT)
            assert init.status_code == 200
            r = await _rpc(c, {"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    assert r.status_code == 200
    names = {t["name"] for t in r.json()["result"]["tools"]}
    assert names == {"get_status", "list_users", "admin_reset_key"}


async def test_call_increments_counter():
    m = build_mock(json_response=True)
    async with m.server.session_manager.run():
        async with _client(m) as c:
            await _rpc(c, INIT)
            r = await _rpc(
                c,
                {
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "tools/call",
                    "params": {"name": "admin_reset_key", "arguments": {"user": "alice"}},
                },
            )
    assert r.status_code == 200
    assert r.json()["result"]["isError"] is False
    assert m.calls["admin_reset_key"] == 1
    assert m.calls["get_status"] == 0


async def test_foreign_host_rejected_421():
    m = build_mock(json_response=True)
    async with m.server.session_manager.run():
        async with _client(m, "http://mcp-gateway.internal:8001") as c:
            r = await _rpc(c, INIT)
    assert r.status_code == 421
