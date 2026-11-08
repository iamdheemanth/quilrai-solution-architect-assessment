"""Shared fixtures for gateway tests: tokens, request builders and an in-process gateway."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager

import httpx2

from mcp_gateway.app import create_app
from mcp_gateway.config import Settings
from mcp_gateway.tokens import mint_token

TEST_SECRET = "test-secret-" + "x" * 24


def token(role: str, **kw) -> str:
    return mint_token(TEST_SECRET, role, **kw)


def headers(role: str | None = "viewer", **extra) -> dict:
    out = {"content-type": "application/json", "accept": "application/json, text/event-stream"}
    if role is not None:
        out["authorization"] = f"Bearer {token(role)}"
    out.update(extra)
    return out


def rpc(method, params=None, id=1) -> bytes:
    msg: dict = {"jsonrpc": "2.0", "id": id, "method": method}
    if params is not None:
        msg["params"] = params
    return json.dumps(msg).encode()


def default_handler(request: httpx2.Request) -> httpx2.Response:
    try:
        rpc_id = json.loads(request.content).get("id")
    except ValueError:
        rpc_id = None
    body = json.dumps({"jsonrpc": "2.0", "id": rpc_id, "result": {}}).encode()
    return httpx2.Response(200, headers={"content-type": "application/json"}, content=body)


class _Chunks(httpx2.AsyncByteStream):
    """Unread response stream, like a real transport (a prebuilt Response is already consumed)."""

    def __init__(self, content: bytes) -> None:
        self._content = content

    async def __aiter__(self):
        yield self._content


@asynccontextmanager
async def gateway(handler=None, **settings_overrides):
    """Yield ``(client, calls)``: a client driving the gateway over a recording MockTransport."""
    calls: list[httpx2.Request] = []
    inner = handler or default_handler

    async def record(request: httpx2.Request) -> httpx2.Response:
        await request.aread()
        calls.append(request)
        result = inner(request)
        if hasattr(result, "__await__"):
            result = await result
        return httpx2.Response(
            result.status_code, headers=result.headers, stream=_Chunks(result.content)
        )

    settings = Settings(**{"jwt_secret": TEST_SECRET, **settings_overrides})
    upstream = httpx2.AsyncClient(
        base_url="http://127.0.0.1:8001", transport=httpx2.MockTransport(record)
    )
    upstream.headers.clear()
    app = create_app(settings, upstream_client=upstream)
    client = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app), base_url="http://gateway")
    try:
        yield client, calls
    finally:
        await client.aclose()
        await upstream.aclose()
