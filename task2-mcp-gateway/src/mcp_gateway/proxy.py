"""Transparent forwarding to the configured downstream (the ``/mcp`` route endpoint)."""

from __future__ import annotations

from urllib.parse import urlsplit

import anyio
import httpx2
from mcp_types import INTERNAL_ERROR
from starlette.background import BackgroundTask
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse

from .jsonrpc import error_body

HOP_BY_HOP = frozenset(
    {
        b"connection",
        b"keep-alive",
        b"proxy-connection",
        b"te",
        b"trailer",
        b"transfer-encoding",
        b"upgrade",
    }
)
REQUEST_DROP = HOP_BY_HOP | frozenset(
    {
        b"authorization",
        b"cookie",
        b"host",
        b"content-length",
        b"origin",
        b"proxy-authorization",
        b"x-request-id",
        b"accept-encoding",  # replaced by "identity" so the relayed body is never compressed
    }
)
RESPONSE_DROP = HOP_BY_HOP | frozenset({b"server", b"date"})
_GATEWAY_PREFIX = b"x-gateway-"


def filter_headers(
    raw: list[tuple[bytes, bytes]], drop: frozenset[bytes]
) -> list[tuple[bytes, bytes]]:
    """Lowercase names; drop ``drop``, ``x-gateway-*`` and every name listed in ``connection``."""
    named: set[bytes] = set()
    for name, value in raw:
        if name.lower() == b"connection":
            named.update(t.strip().lower() for t in value.split(b",") if t.strip())
    out: list[tuple[bytes, bytes]] = []
    for name, value in raw:
        lowered = name.lower()
        if lowered in drop or lowered in named or lowered.startswith(_GATEWAY_PREFIX):
            continue
        out.append((lowered, value))
    return out


def json_response(status: int, rpc_id, message: str, code: int = INTERNAL_ERROR) -> Response:
    return Response(
        error_body(rpc_id, code, message), status_code=status, media_type="application/json"
    )


async def proxy_endpoint(request: Request) -> Response:
    client: httpx2.AsyncClient = request.app.state.upstream
    settings = request.app.state.settings
    state = request.scope.get("state", {})
    rpc = state.get("rpc")
    rpc_id = rpc.id if rpc is not None else None
    body: bytes = state.get("body", b"")
    target = urlsplit(settings.downstream_url).path or "/"
    query = request.scope.get("query_string", b"").decode("latin-1")
    if query:
        target = f"{target}?{query}"

    headers = filter_headers(request.headers.raw, REQUEST_DROP)
    headers.append((b"accept-encoding", b"identity"))
    headers.append((b"x-request-id", state.get("request_id", "").encode("ascii")))
    up_req = client.build_request(request.method, target, headers=headers, content=body)
    try:
        upstream = await client.send(up_req, stream=True)
    except httpx2.TimeoutException:  # subclass of TransportError: catch first
        return json_response(504, rpc_id, "Downstream timeout")
    except httpx2.TransportError:
        return json_response(502, rpc_id, "Downstream unavailable")

    async def relay():
        try:
            async for chunk in upstream.aiter_raw():
                yield chunk
        finally:
            with anyio.CancelScope(shield=True):
                await upstream.aclose()

    response = StreamingResponse(
        relay(), status_code=upstream.status_code, background=BackgroundTask(upstream.aclose)
    )
    response.raw_headers = filter_headers(upstream.headers.raw, RESPONSE_DROP)
    return response
