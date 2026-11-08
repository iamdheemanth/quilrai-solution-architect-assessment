"""Forwarding to the upstream provider, with a fail-closed redacting stream relay."""

from __future__ import annotations

import contextlib
import json
import logging
import time
from collections.abc import AsyncIterator, Callable

import anyio
import httpx2
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse

from .sse import SSEParser
from .transform import ERROR_FRAME, StreamTransformer, redact_completion, redact_error_body

logger = logging.getLogger("llm_gateway")

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
        b"proxy-authorization",
        b"accept-encoding",  # replaced by "identity" so the relayed body is never compressed
    }
)
SSE_HEADERS = {
    "content-type": "text/event-stream; charset=utf-8",
    "cache-control": "no-cache",
    "x-accel-buffering": "no",
}


def openai_error(status: int, message: str, type_: str, code: str) -> JSONResponse:
    body = {"error": {"message": message, "type": type_, "code": code}}
    return JSONResponse(body, status_code=status)


def filter_request_headers(raw: list[tuple[bytes, bytes]]) -> list[tuple[bytes, bytes]]:
    """Lowercase names; drop ``REQUEST_DROP`` and every name listed in ``connection``."""
    named: set[bytes] = set()
    for name, value in raw:
        if name.lower() == b"connection":
            named.update(t.strip().lower() for t in value.split(b",") if t.strip())
    out: list[tuple[bytes, bytes]] = []
    for name, value in raw:
        lowered = name.lower()
        if lowered in REQUEST_DROP or lowered in named:
            continue
        out.append((lowered, value))
    return out


def log_request(
    method: str,
    path: str,
    status: int | None,
    outcome: str,
    started: float,
    stats: dict | None = None,
) -> None:
    """One INFO line per request: metadata and redaction counts only, never content."""
    stats = stats or {}
    logger.info(
        "request method=%s path=%s upstream_status=%s outcome=%s redactions=%s "
        "max_held=%d duration_ms=%.1f",
        method,
        path,
        status if status is not None else "-",
        outcome,
        json.dumps(stats.get("counts", {}), sort_keys=True),
        stats.get("max_held", 0),
        (time.perf_counter() - started) * 1000,
    )


class ClosingStreamingResponse(StreamingResponse):
    """Close the body iterator when streaming ends or is cancelled, not at GC time."""

    async def stream_response(self, send) -> None:
        async with contextlib.aclosing(self.body_iterator):
            await super().stream_response(send)


async def relay(
    upstream: httpx2.Response,
    tx: StreamTransformer,
    on_done: Callable[[str], None] | None = None,
) -> AsyncIterator[bytes]:
    """Redact an upstream SSE body; fail closed with an error frame on any exception."""
    parser = SSEParser()
    outcome = "stream_failed"
    try:
        async for raw in upstream.aiter_bytes():
            out = b"".join(tx.on_event(ev) for ev in parser.feed(raw))
            if out:
                yield out
            if tx.done:
                break
        tail = tx.on_eof()
        if tail:
            yield tail
        if tail != ERROR_FRAME:
            outcome = "upstream_error" if upstream.status_code >= 400 else "ok"
    except Exception as exc:  # transport drop, parse error, bad frame: never log content
        logger.warning("stream failed closed: %s", type(exc).__name__)
        tx.discard()
        yield ERROR_FRAME
    finally:
        with anyio.CancelScope(shield=True):
            await upstream.aclose()
        if on_done is not None:
            on_done(outcome)


async def _read_capped(upstream: httpx2.Response, cap: int) -> bytes | None:
    buf = bytearray()
    async for part in upstream.aiter_bytes():
        buf += part
        if len(buf) > cap:
            return None
    return bytes(buf)


def _media_type(upstream: httpx2.Response) -> str:
    return upstream.headers.get("content-type", "").split(";", 1)[0].strip().lower()


async def proxy_endpoint(request: Request) -> Response:
    started = time.perf_counter()
    client: httpx2.AsyncClient = request.app.state.upstream
    settings = request.app.state.settings
    method, path = request.method, request.url.path

    def done(status: int | None, outcome: str, stats: dict | None = None) -> None:
        log_request(method, path, status, outcome, started, stats)

    body = await request.body()
    url = settings.upstream_base_url.rstrip("/") + "/chat/completions"
    query = request.scope.get("query_string", b"").decode("latin-1")
    if query:
        url = f"{url}?{query}"

    headers = filter_request_headers(request.headers.raw)
    if settings.upstream_api_key:
        headers.append((b"authorization", f"Bearer {settings.upstream_api_key}".encode()))
    headers.append((b"accept-encoding", b"identity"))
    up_req = client.build_request("POST", url, headers=headers, content=body)

    try:
        upstream = await client.send(up_req, stream=True)
    except httpx2.TimeoutException as exc:  # subclass of TransportError: catch first
        logger.warning("upstream call failed: %s", type(exc).__name__)
        done(None, "timeout")
        return openai_error(504, "Upstream timeout", "upstream_error", "upstream_timeout")
    except httpx2.TransportError as exc:
        logger.warning("upstream call failed: %s", type(exc).__name__)
        done(None, "bad_gateway")
        return openai_error(502, "Upstream unavailable", "upstream_error", "upstream_unavailable")

    status = upstream.status_code
    media = _media_type(upstream)

    if media == "text/event-stream":
        tx = StreamTransformer()

        def finish(outcome: str) -> None:
            done(status, outcome, tx.stats())

        return ClosingStreamingResponse(
            relay(upstream, tx, finish), status_code=status, headers=SSE_HEADERS
        )

    try:
        if media == "application/json" or media.endswith("+json"):
            raw = await _read_capped(upstream, settings.max_body_bytes)
            if raw is not None:
                obj = json.loads(raw)
                if isinstance(obj, dict):
                    obj = redact_completion(obj) if status < 400 else redact_error_body(obj)
                    outcome = "ok" if status < 400 else "upstream_error"
                    done(status, outcome)
                    return JSONResponse(obj, status_code=status)
    except httpx2.TimeoutException as exc:
        logger.warning("upstream call failed: %s", type(exc).__name__)
        done(status, "timeout")
        return openai_error(504, "Upstream timeout", "upstream_error", "upstream_timeout")
    except (httpx2.TransportError, ValueError) as exc:
        logger.warning("upstream body unusable: %s", type(exc).__name__)
    finally:
        with anyio.CancelScope(shield=True):
            await upstream.aclose()
    done(status, "bad_gateway")
    return openai_error(502, "Invalid upstream response", "upstream_error", "bad_gateway")
