"""Application factory and the one endpoint, ``POST /v1/chat/completions``.

Request flow (each step only runs if the previous one succeeded):

1. a fresh ``uuid4().hex`` request id (the limiter's ``request_id`` column is UNIQUE);
2. ``authenticate`` (401 before the body is even read);
3. read the body (1 MiB cap, 413) and parse it: a JSON object with a ``messages`` list,
   ``stream: true`` rejected;
4. ``estimate_tokens`` then ``limiter.reserve`` (one reservation per request);
5. ``route`` (primary, failover to secondary), abandoned if the client disconnects;
6. settle the reservation in a shielded ``finally``: reconcile to ``usage.total_tokens`` on
   success, refund on every other exit (gateway error, unexpected exception, cancellation);
7. respond: the upstream JSON and 2xx status with our headers only, or a gateway error;
8. exactly one INFO log line.

Every error is the standard gateway payload with ``x-request-id``; unknown paths, other
methods and oversized bodies included.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import anyio
import httpx2
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import ClientDisconnect, Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from .auth import authenticate
from .config import Settings
from .context import install_request_id_filter, request_id_var
from .errors import (
    ClientDisconnected,
    GatewayError,
    InternalError,
    InvalidRequest,
    MethodNotAllowed,
    NotFound,
    PayloadTooLarge,
    error_response,
    get_request_id,
    unhandled_exception_handler,
)
from .limiter import AsyncLimiter, Reservation, SqliteTokenLimiter
from .router import RouteResult, is_json_safe, route
from .tokens import estimate_tokens

logger = logging.getLogger("llm_router")

CHAT_PATH = "/v1/chat/completions"
MAX_BODY_BYTES = 1024 * 1024
STREAMING_MESSAGE = "Streaming is not supported by this gateway"
_TENANT_PREFIX = 8


def _new_request_id(request: Request) -> str:
    """Always a fresh id: never reuse one that might linger in shared request state."""
    request.state.request_id = uuid.uuid4().hex
    return get_request_id(request)


@dataclass
class _RequestLog:
    request_id: str
    started: float
    tenant: str = "-"
    status: int = 0
    outcome: str = "-"
    provider: str = "-"
    fallback_reason: str = "-"
    est_tokens: int | None = None
    actual_tokens: int | None = None

    def fail(self, err: GatewayError) -> None:
        self.status, self.outcome = err.status, err.code

    def emit(self) -> None:
        logger.info(
            "request request_id=%s tenant=%s status=%d outcome=%s provider=%s "
            "fallback_reason=%s est_tokens=%s actual_tokens=%s duration_ms=%d",
            self.request_id,
            self.tenant,
            self.status,
            self.outcome,
            self.provider,
            self.fallback_reason,
            "-" if self.est_tokens is None else self.est_tokens,
            "-" if self.actual_tokens is None else self.actual_tokens,
            round((time.perf_counter() - self.started) * 1000),
        )


async def _read_body(request: Request) -> bytes:
    declared = request.headers.get("content-length")
    if declared is not None and declared.isdigit() and int(declared) > MAX_BODY_BYTES:
        raise PayloadTooLarge()
    buf = bytearray()
    async for chunk in request.stream():
        buf += chunk
        if len(buf) > MAX_BODY_BYTES:
            raise PayloadTooLarge()
    return bytes(buf)


def _parse(raw: bytes) -> dict[str, Any]:
    try:
        body = json.loads(raw)
    except (ValueError, RecursionError):  # bad JSON, bad UTF-8, or absurd nesting
        raise InvalidRequest("Request body must be valid JSON.") from None
    if not is_json_safe(body):  # NaN, 1e999 or a lone surrogate could never be forwarded
        raise InvalidRequest("Request body must be valid JSON.")
    if not isinstance(body, dict):
        raise InvalidRequest("Request body must be a JSON object.")
    if not isinstance(body.get("messages"), list):
        raise InvalidRequest("'messages' must be a list.", param="messages")
    stream = body.get("stream")
    if stream is True:
        raise InvalidRequest(STREAMING_MESSAGE, param="stream")
    if stream is not None and stream is not False:
        raise InvalidRequest("'stream' must be a boolean.", param="stream")
    return body


def _usage_tokens(body: dict[str, Any], estimate: int) -> int:
    usage = body.get("usage")
    total = usage.get("total_tokens") if isinstance(usage, dict) else None
    if isinstance(total, int) and not isinstance(total, bool) and total >= 0:
        return total
    return estimate  # never under-charge when upstream omits or garbles usage


def _success(result: RouteResult, reservation: Reservation, request_id: str) -> JSONResponse:
    headers = {
        "x-request-id": request_id,
        "x-gateway-provider": result.provider,
        "x-ratelimit-limit-tokens": str(reservation.limit),
        "x-ratelimit-remaining-tokens": str(reservation.remaining),
        "x-ratelimit-reset-tokens": f"{reservation.reset_s}s",
    }
    if result.fallback_reason is not None:
        headers["x-gateway-fallback-reason"] = result.fallback_reason
    return JSONResponse(result.body, status_code=result.status, headers=headers)


async def _settle(limiter: AsyncLimiter, request_id: str, actual: int | None) -> None:
    """Reconcile (actual is not None) or refund. Shielded; a failure is logged, not raised."""
    # The admission decision already stands; a failed settle only leaves the estimate
    # charged (never an under-charge). Detail stays server-side.
    action = "refund" if actual is None else "reconcile"
    with anyio.CancelScope(shield=True):
        try:
            if actual is None:
                await limiter.refund(request_id)
            else:
                await limiter.reconcile(request_id, actual)
        except GatewayError:  # RateLimiterUnavailable: the limiter already logged the detail
            logger.warning("could not %s reservation", action)
        except Exception:
            logger.warning("could not %s reservation", action, exc_info=True)


async def _route_until_disconnect(
    request: Request, client: httpx2.AsyncClient, settings: Settings, body: dict[str, Any]
) -> RouteResult:
    """Run ``route``, abandoning it if the client goes away.

    Starlette delivers no cancellation on disconnect, so without this a client that left
    during a stalled primary would still trigger failover and be charged for it. Call this
    only after the body is fully read, so the watcher can never consume a body message.

    A result that was already produced is kept even if the disconnect lands at the same
    moment. Exceptions are carried out of the group by hand so callers see the original
    exception rather than an ExceptionGroup.
    """
    result: RouteResult | None = None
    failure: Exception | None = None

    async def watch(scope: anyio.CancelScope) -> None:
        while (await request.receive())["type"] != "http.disconnect":
            pass
        scope.cancel()

    async with anyio.create_task_group() as tg:
        tg.start_soon(watch, tg.cancel_scope)
        try:
            result = await route(client, settings, body)
        except Exception as exc:
            failure = exc
        tg.cancel_scope.cancel()
    if failure is not None:
        raise failure
    if result is None:
        raise ClientDisconnected()
    return result


async def _handle(request: Request, request_id: str, log: _RequestLog) -> Response:
    state = request.app.state
    settings: Settings = state.settings
    limiter: AsyncLimiter = state.limiter
    reserve_attempted = False
    actual: int | None = None  # set only once a response is ready: anything else refunds
    try:
        tenant = authenticate(request.headers, settings)
        log.tenant = tenant[:_TENANT_PREFIX]
        try:
            raw_body = await _read_body(request)
        except ClientDisconnect:
            raise ClientDisconnected() from None
        body = _parse(raw_body)
        estimate, forward = estimate_tokens(body, settings.default_max_tokens)
        log.est_tokens = estimate
        reserve_attempted = True  # reserve is not shielded: a cancelled one may still commit
        reservation = await limiter.reserve(tenant, request_id, estimate)
        result = await _route_until_disconnect(request, state.upstream, settings, forward)
        log.provider = result.provider
        log.fallback_reason = result.fallback_reason or "-"
        response = _success(result, reservation, request_id)
        actual = _usage_tokens(result.body, estimate)
    except GatewayError as err:
        log.fail(err)
        return error_response(err, request_id)
    except Exception:
        logger.warning("unhandled exception request_id=%s", request_id, exc_info=True)
        err = InternalError()
        log.fail(err)
        return error_response(err, request_id)
    finally:
        if reserve_attempted:
            await _settle(limiter, request_id, actual)
    log.status, log.outcome, log.actual_tokens = response.status_code, "ok", actual
    return response


async def chat_completions(request: Request) -> Response:
    request_id = _new_request_id(request)
    token = request_id_var.set(request_id)
    log = _RequestLog(request_id, time.perf_counter())
    try:
        return await _handle(request, request_id, log)
    except anyio.get_cancelled_exc_class():
        log.outcome = "client_disconnected"
        raise
    finally:
        log.emit()
        request_id_var.reset(token)


def _front_door(err_cls: type[GatewayError]):
    async def handler(request: Request, exc: HTTPException) -> Response:
        request_id = _new_request_id(request)
        token = request_id_var.set(request_id)
        try:
            err = err_cls()
            log = _RequestLog(request_id, time.perf_counter())
            log.fail(err)
            log.emit()
            return error_response(err, request_id)
        finally:
            request_id_var.reset(token)

    return handler


def create_app(
    settings: Settings,
    upstream_client: httpx2.AsyncClient | None = None,
    limiter: AsyncLimiter | None = None,
) -> Starlette:
    """Build the router app. Injected dependencies are used as-is and never closed here."""

    @asynccontextmanager
    async def lifespan(app: Starlette):
        owned_limiter = owned_client = None
        try:
            if limiter is None:
                owned_limiter = AsyncLimiter(
                    SqliteTokenLimiter(
                        settings.db_path, settings.tokens_per_minute, settings.window_seconds
                    )
                )
                app.state.limiter = owned_limiter
            if upstream_client is None:
                owned_client = httpx2.AsyncClient(follow_redirects=False, trust_env=False)
                owned_client.headers.clear()  # never send httpx2 defaults (user-agent etc.)
                app.state.upstream = owned_client
            yield
        finally:
            if owned_client is not None:
                await owned_client.aclose()
            if owned_limiter is not None:
                owned_limiter.close()

    install_request_id_filter(logger)
    app = Starlette(
        routes=[Route(CHAT_PATH, chat_completions, methods=["POST"])],
        exception_handlers={
            404: _front_door(NotFound),
            405: _front_door(MethodNotAllowed),
            Exception: unhandled_exception_handler,
        },
        lifespan=lifespan,
    )
    app.router.redirect_slashes = False
    app.state.settings = settings
    if upstream_client is not None:
        app.state.upstream = upstream_client
    if limiter is not None:
        app.state.limiter = limiter
    return app
