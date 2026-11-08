"""Application factory: one POST route, JSON errors for everything else."""

from __future__ import annotations

import time
from contextlib import asynccontextmanager

import httpx2
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from .config import Settings
from .proxy import log_request, openai_error, proxy_endpoint

CHAT_PATH = "/v1/chat/completions"


def _front_door(status: int, outcome: str, message: str, code: str, allow: str | None = None):
    def handler(request: Request, exc: Exception) -> Response:
        started = request.scope.get("_started", time.perf_counter())
        log_request(request.method, "unknown", None, outcome, started)
        response = openai_error(status, message, "invalid_request_error", code)
        if allow:
            response.headers["allow"] = allow
        return response

    return handler


class _StartClock:
    """Stamp request start so front-door handlers can report a real duration."""

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] == "http":
            scope["_started"] = time.perf_counter()
        await self.app(scope, receive, send)


def create_app(settings: Settings, upstream_client: httpx2.AsyncClient | None = None) -> Starlette:
    @asynccontextmanager
    async def lifespan(app: Starlette):
        owned = None
        if upstream_client is None:
            owned = httpx2.AsyncClient(
                follow_redirects=False,
                timeout=httpx2.Timeout(
                    connect=5, read=settings.upstream_read_timeout, write=30, pool=5
                ),
            )
            owned.headers.clear()  # never leak httpx2 defaults (accept */*, user-agent)
            app.state.upstream = owned
        try:
            yield
        finally:
            if owned is not None:
                await owned.aclose()

    app = Starlette(
        routes=[Route(CHAT_PATH, proxy_endpoint, methods=["POST"])],
        middleware=[Middleware(_StartClock)],
        exception_handlers={
            404: _front_door(404, "not_found", "Not found", "not_found"),
            405: _front_door(
                405, "method_not_allowed", "Method not allowed", "method_not_allowed", "POST"
            ),
        },
        max_body_size=settings.max_body_bytes,
        lifespan=lifespan,
    )
    app.router.redirect_slashes = False
    app.state.settings = settings
    if upstream_client is not None:
        app.state.upstream = upstream_client
    return app
