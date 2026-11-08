"""Application factory: one POST route, JSON errors for everything else."""

from __future__ import annotations

import time
from contextlib import asynccontextmanager

import httpx2
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from .config import Settings
from .proxy import log_request, openai_error, proxy_endpoint

CHAT_PATH = "/v1/chat/completions"


def _not_found(request: Request, exc: Exception) -> Response:
    log_request(request.method, request.url.path, None, "not_found", time.perf_counter())
    return openai_error(404, "Not found", "invalid_request_error", "not_found")


def _method_not_allowed(request: Request, exc: Exception) -> Response:
    log_request(request.method, request.url.path, None, "method_not_allowed", time.perf_counter())
    response = openai_error(
        405, "Method not allowed", "invalid_request_error", "method_not_allowed"
    )
    response.headers["allow"] = "POST"
    return response


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
        exception_handlers={404: _not_found, 405: _method_not_allowed},
        max_body_size=settings.max_body_bytes,
        lifespan=lifespan,
    )
    app.state.settings = settings
    if upstream_client is not None:
        app.state.upstream = upstream_client
    return app
