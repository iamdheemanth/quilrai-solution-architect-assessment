"""Application factory wiring middleware order: auth, then policy, then the proxy route."""

from __future__ import annotations

from contextlib import asynccontextmanager
from urllib.parse import urlsplit

import httpx2
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.routing import Route

from .config import Settings
from .middleware import MCP_PATH, BearerAuthMiddleware, JsonRpcPolicyMiddleware
from .proxy import proxy_endpoint


def _origin(url: str) -> str:
    parts = urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}"


def create_app(settings: Settings, upstream_client: httpx2.AsyncClient | None = None) -> Starlette:
    @asynccontextmanager
    async def lifespan(app: Starlette):
        owned = None
        if upstream_client is None:
            owned = httpx2.AsyncClient(
                base_url=_origin(settings.downstream_url),
                follow_redirects=False,
                timeout=httpx2.Timeout(connect=5, read=300, write=30, pool=5),
            )
            owned.headers.clear()  # never leak httpx2 defaults (accept */*, user-agent)
            app.state.upstream = owned
        try:
            yield
        finally:
            if owned is not None:
                await owned.aclose()

    app = Starlette(
        routes=[Route(MCP_PATH, proxy_endpoint, methods=["POST", "GET", "DELETE"])],
        middleware=[
            Middleware(BearerAuthMiddleware, settings=settings),
            Middleware(JsonRpcPolicyMiddleware, settings=settings),
        ],
        max_body_size=settings.max_body_bytes,
        lifespan=lifespan,
    )
    app.state.settings = settings
    if upstream_client is not None:
        app.state.upstream = upstream_client
    return app
