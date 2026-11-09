"""The wired app in-process: ASGITransport in front, MockTransport upstreams behind."""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import sys
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass

import anyio
import httpx2
import pytest

import llm_router.__main__ as router_main
from llm_router.app import MAX_BODY_BYTES, create_app
from llm_router.auth import tenant_id
from llm_router.config import load_settings
from llm_router.context import RequestIdFilter
from llm_router.errors import unhandled_exception_handler
from llm_router.limiter import AsyncLimiter, SqliteTokenLimiter
from llm_router.tokens import estimate_tokens

pytestmark = pytest.mark.anyio

KEY = "tk-alpha-0123456789"
OTHER_KEY = "tk-beta-9876543210"
PEPPER = "pepper-for-tests-0123456789"
PRIMARY_KEY = "pk-primary-provider-secret"
SECONDARY_KEY = "sk-secondary-provider-secret"
PROMPT = "PROMPT-CANARY-do-not-log"
BODY = {
    "model": "client-model",
    "messages": [{"role": "user", "content": PROMPT}],
    "max_tokens": 64,
}
EST = estimate_tokens(BODY, 1024)[0]
T0_MS = 1_700_000_000_000
RID_RE = re.compile(r"^[0-9a-f]{32}$")

Handler = Callable[[httpx2.Request], Awaitable[httpx2.Response]]


@pytest.fixture
def anyio_backend():
    return "asyncio"


class FakeClock:
    def __init__(self, ms: int = T0_MS) -> None:
        self.ms = ms

    def __call__(self) -> float:
        return self.ms / 1000


def make_settings(db_path: str, **extra: str):
    env = {
        "ROUTER_TENANT_KEYS": f"{KEY}=alpha,{OTHER_KEY}=beta",
        "ROUTER_KEY_PEPPER": PEPPER,
        "ROUTER_DB_PATH": db_path,
        "PRIMARY_BASE_URL": "http://primary.internal:9001/v1",
        "PRIMARY_API_KEY": PRIMARY_KEY,
        "PRIMARY_MODEL": "primary-model",
        "SECONDARY_BASE_URL": "http://secondary.internal:9002/v1",
        "SECONDARY_API_KEY": SECONDARY_KEY,
        "SECONDARY_MODEL": "secondary-model",
        "PRIMARY_TIMEOUT_MS": "200",
        "SECONDARY_TIMEOUT_MS": "200",
    }
    env.update(extra)
    return load_settings(env)


# --- upstream handlers ------------------------------------------------------------------


def ok(name: str, tokens: int = 7, status: int = 200) -> Handler:
    async def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(
            status,
            json={"id": f"cmpl-{name}", "served_by": name, "usage": {"total_tokens": tokens}},
            headers={"x-upstream-secret": "upstream-internal-header", "server": "nginx/1.2"},
        )

    return handler


def status(code: int, body: dict | None = None) -> Handler:
    async def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(code, json=body or {"error": {"message": f"upstream {code}"}})

    return handler


def stall(flags: dict | None = None) -> Handler:
    async def handler(request: httpx2.Request) -> httpx2.Response:
        if flags is not None:
            flags["entered"].set()
        await anyio.Event().wait()
        raise AssertionError("unreachable")

    return handler


class Upstreams:
    def __init__(self, primary: Handler, secondary: Handler) -> None:
        self.primary, self.secondary = primary, secondary
        self.calls: list[str] = []
        self.requests: list[httpx2.Request] = []

    async def __call__(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        if request.url.host == "primary.internal":
            self.calls.append("primary")
            return await self.primary(request)
        self.calls.append("secondary")
        return await self.secondary(request)


@dataclass
class Gw:
    client: httpx2.AsyncClient
    up: Upstreams
    core: object
    db: str
    settings: object

    async def post(self, body=BODY, key: str | None = KEY, **kw) -> httpx2.Response:
        headers = dict(kw.pop("headers", {}))
        if key is not None:
            headers.setdefault("Authorization", f"Bearer {key}")
        if "content" not in kw:
            kw["content"] = body if isinstance(body, bytes) else json.dumps(body).encode()
        return await self.client.post("/v1/chat/completions", headers=headers, **kw)

    def rows(self) -> list[tuple]:
        with sqlite3.connect(self.db) as c:
            out = c.execute("SELECT tenant, tokens, request_id FROM usage ORDER BY id").fetchall()
        c.close()
        return out


@asynccontextmanager
async def gateway(
    tmp_path, primary=None, secondary=None, *, core=None, clock=None, **env
) -> AsyncIterator[Gw]:
    db = str(tmp_path / "router.db")
    settings = make_settings(db, **env)
    up = Upstreams(primary or ok("primary"), secondary or ok("secondary"))
    if core is None:
        core = SqliteTokenLimiter(
            db, settings.tokens_per_minute, settings.window_seconds, clock=clock or FakeClock()
        )
    upstream = httpx2.AsyncClient(transport=httpx2.MockTransport(up))
    app = create_app(settings, upstream_client=upstream, limiter=AsyncLimiter(core))
    client = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app), base_url="http://router")
    try:
        yield Gw(client, up, core, db, settings)
    finally:
        await client.aclose()
        await upstream.aclose()
        core.close()


def assert_error(
    resp: httpx2.Response,
    status_code: int,
    code: str,
    type_: str,
    message: str,
    param: str | None = None,
    retry_after: str | None = None,
) -> str:
    assert resp.status_code == status_code
    assert resp.headers["content-type"] == "application/json"
    rid = resp.headers["x-request-id"]
    assert RID_RE.match(rid)
    assert resp.json() == {
        "error": {
            "message": message,
            "type": type_,
            "param": param,
            "code": code,
            "request_id": rid,
        }
    }
    assert resp.headers.get("retry-after") == retry_after
    assert "x-gateway-provider" not in resp.headers
    return rid


def info_records(caplog) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.name == "llm_router" and r.levelno == logging.INFO]


# --- success ----------------------------------------------------------------------------


async def test_success_via_primary_relays_body_with_exact_headers(tmp_path):
    async with gateway(tmp_path) as gw:
        resp = await gw.post()
        rows = gw.rows()
    assert resp.status_code == 200
    assert resp.json() == {
        "id": "cmpl-primary",
        "served_by": "primary",
        "usage": {"total_tokens": 7},
    }
    rid = resp.headers["x-request-id"]
    assert RID_RE.match(rid)
    assert set(resp.headers.keys()) == {
        "content-length",
        "content-type",
        "x-request-id",
        "x-gateway-provider",
        "x-ratelimit-limit-tokens",
        "x-ratelimit-remaining-tokens",
        "x-ratelimit-reset-tokens",
    }
    assert resp.headers["content-type"] == "application/json"
    assert resp.headers["x-gateway-provider"] == "primary"
    assert resp.headers["x-ratelimit-limit-tokens"] == "50000"
    assert resp.headers["x-ratelimit-remaining-tokens"] == str(50000 - EST)
    assert resp.headers["x-ratelimit-reset-tokens"] == "60s"
    assert gw.up.calls == ["primary"]
    assert rows == [(tenant_id(PEPPER, KEY), 7, rid)]


async def test_failover_on_primary_429_is_charged_once_at_secondary_usage(tmp_path):
    async with gateway(tmp_path, status(429), ok("secondary", tokens=33)) as gw:
        resp = await gw.post()
        rows = gw.rows()
    assert resp.status_code == 200
    assert resp.json()["served_by"] == "secondary"
    assert resp.headers["x-gateway-provider"] == "secondary"
    assert resp.headers["x-gateway-fallback-reason"] == "rate_limited"
    assert gw.up.calls == ["primary", "secondary"]
    assert rows == [(tenant_id(PEPPER, KEY), 33, resp.headers["x-request-id"])]


async def test_failover_on_primary_timeout_ties_warning_to_request_id(tmp_path, caplog):
    async with gateway(tmp_path, stall(), ok("secondary")) as gw:
        with caplog.at_level(logging.INFO, logger="llm_router"):
            resp = await gw.post()
    assert resp.headers["x-gateway-provider"] == "secondary"
    assert resp.headers["x-gateway-fallback-reason"] == "timeout"
    rid = resp.headers["x-request-id"]
    [warning] = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warning.getMessage() == "upstream primary timed out after 200 ms (TimeoutError)"
    assert warning.request_id == rid
    [info] = info_records(caplog)
    assert info.request_id == rid


async def test_upstream_2xx_status_is_relayed(tmp_path):
    async with gateway(tmp_path, ok("primary", status=201)) as gw:
        resp = await gw.post()
    assert resp.status_code == 201
    assert resp.headers["x-gateway-provider"] == "primary"


@pytest.mark.parametrize(
    "usage",
    [None, {}, {"total_tokens": "12"}, {"total_tokens": True}, {"total_tokens": -1}, "x"],
    ids=["missing", "empty", "string", "bool", "negative", "not-object"],
)
async def test_unusable_usage_keeps_the_estimate(tmp_path, usage):
    async def primary(request):
        body = {"id": "x"} if usage is None else {"id": "x", "usage": usage}
        return httpx2.Response(200, json=body)

    async with gateway(tmp_path, primary) as gw:
        resp = await gw.post()
        rows = gw.rows()
    assert resp.status_code == 200
    assert [r[1] for r in rows] == [EST]


async def test_usage_zero_is_reconciled_to_zero(tmp_path):
    async with gateway(tmp_path, ok("primary", tokens=0)) as gw:
        await gw.post()
        assert [r[1] for r in gw.rows()] == [0]


async def test_each_request_gets_a_fresh_request_id_and_its_own_row(tmp_path):
    async with gateway(tmp_path) as gw:
        r1 = await gw.post()
        r2 = await gw.post()
        rows = gw.rows()
    assert r1.status_code == r2.status_code == 200
    assert r1.headers["x-request-id"] != r2.headers["x-request-id"]
    assert [r[2] for r in rows] == [r1.headers["x-request-id"], r2.headers["x-request-id"]]
    assert r2.headers["x-ratelimit-remaining-tokens"] == str(50000 - 7 - EST)


async def test_default_max_tokens_is_injected_and_reserved(tmp_path):
    body = {"messages": [{"role": "user", "content": "hi"}]}
    est, _ = estimate_tokens(body, 1024)
    async with gateway(tmp_path, ok("primary", tokens=-5)) as gw:  # unusable usage
        resp = await gw.post(body)
        rows = gw.rows()
    assert resp.status_code == 200
    assert json.loads(gw.up.requests[0].content)["max_tokens"] == 1024
    assert rows[0][1] == est


async def test_forwarded_request_carries_provider_key_not_client_credentials(tmp_path):
    async with gateway(tmp_path) as gw:
        await gw.post(headers={"Cookie": "session=client-cookie", "x-api-key": OTHER_KEY})
    [req] = gw.up.requests
    assert req.headers.get_list("authorization") == [f"Bearer {PRIMARY_KEY}"]
    assert set(req.headers.keys()) == {"host", "authorization", "content-type", "content-length"}
    blob = str(req.headers.raw) + req.content.decode()
    for secret in (KEY, OTHER_KEY, "client-cookie"):
        assert secret not in blob
    assert json.loads(req.content) == {**BODY, "model": "primary-model"}


# --- the error table, end to end ---------------------------------------------------------


@pytest.mark.parametrize(
    "headers",
    [{}, {"Authorization": "Bearer wrong-key"}, {"Authorization": "Basic abc"}],
    ids=["missing", "wrong", "not-bearer"],
)
async def test_401_invalid_api_key(tmp_path, headers):
    async with gateway(tmp_path) as gw:
        resp = await gw.post(key=None, headers=headers)
        rows = gw.rows()
    assert_error(
        resp, 401, "invalid_api_key", "authentication_error", "Missing or invalid API key."
    )
    assert gw.up.calls == [] and rows == []


async def test_401_comes_before_body_parsing(tmp_path):
    async with gateway(tmp_path) as gw:
        resp = await gw.post(b"{not json", key=None)
    assert resp.status_code == 401


@pytest.mark.parametrize(
    ("content", "message", "param"),
    [
        (b"{not json", "Request body must be valid JSON.", None),
        (b"\xff\xfe", "Request body must be valid JSON.", None),
        (b"[" * 100_000 + b"]" * 100_000, "Request body must be valid JSON.", None),
        (b"[]", "Request body must be a JSON object.", None),
        (b'"text"', "Request body must be a JSON object.", None),
        (b"{}", "'messages' must be a list.", "messages"),
        (b'{"messages": "hi"}', "'messages' must be a list.", "messages"),
        (
            b'{"messages": [], "stream": true}',
            "Streaming is not supported by this gateway",
            "stream",
        ),
        (b'{"messages": [], "stream": "yes"}', "'stream' must be a boolean.", "stream"),
        (b'{"messages": [], "stream": 1}', "'stream' must be a boolean.", "stream"),
        (
            b'{"messages": [], "max_tokens": -1}',
            "'max_tokens' must be a non-negative integer",
            "max_tokens",
        ),
    ],
    ids=[
        "bad-json",
        "bad-utf8",
        "deep-nesting",
        "array",
        "string",
        "no-messages",
        "messages-not-list",
        "stream-true",
        "stream-string",
        "stream-int",
        "bad-max-tokens",
    ],
)
async def test_400_invalid_request(tmp_path, content, message, param):
    async with gateway(tmp_path) as gw:
        resp = await gw.post(content)
        rows = gw.rows()
    assert_error(resp, 400, "invalid_request", "invalid_request_error", message, param)
    assert gw.up.calls == [] and rows == []


async def test_stream_false_is_accepted(tmp_path):
    async with gateway(tmp_path) as gw:
        resp = await gw.post({**BODY, "stream": False})
    assert resp.status_code == 200


async def test_400_request_exceeds_rate_limit_without_retry_after(tmp_path):
    async with gateway(tmp_path) as gw:
        resp = await gw.post({**BODY, "max_tokens": 50_001})
        rows = gw.rows()
    assert_error(
        resp,
        400,
        "request_exceeds_rate_limit",
        "invalid_request_error",
        "The request is larger than the total token budget allowed for this API key.",
    )
    assert gw.up.calls == [] and rows == []


async def test_429_gateway_rate_limited_after_prefilled_window(tmp_path):
    clock = FakeClock()
    async with gateway(tmp_path, clock=clock) as gw:
        gw.core.reserve(tenant_id(PEPPER, KEY), "prefill", 50_000 - EST + 1)
        clock.ms += 15_000
        resp = await gw.post()
        other = await gw.post(key=OTHER_KEY)  # a different tenant is unaffected
        rows = gw.rows()
    assert_error(
        resp,
        429,
        "gateway_rate_limited",
        "rate_limit_error",
        "Rate limit exceeded for this API key. Retry after the time in the Retry-After header.",
        retry_after="45",
    )
    assert other.status_code == 200
    assert gw.up.calls == ["primary"]  # only the other tenant's request went upstream
    assert [r[2] for r in rows] == ["prefill", other.headers["x-request-id"]]


async def test_429_upstream_rate_limited_when_both_429_and_refunds(tmp_path):
    async with gateway(tmp_path, status(429), status(429)) as gw:
        resp = await gw.post()
        rows = gw.rows()
    rid = assert_error(
        resp,
        429,
        "upstream_rate_limited",
        "rate_limit_error",
        "All upstream providers are rate limited. Please retry shortly.",
        retry_after="1",
    )
    assert gw.up.calls == ["primary", "secondary"]
    assert rows == [(tenant_id(PEPPER, KEY), 0, rid)]


@pytest.mark.parametrize(
    "primary", [status(429), stall()], ids=["primary-429-secondary-timeout", "both-timeout"]
)
async def test_504_upstream_timeout_and_refunds(tmp_path, primary):
    async with gateway(tmp_path, primary, stall()) as gw:
        resp = await gw.post()
        rows = gw.rows()
    rid = assert_error(
        resp,
        504,
        "upstream_timeout",
        "api_error",
        "The upstream provider did not respond in time.",
    )
    assert gw.up.calls == ["primary", "secondary"]
    assert rows == [(tenant_id(PEPPER, KEY), 0, rid)]


async def _connect_error(request):
    raise httpx2.ConnectError("connect to db-01.internal.corp.local refused", request=request)


async def _malformed(request):
    return httpx2.Response(200, content=b"{oops")


@pytest.mark.parametrize(
    ("primary", "secondary", "calls"),
    [
        (status(500), ok("secondary"), ["primary"]),
        (status(400), ok("secondary"), ["primary"]),
        (_connect_error, ok("secondary"), ["primary"]),
        (_malformed, ok("secondary"), ["primary"]),
        (status(429), status(500), ["primary", "secondary"]),
        (status(429), _connect_error, ["primary", "secondary"]),
    ],
    ids=[
        "primary-500",
        "primary-400",
        "connect-error",
        "malformed-json",
        "secondary-500",
        "secondary-connect",
    ],
)
async def test_502_upstream_error_and_refunds(tmp_path, primary, secondary, calls):
    async with gateway(tmp_path, primary, secondary) as gw:
        resp = await gw.post()
        rows = gw.rows()
    rid = assert_error(
        resp, 502, "upstream_error", "api_error", "The upstream provider returned an error."
    )
    assert gw.up.calls == calls
    assert rows == [(tenant_id(PEPPER, KEY), 0, rid)]


class BrokenCore:
    """A limiter core whose storage fails with a path-bearing OperationalError."""

    def __init__(self, fail_on: str = "reserve", real: SqliteTokenLimiter | None = None) -> None:
        self.fail_on = fail_on
        self.real = real
        self.calls: list[str] = []

    def _maybe_fail(self, name: str) -> None:
        self.calls.append(name)
        if name == self.fail_on:
            raise sqlite3.OperationalError("unable to open database file /srv/secret/router.db")

    def reserve(self, *args):
        self._maybe_fail("reserve")
        return self.real.reserve(*args)

    def reconcile(self, *args):
        self._maybe_fail("reconcile")
        return self.real.reconcile(*args)

    def refund(self, *args):
        self._maybe_fail("refund")
        return self.real.refund(*args)

    def close(self):
        if self.real is not None:
            self.real.close()


async def test_503_rate_limiter_unavailable_fails_closed(tmp_path):
    core = BrokenCore("reserve")
    async with gateway(tmp_path, core=core) as gw:
        resp = await gw.post()
    assert_error(
        resp,
        503,
        "rate_limiter_unavailable",
        "api_error",
        "The rate limiter is temporarily unavailable. Please retry shortly.",
    )
    assert gw.up.calls == []
    assert core.calls == ["reserve", "refund"]


async def test_reconcile_failure_still_returns_the_answer(tmp_path, caplog):
    db = str(tmp_path / "router.db")
    real = SqliteTokenLimiter(db, 50_000, 60, clock=FakeClock())
    core = BrokenCore("reconcile", real)
    async with gateway(tmp_path, core=core) as gw:
        with caplog.at_level(logging.INFO, logger="llm_router"):
            resp = await gw.post()
        rows = gw.rows()
    assert resp.status_code == 200
    assert core.calls == ["reserve", "reconcile"]
    assert [r[1] for r in rows] == [EST]  # kept the estimate: never under-charges
    assert len(info_records(caplog)) == 1
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings == ["rate limiter storage error", "could not reconcile reservation"]


async def test_500_internal_error_on_unexpected_exception_and_refunds(tmp_path, monkeypatch):
    async def boom(client, settings, body):
        raise RuntimeError("secret detail")

    monkeypatch.setattr("llm_router.app.route", boom)
    async with gateway(tmp_path) as gw:
        resp = await gw.post()
        rows = gw.rows()
    rid = assert_error(
        resp, 500, "internal_error", "server_error", "The gateway encountered an internal error."
    )
    assert rows == [(tenant_id(PEPPER, KEY), 0, rid)]


@pytest.mark.parametrize("path", ["/v1/other", "/v1/chat/completions/", "/", "/v1/models"])
async def test_404_not_found_json(tmp_path, path):
    async with gateway(tmp_path) as gw:
        resp = await gw.client.post(path, content=b"{}")
    assert_error(resp, 404, "not_found", "invalid_request_error", "Not found.")
    assert gw.up.calls == []


@pytest.mark.parametrize("method", ["GET", "PUT", "DELETE", "PATCH", "HEAD"])
async def test_405_method_not_allowed_json(tmp_path, method):
    async with gateway(tmp_path) as gw:
        resp = await gw.client.request(method, "/v1/chat/completions")
    assert resp.status_code == 405
    assert resp.headers["allow"] == "POST"
    rid = resp.headers["x-request-id"]
    assert RID_RE.match(rid)
    if method != "HEAD":
        assert_error(
            resp, 405, "method_not_allowed", "invalid_request_error", "Method not allowed."
        )
    assert gw.up.calls == []


async def test_413_declared_oversized_body(tmp_path):
    async with gateway(tmp_path) as gw:
        resp = await gw.post(b"x" * (MAX_BODY_BYTES + 1))
        rows = gw.rows()
    assert_error(
        resp, 413, "payload_too_large", "invalid_request_error", "The request body is too large."
    )
    assert gw.up.calls == [] and rows == []


async def test_413_chunked_oversized_body(tmp_path):
    async def chunks():
        for _ in range(5):
            yield b"x" * (256 * 1024)

    async with gateway(tmp_path) as gw:
        resp = await gw.post(content=chunks())
    assert_error(
        resp, 413, "payload_too_large", "invalid_request_error", "The request body is too large."
    )
    assert gw.up.calls == []


async def test_body_at_exactly_the_limit_is_accepted(tmp_path):
    base = json.dumps({**BODY, "pad": ""}).encode()
    padded = json.dumps({**BODY, "pad": "x" * (MAX_BODY_BYTES - len(base))}).encode()
    assert len(padded) == MAX_BODY_BYTES
    async with gateway(tmp_path) as gw:
        resp = await gw.post(padded)
    assert resp.status_code == 200


# --- cancellation ------------------------------------------------------------------------


async def test_client_disconnect_mid_route_refunds_the_reservation(tmp_path, caplog):
    flags = {"entered": anyio.Event()}
    async with gateway(
        tmp_path, stall(flags), PRIMARY_TIMEOUT_MS="5000", SECONDARY_TIMEOUT_MS="5000"
    ) as gw:
        with caplog.at_level(logging.INFO, logger="llm_router"):
            with anyio.CancelScope() as scope:
                async with anyio.create_task_group() as tg:
                    tg.start_soon(gw.post)
                    await flags["entered"].wait()
                    assert [r[1] for r in gw.rows()] == [EST]  # reserved while in flight
                    scope.cancel()
        rows = gw.rows()
    assert scope.cancelled_caught
    assert gw.up.calls == ["primary"]  # cancellation never triggers failover
    assert [r[1] for r in rows] == [0]
    [info] = info_records(caplog)
    assert "outcome=client_disconnected" in info.getMessage()


# --- Review Focus 4: no planted secret ever reaches a client -----------------------------

CANARIES = (
    "Traceback",
    "sk-LEAK",
    "internal.corp.local",
    "/srv/secret",
    "secret detail",
    "RuntimeError",
    "OperationalError",
    "ConnectError",
    "primary.internal",
    "secondary.internal",
    PRIMARY_KEY,
    SECONDARY_KEY,
)

LEAKY_BODY = {
    "error": {
        "message": 'Traceback (most recent call last):\n  File "/srv/secret/app.py", line 9',
        "key": "sk-LEAK-0123456789",
        "host": "llm-07.internal.corp.local",
    }
}


async def _leaky(request):
    return httpx2.Response(500, json=LEAKY_BODY)


async def _leaky_429(request):
    return httpx2.Response(429, json=LEAKY_BODY)


async def _leaky_secondary_timeout(request):
    await anyio.sleep(10)
    raise AssertionError("unreachable")


@pytest.mark.parametrize(
    "scenario",
    [
        "upstream-500-body",
        "both-429-bodies",
        "429-then-timeout",
        "connect-error",
        "sqlite-path",
        "runtime-error",
        "malformed",
    ],
)
async def test_no_canary_in_any_response(tmp_path, monkeypatch, scenario):
    kwargs: dict = {}
    if scenario == "upstream-500-body":
        kwargs = {"primary": _leaky}
    elif scenario == "both-429-bodies":
        kwargs = {"primary": _leaky_429, "secondary": _leaky_429}
    elif scenario == "429-then-timeout":
        kwargs = {"primary": _leaky_429, "secondary": _leaky_secondary_timeout}
    elif scenario == "connect-error":
        kwargs = {"primary": _connect_error}
    elif scenario == "sqlite-path":
        kwargs = {"core": BrokenCore("reserve")}
    elif scenario == "runtime-error":

        async def boom(client, settings, body):
            raise RuntimeError("secret detail at /srv/secret via sk-LEAK")

        monkeypatch.setattr("llm_router.app.route", boom)
    elif scenario == "malformed":

        async def primary(request):
            return httpx2.Response(200, content=b"Traceback sk-LEAK internal.corp.local {")

        kwargs = {"primary": primary}

    async with gateway(tmp_path, **kwargs) as gw:
        resp = await gw.post()
    assert resp.status_code >= 400
    body = resp.json()
    assert set(body) == {"error"}
    assert set(body["error"]) == {"message", "type", "param", "code", "request_id"}
    assert resp.headers["x-request-id"] == body["error"]["request_id"]
    surfaces = [resp.text, *resp.headers.values(), *resp.headers.keys()]
    for canary in CANARIES:
        for surface in surfaces:
            assert canary not in surface, (scenario, canary)


# --- logging -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("primary", "post_kwargs", "status_code", "outcome"),
    [
        (ok("primary"), {}, 200, "ok"),
        (status(429), {}, 200, "ok"),
        (status(500), {}, 502, "upstream_error"),
        (ok("primary"), {"key": "wrong-key"}, 401, "invalid_api_key"),
        (ok("primary"), {"body": {**BODY, "stream": True}}, 400, "invalid_request"),
    ],
    ids=["ok", "failover", "upstream-error", "auth", "stream"],
)
async def test_exactly_one_info_record_without_key_or_prompt(
    tmp_path, caplog, primary, post_kwargs, status_code, outcome
):
    async with gateway(tmp_path, primary) as gw:
        with caplog.at_level(logging.DEBUG, logger="llm_router"):
            resp = await gw.post(**post_kwargs)
    assert resp.status_code == status_code
    [record] = info_records(caplog)
    msg = record.getMessage()
    rid = resp.headers["x-request-id"]
    assert msg.startswith(f"request request_id={rid} ")
    assert f" status={status_code} " in msg
    assert f" outcome={outcome} " in msg
    assert re.search(r" duration_ms=\d+$", msg)
    for secret in (KEY, PROMPT, PRIMARY_KEY, SECONDARY_KEY, PEPPER, "wrong-key"):
        for record_ in caplog.records:
            assert secret not in record_.getMessage()


async def test_info_line_fields_on_failover(tmp_path, caplog):
    async with gateway(tmp_path, status(429), ok("secondary", tokens=33)) as gw:
        with caplog.at_level(logging.INFO, logger="llm_router"):
            resp = await gw.post()
    [record] = info_records(caplog)
    tenant = tenant_id(PEPPER, KEY)[:8]
    assert record.getMessage() == (
        f"request request_id={resp.headers['x-request-id']} tenant={tenant} status=200 "
        f"outcome=ok provider=secondary fallback_reason=rate_limited est_tokens={EST} "
        f"actual_tokens=33 duration_ms={record.getMessage().rsplit('=', 1)[1]}"
    )


async def test_front_door_and_internal_error_log_one_info_record(tmp_path, caplog, monkeypatch):
    async def boom(client, settings, body):
        raise RuntimeError("secret detail")

    monkeypatch.setattr("llm_router.app.route", boom)
    async with gateway(tmp_path) as gw:
        with caplog.at_level(logging.INFO, logger="llm_router"):
            r404 = await gw.client.get("/nope")
            r405 = await gw.client.get("/v1/chat/completions")
            r500 = await gw.post()
    msgs = [r.getMessage() for r in info_records(caplog)]
    assert len(msgs) == 3
    for resp, msg, outcome in zip(
        (r404, r405, r500), msgs, ("not_found", "method_not_allowed", "internal_error"), strict=True
    ):
        assert msg.startswith(f"request request_id={resp.headers['x-request-id']} ")
        assert f" outcome={outcome} " in msg
    [warning] = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warning.request_id == r500.headers["x-request-id"]
    assert warning.exc_info is not None
    assert "secret detail" not in warning.getMessage()


# --- lifespan and entry point ------------------------------------------------------------


async def test_lifespan_creates_and_closes_client_and_limiter(tmp_path):
    settings = make_settings(str(tmp_path / "router.db"))
    app = create_app(settings)
    assert app.exception_handlers[Exception] is unhandled_exception_handler
    assert app.router.redirect_slashes is False
    async with app.router.lifespan_context(app):
        client = app.state.upstream
        limiter = app.state.limiter
        assert isinstance(client, httpx2.AsyncClient)
        assert dict(client.headers) == {}
        assert client.follow_redirects is False
        assert isinstance(limiter, AsyncLimiter)
        res = await limiter.reserve("t", "rid-1", 10)
        assert res.tokens == 10
    assert client.is_closed
    with pytest.raises(sqlite3.ProgrammingError):
        limiter._limiter.reserve("t", "rid-2", 10)


async def test_lifespan_does_not_close_injected_dependencies(tmp_path):
    db = str(tmp_path / "router.db")
    settings = make_settings(db)
    core = SqliteTokenLimiter(db, 100, 60)
    injected = AsyncLimiter(core)
    async with httpx2.AsyncClient() as upstream:
        app = create_app(settings, upstream_client=upstream, limiter=injected)
        async with app.router.lifespan_context(app):
            assert app.state.upstream is upstream
            assert app.state.limiter is injected
        assert not upstream.is_closed
        assert core.reserve("t", "still-open", 1).tokens == 1
    core.close()


def test_main_configures_logging_and_runs_uvicorn(monkeypatch, tmp_path):
    seen: dict = {}

    def fake_run(app, **kwargs):
        seen["app"] = app
        seen.update(kwargs)

    env = {
        "ROUTER_TENANT_KEYS": f"{KEY}=alpha",
        "ROUTER_KEY_PEPPER": PEPPER,
        "ROUTER_DB_PATH": str(tmp_path / "r.db"),
        "PRIMARY_BASE_URL": "http://127.0.0.1:1/v1",
        "PRIMARY_API_KEY": PRIMARY_KEY,
        "PRIMARY_MODEL": "p",
        "SECONDARY_BASE_URL": "http://127.0.0.1:2/v1",
        "SECONDARY_API_KEY": SECONDARY_KEY,
        "SECONDARY_MODEL": "s",
        "ROUTER_PORT": "8123",
        "LOG_LEVEL": "debug",
    }
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(router_main.uvicorn, "run", fake_run)
    root = logging.getLogger()
    saved = (root.handlers[:], root.level)
    noisy = {n: logging.getLogger(n).level for n in ("httpx2", "httpcore2")}
    root.handlers[:] = []  # basicConfig(force=True) would otherwise close pytest's handlers
    try:
        router_main.main()
        handlers = root.handlers[:]
        assert len(handlers) == 1
        assert handlers[0].stream is sys.stderr
        assert any(isinstance(f, RequestIdFilter) for f in handlers[0].filters)
        assert root.level == logging.DEBUG
        assert logging.getLogger("httpx2").level == logging.WARNING
        assert logging.getLogger("httpcore2").level == logging.WARNING
    finally:
        root.handlers[:] = saved[0]
        root.setLevel(saved[1])
        for n, level in noisy.items():
            logging.getLogger(n).setLevel(level)
    assert seen["host"] == "127.0.0.1"
    assert seen["port"] == 8123
    assert seen["server_header"] is False
    assert seen["log_level"] == "debug"
    assert seen["app"].state.settings.port == 8123


def test_request_id_filter_defaults_for_records_outside_a_request():
    record = logging.LogRecord("uvicorn", logging.INFO, __file__, 1, "x", None, None)
    assert RequestIdFilter().filter(record) is True
    assert record.request_id == "-"
