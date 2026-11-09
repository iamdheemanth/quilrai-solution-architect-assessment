import json
import logging

import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from llm_router.errors import (
    GatewayError,
    GatewayRateLimited,
    InternalError,
    InvalidApiKey,
    InvalidRequest,
    RateLimiterUnavailable,
    RequestExceedsRateLimit,
    UpstreamError,
    UpstreamRateLimited,
    UpstreamTimeout,
    error_response,
    get_request_id,
    unhandled_exception_handler,
)

RID = "0123456789abcdef0123456789abcdef"

CASES = [
    (
        InvalidApiKey(),
        401,
        "authentication_error",
        "invalid_api_key",
        "Missing or invalid API key.",
        None,
    ),
    (
        InvalidRequest(message="Streaming is not supported by this gateway"),
        400,
        "invalid_request_error",
        "invalid_request",
        "Streaming is not supported by this gateway",
        None,
    ),
    (
        RequestExceedsRateLimit(),
        400,
        "invalid_request_error",
        "request_exceeds_rate_limit",
        "The request is larger than the total token budget allowed for this API key.",
        None,
    ),
    (
        GatewayRateLimited(retry_after=7),
        429,
        "rate_limit_error",
        "gateway_rate_limited",
        "Rate limit exceeded for this API key. Retry after the time in the Retry-After header.",
        "7",
    ),
    (
        UpstreamRateLimited(),
        429,
        "rate_limit_error",
        "upstream_rate_limited",
        "All upstream providers are rate limited. Please retry shortly.",
        "1",
    ),
    (
        UpstreamTimeout(),
        504,
        "api_error",
        "upstream_timeout",
        "The upstream provider did not respond in time.",
        None,
    ),
    (
        UpstreamError(),
        502,
        "api_error",
        "upstream_error",
        "The upstream provider returned an error.",
        None,
    ),
    (
        RateLimiterUnavailable(),
        503,
        "api_error",
        "rate_limiter_unavailable",
        "The rate limiter is temporarily unavailable. Please retry shortly.",
        None,
    ),
    (
        InternalError(),
        500,
        "server_error",
        "internal_error",
        "The gateway encountered an internal error.",
        None,
    ),
]


@pytest.mark.parametrize(("err", "status", "etype", "code", "message", "retry"), CASES)
def test_error_response_exact_shape(err, status, etype, code, message, retry):
    resp = error_response(err, RID)
    assert resp.status_code == status
    assert json.loads(resp.body) == {
        "error": {
            "message": message,
            "type": etype,
            "param": None,
            "code": code,
            "request_id": RID,
        }
    }
    assert resp.headers["x-request-id"] == RID
    assert resp.headers.get("retry-after") == retry
    assert resp.media_type == "application/json"


def test_all_errors_are_gateway_errors():
    for case in CASES:
        assert isinstance(case[0], GatewayError)


def test_extra_headers_merged_and_never_override_request_id():
    resp = error_response(InternalError(), RID, headers={"x-extra": "1", "x-request-id": "evil"})
    assert resp.headers["x-extra"] == "1"
    assert resp.headers["x-request-id"] == RID


def test_param_included_when_given():
    resp = error_response(InvalidRequest(message="Bad field", param="messages"), RID)
    assert json.loads(resp.body)["error"]["param"] == "messages"


def test_invalid_request_requires_fixed_message():
    with pytest.raises(TypeError):
        InvalidRequest()  # type: ignore[call-arg]


def test_gateway_rate_limited_retry_after_is_at_least_one():
    assert error_response(GatewayRateLimited(retry_after=0), RID).headers["retry-after"] == "1"


def test_get_request_id_creates_and_reuses():
    scope = {"type": "http", "method": "GET", "path": "/", "headers": [], "state": {}}
    request = Request(scope)
    first = get_request_id(request)
    assert len(first) == 32
    assert get_request_id(request) == first
    assert request.state.request_id == first


CANARY = "CANARY-secret /srv/x"


def _app() -> Starlette:
    async def boom(request: Request):
        raise RuntimeError(CANARY)

    async def gateway(request: Request):
        raise GatewayRateLimited(retry_after=3)

    async def ok(request: Request):
        return PlainTextResponse("ok")

    return Starlette(
        routes=[Route("/boom", boom), Route("/gw", gateway), Route("/ok", ok)],
        exception_handlers={Exception: unhandled_exception_handler},
    )


def test_unhandled_exception_returns_sanitized_500(caplog):
    client = TestClient(_app(), raise_server_exceptions=False)
    with caplog.at_level(logging.WARNING, logger="llm_router"):
        resp = client.get("/boom")
    assert resp.status_code == 500
    body = resp.json()
    rid = resp.headers["x-request-id"]
    assert body == {
        "error": {
            "message": "The gateway encountered an internal error.",
            "type": "server_error",
            "param": None,
            "code": "internal_error",
            "request_id": rid,
        }
    }
    blob = resp.text + str(dict(resp.headers))
    for forbidden in ("CANARY", "/srv/x", "RuntimeError", "Traceback"):
        assert forbidden not in blob
    # Full detail goes to the server log only, tied to the request id.
    records = [r for r in caplog.records if r.name == "llm_router"]
    assert records
    rec = records[0]
    assert rec.levelno >= logging.WARNING
    assert rec.exc_info is not None
    assert rid in rec.getMessage()
    assert CANARY in "".join(logging.Formatter().formatException(rec.exc_info))


def test_request_id_stable_between_handler_and_state():
    seen = {}

    async def boom(request: Request):
        seen["rid"] = get_request_id(request)
        raise RuntimeError(CANARY)

    app = Starlette(
        routes=[Route("/boom", boom)],
        exception_handlers={Exception: unhandled_exception_handler},
    )
    resp = TestClient(app, raise_server_exceptions=False).get("/boom")
    assert resp.headers["x-request-id"] == seen["rid"]
    assert resp.json()["error"]["request_id"] == seen["rid"]
