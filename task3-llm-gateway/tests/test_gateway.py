import json
import logging
from contextlib import asynccontextmanager

import httpx2
import pytest

from llm_gateway.app import create_app
from llm_gateway.config import Settings
from llm_gateway.patterns import batch_redact
from mock_llm.server import DEFAULT_TOKENS, build_mock_app
from tests.helpers import StreamBody, chunk, parse_out, sse

pytestmark = pytest.mark.anyio

KEY = "sk-upstream-secret-key"
BODY = json.dumps({"model": "m", "messages": [{"role": "user", "content": "hi"}]}).encode()
SSE_CT = {"content-type": "text/event-stream; charset=utf-8"}


@pytest.fixture
def anyio_backend():
    return "asyncio"


@asynccontextmanager
async def gateway(upstream=None, *, mock=None, **overrides):
    """Yield ``(client, calls)``; upstream is a MockTransport handler or a mock-LLM app."""
    calls: list[httpx2.Request] = []
    if mock is not None:
        transport = httpx2.ASGITransport(app=mock)
    else:

        async def record(request: httpx2.Request) -> httpx2.Response:
            await request.aread()
            calls.append(request)
            result = upstream(request)
            if hasattr(result, "__await__"):
                result = await result
            return result

        transport = httpx2.MockTransport(record)
    up = httpx2.AsyncClient(transport=transport, base_url="http://upstream.test")
    up.headers.clear()
    settings = Settings(
        **{"upstream_base_url": "http://upstream.test/v1", "upstream_api_key": KEY, **overrides}
    )
    app = create_app(settings, upstream_client=up)
    client = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app), base_url="http://gw")
    try:
        yield client, calls
    finally:
        await client.aclose()
        await up.aclose()


def sse_response(parts, error=None, status=200, headers=SSE_CT):
    return httpx2.Response(status, headers=headers, stream=StreamBody(parts, error))


def json_response(obj, status=200):
    raw = json.dumps(obj).encode()
    return httpx2.Response(
        status, headers={"content-type": "application/json"}, stream=StreamBody([raw])
    )


def stream_text(raw: bytes) -> str:
    out = ""
    for obj in parse_out(raw):
        if obj != "[DONE]" and obj.get("choices"):
            out += obj["choices"][0]["delta"].get("content") or ""
    return out


async def post(client, body=BODY, **kw):
    return await client.post("/v1/chat/completions", content=body, **kw)


# ---- streaming ----


async def test_stream_content_equals_batch_redact_with_sse_headers():
    async with gateway(mock=build_mock_app()) as (c, _):
        r = await post(c, json.dumps({"stream": True, "messages": []}).encode())
    assert r.status_code == 200
    assert stream_text(r.content) == batch_redact("".join(DEFAULT_TOKENS))
    assert "[REDACTED]" in r.text
    assert r.text.endswith("data: [DONE]\n\n")
    assert r.headers["content-type"].startswith("text/event-stream")
    assert r.headers["cache-control"] == "no-cache"
    assert r.headers["x-accel-buffering"] == "no"
    assert "content-length" not in r.headers


async def test_non_streaming_redacted_and_logprobs_null():
    obj = {
        "id": "x",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": "mail a@b.com now"},
                "logprobs": {"content": [{"token": "a@b.com"}]},
                "finish_reason": "stop",
            }
        ],
    }
    async with gateway(lambda req: json_response(obj)) as (c, _):
        r = await post(c)
    assert r.status_code == 200
    got = r.json()["choices"][0]
    assert got["message"]["content"] == "mail [REDACTED] now"
    assert got["logprobs"] is None
    assert "a@b.com" not in r.text


# ---- upstream request ----


async def test_upstream_request_headers_url_and_body():
    async with gateway(lambda req: json_response({"choices": []})) as (c, calls):
        r = await post(
            c,
            headers={
                "authorization": "Bearer client-token",
                "cookie": "sid=1",
                "content-type": "application/json",
                "x-custom": "kept",
                "accept-encoding": "gzip",
            },
        )
    assert r.status_code == 200
    req = calls[0]
    assert str(req.url) == "http://upstream.test/v1/chat/completions"
    assert req.headers["authorization"] == f"Bearer {KEY}"
    assert "cookie" not in req.headers
    assert req.headers["host"] == "upstream.test"
    assert req.headers["accept-encoding"] == "identity"
    assert req.headers["x-custom"] == "kept"
    assert req.content == BODY


async def test_empty_key_sends_no_authorization():
    async with gateway(lambda req: json_response({"choices": []}), upstream_api_key="") as (
        c,
        calls,
    ):
        await post(c, headers={"authorization": "Bearer client-token"})
    assert "authorization" not in calls[0].headers


# ---- errors ----


def _raise(exc):
    def handler(req):
        raise exc

    return handler


async def test_connect_error_is_502_openai_shape():
    async with gateway(_raise(httpx2.ConnectError("boom"))) as (c, _):
        r = await post(c)
    assert r.status_code == 502
    err = r.json()["error"]
    assert set(err) == {"message", "type", "code"}


async def test_read_timeout_before_headers_is_504():
    async with gateway(_raise(httpx2.ReadTimeout("slow"))) as (c, _):
        r = await post(c)
    assert r.status_code == 504
    assert set(r.json()["error"]) == {"message", "type", "code"}


async def test_upstream_400_json_relayed_and_redacted():
    err = {"error": {"message": "bad input joe@example.com", "type": "invalid", "code": "x"}}
    async with gateway(lambda req: json_response(err, 400)) as (c, _):
        r = await post(c)
    assert r.status_code == 400
    assert r.json()["error"]["message"] == "bad input [REDACTED]"


async def test_non_json_upstream_is_502_and_closed():
    stream = StreamBody([b"<html>"])
    resp = httpx2.Response(200, headers={"content-type": "text/html"}, stream=stream)
    async with gateway(lambda req: resp) as (c, _):
        r = await post(c)
    assert r.status_code == 502
    assert stream.closed


async def test_json_over_cap_is_502():
    big = json_response({"choices": [], "pad": "x" * 5000})
    async with gateway(lambda req: big, max_body_bytes=1000) as (c, _):
        r = await post(c, b"{}")
    assert r.status_code == 502


# ---- reply path ----


async def test_sse_reply_to_stream_false_request_is_redacted_stream():
    frames = [
        sse(chunk("mail a@b.co", role="assistant")),
        sse(chunk("m now")),
        sse(chunk(finish="stop")),
        sse("[DONE]"),
    ]
    body = json.dumps({"stream": False}).encode()
    async with gateway(lambda req: sse_response(frames)) as (c, _):
        r = await post(c, body)
    assert r.headers["content-type"].startswith("text/event-stream")
    assert stream_text(r.content) == "mail [REDACTED] now"
    assert "a@b.co" not in r.text


# ---- front door ----


async def test_other_path_404_json():
    async with gateway(lambda req: json_response({})) as (c, calls):
        r = await c.post("/v1/other", content=b"{}")
    assert r.status_code == 404
    assert set(r.json()["error"]) == {"message", "type", "code"}
    assert calls == []


@pytest.mark.parametrize("method", ["GET", "HEAD", "PUT", "DELETE"])
async def test_other_methods_405(method):
    async with gateway(lambda req: json_response({})) as (c, calls):
        r = await c.request(method, "/v1/chat/completions")
    assert r.status_code == 405
    assert calls == []
    if method != "HEAD":
        assert set(r.json()["error"]) == {"message", "type", "code"}


async def test_oversized_body_413():
    async with gateway(lambda req: json_response({})) as (c, calls):
        r = await post(c, b"x" * (5 * 1024 * 1024))
    assert r.status_code == 413
    assert calls == []


# ---- mid-stream failures ----


async def test_transport_error_mid_stream_gives_error_frame_without_leak():
    parts = [sse(chunk("mail jo", role="assistant")), sse(chunk("hn@exa"))]
    handler = lambda req: sse_response(parts, error=httpx2.ReadError("drop"))  # noqa: E731
    async with gateway(handler) as (c, _):
        r = await post(c)
    assert r.status_code == 200
    assert b"upstream_error" in r.content
    assert b"[DONE]" not in r.content
    assert b"hn@exa" not in r.content
    assert b"john" not in r.content


async def test_non_json_data_frame_gives_error_frame():
    parts = [sse(chunk("hi", role="assistant")), b"data: not json\n\n"]
    async with gateway(lambda req: sse_response(parts)) as (c, _):
        r = await post(c)
    assert r.content.endswith(b'"code":"upstream_error"}}\n\n')
    assert b"[DONE]" not in r.content


async def test_eof_without_done_before_finish_is_error_frame():
    parts = [sse(chunk("mail a@b", role="assistant"))]
    async with gateway(lambda req: sse_response(parts)) as (c, _):
        r = await post(c)
    assert b"upstream_error" in r.content
    assert b"a@b" not in r.content


async def test_upstream_closed_after_stream():
    stream = StreamBody([sse(chunk("ok", role="assistant", finish="stop")), sse("[DONE]")])
    resp = httpx2.Response(200, headers=SSE_CT, stream=stream)
    async with gateway(lambda req: resp) as (c, _):
        r = await post(c)
    assert r.status_code == 200
    assert stream.closed


# ---- logging ----


def _records(caplog):
    return [r for r in caplog.records if r.name == "llm_gateway"]


async def test_one_log_record_per_stream_request_no_content(caplog):
    with caplog.at_level(logging.DEBUG, logger="llm_gateway"):
        async with gateway(mock=build_mock_app()) as (c, _):
            await post(c, json.dumps({"stream": True}).encode(), headers={"x-h": "hdr-secret"})
    recs = _records(caplog)
    assert len(recs) == 1
    assert recs[0].levelno == logging.INFO
    msg = recs[0].getMessage()
    for needle in ("method=POST", "outcome=ok", "upstream_status=200", "duration_ms=", "max_held="):
        assert needle in msg
    assert '"EMAIL": 2' in msg and '"SSN": 1' in msg and '"CC": 1' in msg
    for secret in (KEY, "example.com", "example.org", "123-45-6789", "4111", "hdr-secret"):
        assert secret not in msg


async def test_one_log_record_for_json_error_and_front_door(caplog):
    err = {"error": {"message": "x joe@example.com", "type": "t", "code": "c"}}
    with caplog.at_level(logging.DEBUG, logger="llm_gateway"):
        async with gateway(lambda req: json_response(err, 400)) as (c, _):
            await post(c)
            await c.get("/nope")
            await c.get("/v1/chat/completions")
    msgs = [r.getMessage() for r in _records(caplog)]
    assert len(msgs) == 3
    assert "outcome=upstream_error" in msgs[0] and "upstream_status=400" in msgs[0]
    assert "outcome=not_found" in msgs[1]
    assert "outcome=method_not_allowed" in msgs[2]
    assert all("joe@example.com" not in m and KEY not in m for m in msgs)


async def test_mid_stream_failure_logs_class_name_only_and_one_info(caplog):
    parts = [sse(chunk("mail jo", role="assistant"))]
    handler = lambda req: sse_response(parts, error=httpx2.ReadError("secret-detail"))  # noqa: E731
    with caplog.at_level(logging.DEBUG, logger="llm_gateway"):
        async with gateway(handler) as (c, _):
            await post(c)
    recs = _records(caplog)
    infos = [r for r in recs if r.levelno == logging.INFO]
    assert len(infos) == 1
    assert "outcome=stream_failed" in infos[0].getMessage()
    text = " ".join(r.getMessage() for r in recs)
    assert "ReadError" in text
    assert "secret-detail" not in text and "mail jo" not in text


async def test_timeout_and_bad_gateway_outcomes_logged(caplog):
    with caplog.at_level(logging.INFO, logger="llm_gateway"):
        async with gateway(_raise(httpx2.ReadTimeout("t"))) as (c, _):
            await post(c)
        async with gateway(_raise(httpx2.ConnectError("c"))) as (c, _):
            await post(c)
    msgs = [r.getMessage() for r in _records(caplog) if r.levelno == logging.INFO]
    assert len(msgs) == 2
    assert "outcome=timeout" in msgs[0]
    assert "outcome=bad_gateway" in msgs[1]


# ---- fix round 1: content-based JSON redaction ----


@pytest.mark.parametrize(
    ("status", "obj", "check"),
    [
        (500, {"error": "bad joe@example.com"}, lambda j: j["error"] == "bad [REDACTED]"),
        (
            422,
            {"detail": [{"input": "joe@example.com", "loc": ["body"]}]},
            lambda j: j["detail"][0]["input"] == "[REDACTED]" and j["detail"][0]["loc"] == ["body"],
        ),
        (
            200,
            {"error": {"message": "bad joe@example.com"}},
            lambda j: j["error"]["message"] == "bad [REDACTED]",
        ),
        (
            500,
            {"choices": [{"message": {"content": "joe@example.com"}}]},
            lambda j: j["choices"][0]["message"]["content"] == "[REDACTED]",
        ),
        (302, {"error": "joe@example.com"}, lambda j: j["error"] == "[REDACTED]"),
    ],
)
async def test_every_json_reply_redacted_by_content_status_preserved(status, obj, check):
    async with gateway(lambda req: json_response(obj, status)) as (c, _):
        r = await post(c)
    assert r.status_code == status
    assert check(r.json())
    assert "joe@example.com" not in r.text


async def test_normal_completion_non_pii_fields_unchanged():
    obj = {
        "id": "chatcmpl-1",
        "model": "m",
        "created": 5,
        "usage": {"prompt_tokens": 1, "total_tokens": 3},
        "choices": [{"index": 0, "message": {"role": "assistant", "content": "hi"}}],
    }
    async with gateway(lambda req: json_response(obj)) as (c, _):
        r = await post(c)
    assert r.json() == obj


@pytest.mark.parametrize(
    "raw",
    [
        b'{"choices": 5}',
        b'{"choices":[{"message":{"tool_calls":7}}]}',
        b"[" * 50000 + b"]" * 50000,
        b'{"a":' * 100 + b"1" + b"}" * 100,
        b"not json",
        b"[1]",
    ],
)
async def test_malformed_json_is_502_logged(raw, caplog):
    resp = httpx2.Response(
        200, headers={"content-type": "application/json"}, stream=StreamBody([raw])
    )
    with caplog.at_level(logging.DEBUG, logger="llm_gateway"):
        async with gateway(lambda req: resp) as (c, _):
            r = await post(c)
    assert r.status_code == 502
    assert r.json()["error"]["code"] == "bad_gateway"
    assert len(_records(caplog)) == 1
    assert "outcome=bad_gateway" in _records(caplog)[0].getMessage()


async def test_missing_content_type_is_502():
    resp = httpx2.Response(200, stream=StreamBody([b"{}"]))
    async with gateway(lambda req: resp) as (c, _):
        r = await post(c)
    assert r.status_code == 502


# ---- fix round 1: front door ----


async def test_trailing_slash_is_404_not_redirect():
    async with gateway(lambda req: json_response({})) as (c, calls):
        r = await c.post("/v1/chat/completions/", content=b"{}")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "not_found"
    assert calls == []


# ---- fix round 1: headers ----


async def test_client_credentials_and_named_connection_headers_stripped():
    async with gateway(lambda req: json_response({"choices": []})) as (c, calls):
        await post(
            c,
            headers={
                "x-api-key": "k1",
                "api-key": "k2",
                "proxy-authorization": "Basic zzz",
                "connection": "x-hop",
                "x-hop": "1",
                "x-keep": "1",
            },
        )
    h = calls[0].headers
    for name in ("x-api-key", "api-key", "proxy-authorization", "x-hop", "connection"):
        assert name not in h
    assert h["x-keep"] == "1"


# ---- fix round 1: exactly one log record of any level ----


def _scenarios():
    err_json = lambda req: json_response({"error": {"message": "m"}}, 400)  # noqa: E731
    mid = lambda req: sse_response(  # noqa: E731
        [sse(chunk("x", role="assistant"))], error=httpx2.ReadError("d")
    )
    bad = lambda req: httpx2.Response(  # noqa: E731
        200, headers={"content-type": "application/json"}, stream=StreamBody([b'{"choices":5}'])
    )
    ok_json = lambda req: json_response({"choices": []})  # noqa: E731

    async def stream(c):
        await post(c, b"{}")

    return [
        ("ok_stream", {"mock": build_mock_app()}, stream, "outcome=ok"),
        ("ok_json", {"upstream": ok_json}, stream, "outcome=ok"),
        ("upstream_error", {"upstream": err_json}, stream, "outcome=upstream_error"),
        ("404", {"upstream": ok_json}, lambda c: c.post("/nope"), "outcome=not_found"),
        (
            "405",
            {"upstream": ok_json},
            lambda c: c.get("/v1/chat/completions"),
            "outcome=method_not_allowed",
        ),
        (
            "slash",
            {"upstream": ok_json},
            lambda c: c.post("/v1/chat/completions/"),
            "outcome=not_found",
        ),
        (
            "413",
            {"upstream": ok_json},
            lambda c: post(c, b"x" * (5 * 1024 * 1024)),
            "outcome=payload_too_large",
        ),
        (
            "502",
            {"upstream": _raise(httpx2.ConnectError("c"))},
            stream,
            "outcome=bad_gateway error=ConnectError",
        ),
        (
            "504",
            {"upstream": _raise(httpx2.ReadTimeout("t"))},
            stream,
            "outcome=timeout error=ReadTimeout",
        ),
        ("mid_stream", {"upstream": mid}, stream, "outcome=stream_failed error=ReadError"),
        ("malformed", {"upstream": bad}, stream, "outcome=bad_gateway error=ValueError"),
    ]


@pytest.mark.parametrize("scenario", _scenarios(), ids=lambda s: s[0])
async def test_exactly_one_record_of_any_level_per_request(scenario, caplog):
    _, kwargs, action, expected = scenario
    up = kwargs.get("upstream")
    with caplog.at_level(logging.DEBUG):
        async with gateway(up, mock=kwargs.get("mock")) as (c, _):
            await action(c)
    recs = [r for r in caplog.records if r.name.startswith("llm_gateway")]
    assert len(recs) == 1
    msg = recs[0].getMessage()
    assert expected in msg
    assert "/nope" not in msg and "duration_ms=" in msg and "route=" in msg


async def test_front_door_duration_measured_and_path_not_logged(caplog):
    with caplog.at_level(logging.INFO, logger="llm_gateway"):
        async with gateway(lambda req: json_response({})) as (c, _):
            await c.post("/secret-path-123")
    msg = _records(caplog)[0].getMessage()
    assert "secret-path-123" not in msg
    assert "route=unknown" in msg
    assert "duration_ms=0.0" not in msg


async def test_upstream_3xx_json_relayed_redacted():
    async with gateway(lambda req: json_response({"error": "see joe@example.com"}, 307)) as (c, _):
        r = await post(c)
    assert r.status_code == 307
    assert r.json() == {"error": "see [REDACTED]"}


async def test_client_disconnect_outcome_and_upstream_closed():
    from llm_gateway.proxy import relay
    from llm_gateway.transform import StreamTransformer

    stream = StreamBody([sse(chunk("a", role="assistant")), sse(chunk("b"))])
    up = httpx2.Response(200, headers=SSE_CT, stream=stream)
    seen = []
    gen = relay(up, StreamTransformer(), lambda outcome, error: seen.append((outcome, error)))
    await gen.__anext__()
    await gen.aclose()
    assert seen == [("client_disconnected", None)]
    assert stream.closed


async def test_mid_stream_upstream_error_frame_logged_as_upstream_error(caplog):
    parts = [sse(chunk("hi ")), sse({"error": {"message": "boom", "type": "e"}})]
    with caplog.at_level(logging.INFO, logger="llm_gateway"):
        async with gateway(lambda req: sse_response(parts)) as (c, _):
            r = await post(c)
    assert b"[DONE]" not in r.content
    msgs = [x.getMessage() for x in _records(caplog) if x.levelno == logging.INFO]
    assert len(msgs) == 1 and "outcome=upstream_error" in msgs[0]
