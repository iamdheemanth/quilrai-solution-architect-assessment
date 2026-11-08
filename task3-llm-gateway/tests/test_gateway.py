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
