import json
import logging
import time

import httpx2
import jwt
import pytest

from tests.helpers import TEST_SECRET, gateway, headers, rpc, token

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


ADMIN_CALL = rpc("tools/call", {"name": "admin_reset_key", "arguments": {}}, id=7)


def _call_body(name_fragment: str, id_fragment: str = "1", extra: str = "") -> bytes:
    head = '{"jsonrpc":"2.0","id":' + id_fragment + ',"method":"tools/call","params":{'
    return (head + name_fragment + "}" + extra + "}").encode()


async def test_viewer_admin_tool_denied_without_upstream_call():
    async with gateway() as (c, calls):
        r = await c.post("/mcp", content=ADMIN_CALL, headers=headers("viewer"))
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/json"
    assert r.json() == {
        "jsonrpc": "2.0",
        "id": 7,
        "error": {
            "code": -32001,
            "message": "Unauthorized Tool Call",
            "data": {
                "reason": "forbidden_tool",
                "tool": "admin_reset_key",
                "requiredRole": "admin",
                "role": "viewer",
            },
        },
    }
    assert "mcp-session-id" not in r.headers
    assert calls == []


async def test_string_id_echoed_with_type():
    body = rpc("tools/call", {"name": "admin_reset_key"}, id="7")
    async with gateway() as (c, calls):
        r = await c.post("/mcp", content=body, headers=headers("viewer"))
    assert r.json()["id"] == "7"
    assert r.json()["error"]["code"] == -32001
    assert calls == []


async def test_deny_status_403_opt_in():
    async with gateway(deny_http_status=403) as (c, calls):
        r = await c.post("/mcp", content=ADMIN_CALL, headers=headers("viewer"))
    assert r.status_code == 403
    assert r.json()["error"]["code"] == -32001
    assert r.json()["id"] == 7
    assert calls == []


async def test_admin_admin_tool_forwarded_byte_identical():
    async with gateway() as (c, calls):
        r = await c.post("/mcp", content=ADMIN_CALL, headers=headers("admin"))
    assert r.status_code == 200
    assert r.json() == {"jsonrpc": "2.0", "id": 7, "result": {}}
    assert len(calls) == 1
    assert calls[0].content == ADMIN_CALL
    assert calls[0].url.path == "/mcp"


async def test_viewer_tools_list_forwarded_transparently():
    body = rpc("tools/list", id=3)
    sentinel = b'{"jsonrpc":"2.0","id":3,"result":{"tools":[{"name":"admin_reset_key"}]}}'

    def handler(request):
        return httpx2.Response(200, headers={"content-type": "application/json"}, content=sentinel)

    async with gateway(handler) as (c, calls):
        r = await c.post("/mcp", content=body, headers=headers("viewer"))
    assert r.status_code == 200
    assert r.content == sentinel
    assert len(calls) == 1
    assert calls[0].content == body


BYPASS_BODIES = [
    pytest.param(
        _call_body('"name":"get_status","name":"admin_reset_key"'), 400, -32700, id="dup-name"
    ),
    pytest.param(
        _call_body('"name":"get_status"', extra=',"params":{"name":"admin_reset_key"}'),
        400,
        -32700,
        id="dup-params",
    ),
    pytest.param(
        b'{"jsonrpc":"2.0","id":1,"method":"tools/list","method":"tools/call",'
        b'"params":{"name":"admin_reset_key"}}',
        400,
        -32700,
        id="dup-method",
    ),
    pytest.param(
        b'{"jsonrpc":"2.0","id":1,"id":2,"method":"tools/call",'
        b'"params":{"name":"admin_reset_key"}}',
        400,
        -32700,
        id="dup-id",
    ),
    pytest.param(
        b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_status",'
        b'"n\\u0061me":"admin_reset_key"}}',
        400,
        -32700,
        id="dup-name-escaped",
    ),
    pytest.param(
        b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"\\u0061dmin_reset_key"}}',
        200,
        -32001,
        id="escaped-admin-name",
    ),
    pytest.param(ADMIN_CALL.decode().encode("utf-16"), 400, -32700, id="utf-16"),
    pytest.param(b"\xef\xbb\xbf" + ADMIN_CALL, 400, -32700, id="bom"),
    pytest.param(ADMIN_CALL.decode().encode("utf-16-le"), 400, -32700, id="utf-16-le-no-bom"),
    pytest.param(ADMIN_CALL.decode().encode("utf-16-be"), 400, -32700, id="utf-16-be-no-bom"),
    pytest.param(ADMIN_CALL.decode().encode("utf-32-le"), 400, -32700, id="utf-32-le-no-bom"),
    pytest.param(ADMIN_CALL.decode().encode("utf-32-be"), 400, -32700, id="utf-32-be-no-bom"),
    pytest.param(
        b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"\xed\xa0\x80"}}',
        400,
        -32700,
        id="cesu8-surrogate",
    ),
    pytest.param(_call_body('"name":"get_status","arguments":{"x":NaN}'), 400, -32700, id="nan"),
    pytest.param(b"[" + ADMIN_CALL + b"]", 400, -32600, id="batch"),
    pytest.param(b"[" + ADMIN_CALL + b"," + ADMIN_CALL + b"]", 400, -32600, id="batch-two"),
    pytest.param(
        b'{"jsonrpc":"2.0","id":1,"method":"tools/call","result":{},'
        b'"params":{"name":"admin_reset_key"}}',
        400,
        -32600,
        id="method-plus-result",
    ),
    pytest.param(_call_body('"name":"admin_reset_key"', "null"), 400, -32600, id="id-null"),
    pytest.param(_call_body('"name":"admin_reset_key"', "1.0"), 400, -32600, id="id-float"),
    pytest.param(_call_body('"name":"admin_reset_key"', "true"), 400, -32600, id="id-bool"),
    pytest.param(
        b'{"jsonrpc":"2.0","method":"tools/call","params":{"name":"admin_reset_key"}}',
        400,
        -32600,
        id="idless-tools-call",
    ),
    pytest.param(
        b'{"jsonrpc":"2.0","id":1,"method":"Tools/Call","params":{"name":"admin_reset_key"}}',
        200,
        -32601,
        id="method-case",
    ),
    pytest.param(
        b'{"jsonrpc":"2.0","id":1,"method":"admin/reset","params":{}}', 200, -32601, id="custom"
    ),
    pytest.param(_call_body('"name":123'), 400, -32602, id="name-not-string"),
    pytest.param(_call_body('"arguments":{}'), 400, -32602, id="name-missing"),
    pytest.param(
        b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":["admin_reset_key"]}',
        400,
        -32602,
        id="params-array",
    ),
]


@pytest.mark.parametrize(("body", "status", "code"), BYPASS_BODIES)
async def test_bypass_bodies_never_reach_downstream(body, status, code):
    async with gateway() as (c, calls):
        r = await c.post("/mcp", content=body, headers=headers("viewer"))
    assert r.status_code == status
    assert r.json()["error"]["code"] == code
    if code == -32700:
        assert r.json()["id"] is None
    assert r.headers["content-type"] == "application/json"
    assert calls == []


async def test_modern_era_unknown_method_is_404():
    body = b'{"jsonrpc":"2.0","id":1,"method":"admin/reset","params":{}}'
    hdr = headers("viewer", **{"mcp-protocol-version": "2026-07-28", "mcp-method": "admin/reset"})
    async with gateway() as (c, calls):
        r = await c.post("/mcp", content=body, headers=hdr)
    assert r.status_code == 404
    assert r.json()["error"]["code"] == -32601
    assert calls == []


def _expired():
    return token("admin", now=int(time.time()) - 7200)


def _alg_none():
    claims = {"sub": "x", "role": "admin", "aud": "mcp-gateway", "iss": "mcp-gateway-dev"}
    return jwt.encode(claims, None, algorithm="none")


def _bearer(value: str) -> list:
    return [("content-type", "application/json"), ("authorization", value)]


AUTH_CASES = {
    "no-header": ([("content-type", "application/json")], 401),
    "basic": (_bearer("Basic dXNlcjpwYXNz"), 401),
    "two-headers": (
        _bearer(f"Bearer {token('admin')}") + [("authorization", f"Bearer {token('admin')}")],
        400,
    ),
    "expired": (_bearer(f"Bearer {_expired()}"), 401),
    "alg-none": (_bearer(f"Bearer {_alg_none()}"), 401),
    "wrong-aud": (_bearer(f"Bearer {token('admin', audience='other')}"), 401),
}


@pytest.mark.parametrize("case", sorted(AUTH_CASES))
@pytest.mark.parametrize("body", [rpc("tools/list"), ADMIN_CALL], ids=["tools-list", "tools-call"])
async def test_auth_failures_never_reach_downstream(case, body):
    hdrs, status = AUTH_CASES[case]
    async with gateway() as (c, calls):
        r = await c.post("/mcp", content=body, headers=hdrs)
    assert r.status_code == status
    assert r.headers["www-authenticate"].startswith("Bearer realm=")
    assert r.headers["content-type"] == "application/json"
    err = r.json()
    assert err["id"] is None
    assert err["error"]["code"] == -32005
    assert calls == []


async def test_unparseable_body_without_token_is_401_not_400():
    async with gateway() as (c, calls):
        r = await c.post("/mcp", content=b"\xff not json", headers=headers(None))
    assert r.status_code == 401
    assert calls == []


async def test_mirrored_header_mismatch_rejected():
    hdr = headers("admin", **{"mcp-name": "get_status", "mcp-method": "tools/call"})
    async with gateway() as (c, calls):
        r = await c.post("/mcp", content=ADMIN_CALL, headers=hdr)
    assert r.status_code == 400
    assert r.json()["error"]["code"] == -32020
    assert calls == []


@pytest.mark.parametrize(
    "extra",
    [
        {"content-type": "text/plain"},
        {"content-type": "application/x-www-form-urlencoded"},
        {"content-type": "application/json; charset=utf-16"},
        {"content-encoding": "gzip"},
    ],
    ids=["text", "form", "charset", "gzip"],
)
async def test_wrong_content_type_415(extra):
    async with gateway() as (c, calls):
        r = await c.post("/mcp", content=rpc("tools/list"), headers=headers("admin", **extra))
    assert r.status_code == 415
    assert r.json() == {
        "jsonrpc": "2.0",
        "id": None,
        "error": {"code": -32600, "message": "Unsupported Media Type"},
    }
    assert calls == []


async def test_json_charset_utf8_accepted():
    hdr = headers("admin", **{"content-type": "Application/JSON; charset=UTF-8"})
    async with gateway() as (c, calls):
        r = await c.post("/mcp", content=rpc("tools/list"), headers=hdr)
    assert r.status_code == 200
    assert len(calls) == 1


async def test_oversized_body_413_and_large_legit_body_forwarded():
    big_junk = b"x" * (5 * 1024 * 1024)
    large = rpc("tools/call", {"name": "get_status", "arguments": {"a": "y" * (1024 * 1024)}})
    async with gateway() as (c, calls):
        too_big = await c.post("/mcp", content=big_junk, headers=headers("admin"))
        assert too_big.status_code == 413
        assert calls == []
        ok = await c.post("/mcp", content=large, headers=headers("admin"))
    assert ok.status_code == 200
    assert len(calls) == 1
    assert calls[0].content == large


@pytest.mark.parametrize("path", ["/mcp/", "/MCP", "/other", "//mcp"])
async def test_other_paths_404(path, caplog):
    caplog.set_level(logging.INFO, logger="mcp_gateway.audit")
    async with gateway() as (c, calls):
        r = await c.post(path, content=rpc("tools/list"), headers=headers("admin"))
    assert r.status_code == 404
    assert r.headers["content-type"] == "application/json"
    assert r.json() == {
        "jsonrpc": "2.0",
        "id": None,
        "error": {"code": -32600, "message": "Not Found"},
    }
    assert calls == []
    records = _audit_records(caplog)
    assert [(x["decision"], x["reason"]) for x in records] == [("reject", "unknown_path")]


@pytest.mark.parametrize("method", ["HEAD", "PUT", "PATCH", "OPTIONS", "BREW"])
async def test_other_methods_405_json_and_audited(method, caplog):
    caplog.set_level(logging.INFO, logger="mcp_gateway.audit")
    async with gateway() as (c, calls):
        r = await c.request(method, "/mcp", headers=headers("admin"))
    assert r.status_code == 405
    assert r.headers["allow"] == "POST, GET, DELETE"
    assert r.headers["content-type"] == "application/json"
    if method != "HEAD":
        assert r.json()["error"] == {"code": -32600, "message": "Method Not Allowed"}
    assert calls == []
    records = _audit_records(caplog)
    assert [(x["decision"], x["reason"]) for x in records] == [("reject", "method_not_allowed")]
    assert records[0]["upstream_contacted"] is False


async def test_unauthenticated_head_is_401():
    async with gateway() as (c, calls):
        r = await c.head("/mcp", headers=headers(None))
    assert r.status_code == 401
    assert calls == []


async def test_forwarded_headers_are_filtered():
    hdr = headers(
        "admin",
        **{
            "cookie": "sid=1",
            "origin": "http://evil.example",
            "connection": "x-foo, keep-alive",
            "x-foo": "bar",
            "te": "trailers",
            "x-request-id": "client-chosen",
            "x-gateway-role": "admin",
            "mcp-session-id": "sess-1",
            "mcp-protocol-version": "2025-06-18",
            "x-custom": "kept",
        },
    )
    async with gateway() as (c, calls):
        r = await c.post("/mcp?a=1", content=rpc("tools/list"), headers=hdr)
    assert r.status_code == 200
    sent = calls[0].headers
    for dropped in ("authorization", "cookie", "origin", "connection", "x-foo", "te"):
        assert dropped not in sent
    assert "x-gateway-role" not in sent
    assert sent["mcp-session-id"] == "sess-1"
    assert sent["mcp-protocol-version"] == "2025-06-18"
    assert sent["x-custom"] == "kept"
    assert sent["accept"] == "application/json, text/event-stream"
    assert sent["accept-encoding"] == "identity"
    assert sent["x-request-id"] not in ("client-chosen", "")
    assert len(sent["x-request-id"]) == 32
    assert sent["host"] == "127.0.0.1:8001"
    assert calls[0].url.path == "/mcp"
    assert calls[0].url.query == b"a=1"


async def test_response_headers_relayed():
    def handler(request):
        return httpx2.Response(
            200,
            headers=[
                ("content-type", "text/event-stream"),
                ("mcp-session-id", "down-sess"),
                ("connection", "close"),
                ("transfer-encoding", "chunked"),
                ("server", "uvicorn"),
                ("date", "Sat, 07 Nov 2026 00:00:00 GMT"),
                ("x-dup", "1"),
                ("x-dup", "2"),
            ],
            content=b"event: message\n\n",
        )

    async with gateway(handler) as (c, calls):
        r = await c.post("/mcp", content=rpc("tools/list"), headers=headers("viewer"))
    assert r.headers["content-type"] == "text/event-stream"
    assert r.headers["mcp-session-id"] == "down-sess"
    assert r.headers.get_list("x-dup") == ["1", "2"]
    for dropped in ("connection", "transfer-encoding", "server"):
        assert dropped not in r.headers
    assert r.content == b"event: message\n\n"
    assert len(calls) == 1


@pytest.mark.parametrize("status", [406, 500])
async def test_downstream_errors_relayed_verbatim(status):
    payload = b'{"error":"downstream says no"}'

    def handler(request):
        return httpx2.Response(
            status, headers={"content-type": "application/json"}, content=payload
        )

    async with gateway(handler) as (c, calls):
        r = await c.post("/mcp", content=rpc("tools/list"), headers=headers("viewer"))
    assert r.status_code == status
    assert r.content == payload
    assert len(calls) == 1


async def test_downstream_unreachable_502():
    def handler(request):
        raise httpx2.ConnectError("refused")

    async with gateway(handler) as (c, calls):
        r = await c.post("/mcp", content=rpc("tools/list", id=5), headers=headers("viewer"))
    assert r.status_code == 502
    assert r.json() == {
        "jsonrpc": "2.0",
        "id": 5,
        "error": {"code": -32603, "message": "Downstream unavailable"},
    }
    assert len(calls) == 1


async def test_downstream_timeout_504():
    def handler(request):
        raise httpx2.ReadTimeout("slow")

    async with gateway(handler) as (c, calls):
        r = await c.post("/mcp", content=rpc("tools/list", id=5), headers=headers("viewer"))
    assert r.status_code == 504
    assert r.json()["error"] == {"code": -32603, "message": "Downstream timeout"}
    assert r.json()["id"] == 5
    assert len(calls) == 1


async def test_legacy_get_delete_forwarded_after_auth():
    hdr = headers("viewer", accept="text/event-stream", **{"mcp-session-id": "s1"})
    async with gateway() as (c, calls):
        got = await c.get("/mcp", headers=hdr)
        deleted = await c.delete("/mcp", headers=hdr)
        anon_get = await c.get("/mcp", headers=headers(None))
        anon_delete = await c.delete("/mcp", headers=headers(None))
    assert got.status_code == 200
    assert deleted.status_code == 200
    assert [call.method for call in calls] == ["GET", "DELETE"]
    for call in calls:
        assert "authorization" not in call.headers
        assert call.content == b""
        assert call.headers["mcp-session-id"] == "s1"
    assert anon_get.status_code == 401
    assert anon_delete.status_code == 401
    assert len(calls) == 2


async def test_static_token_mode():
    overrides = {
        "auth_mode": "static",
        "jwt_secret": "",
        "static_tokens": {"viewer-token": "viewer", "admin-token": "admin"},
    }
    async with gateway(**overrides) as (c, calls):
        viewer = await c.post(
            "/mcp",
            content=ADMIN_CALL,
            headers=headers(None, authorization="Bearer viewer-token"),
        )
        assert viewer.json()["error"]["code"] == -32001
        assert calls == []
        admin = await c.post(
            "/mcp", content=ADMIN_CALL, headers=headers(None, authorization="Bearer admin-token")
        )
        jwt_in_static = await c.post("/mcp", content=ADMIN_CALL, headers=headers("admin"))
    assert admin.status_code == 200
    assert len(calls) == 1
    assert calls[0].content == ADMIN_CALL
    assert jwt_in_static.status_code == 401


async def test_session_id_does_not_carry_role():
    hdr = headers("viewer", **{"mcp-session-id": "admin-session"})
    async with gateway() as (c, calls):
        r = await c.post("/mcp", content=ADMIN_CALL, headers=hdr)
    assert r.json()["error"]["code"] == -32001
    assert calls == []


def _audit_records(caplog):
    return [
        json.loads(rec.getMessage()) for rec in caplog.records if rec.name == "mcp_gateway.audit"
    ]


async def test_each_decision_writes_one_audit_record(caplog):
    caplog.set_level(logging.INFO, logger="mcp_gateway.audit")
    secret_call = rpc("tools/call", {"name": "admin_reset_key", "arguments": {"pw": "hunter2"}})
    viewer_hdr = headers("viewer", **{"mcp-session-id": "raw-session-id"})
    async with gateway() as (c, calls):
        await c.post("/mcp", content=secret_call, headers=viewer_hdr)
        denied = _audit_records(caplog)
        await c.post("/mcp", content=rpc("tools/list"), headers=headers("admin"))
        await c.post("/mcp", content=rpc("tools/list"), headers=headers(None))
    assert len(denied) == 1
    deny = denied[0]
    assert deny["decision"] == "deny"
    assert deny["reason"] == "admin_tool_requires_admin_role"
    assert deny["upstream_contacted"] is False
    assert deny["tool"] == "admin_reset_key"
    assert deny["role"] == "viewer"
    assert deny["method"] == "tools/call"
    assert deny["rpc_id"] == 1
    assert deny["subject"].startswith("sha256:") and len(deny["subject"]) == len("sha256:") + 12
    assert len(deny["request_id"]) == 32
    records = _audit_records(caplog)
    assert [r["decision"] for r in records] == ["deny", "allow", "reject"]
    assert records[1]["upstream_contacted"] is True
    assert records[2]["reason"] == "missing_authorization"
    assert len(calls) == 1
    dump = caplog.text
    for forbidden in ("hunter2", "arguments", "raw-session-id", token("viewer"), "demo-user"):
        assert forbidden not in dump


async def test_lifespan_created_client_is_closed_and_defaults_cleared():
    from mcp_gateway.app import create_app
    from mcp_gateway.config import Settings

    app = create_app(Settings(jwt_secret=TEST_SECRET))
    async with app.router.lifespan_context(app):
        client = app.state.upstream
        assert str(client.base_url).rstrip("/") == "http://127.0.0.1:8001"
        assert "accept" not in client.headers
        assert "user-agent" not in client.headers
        assert client.follow_redirects is False
    assert client.is_closed
