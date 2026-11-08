import json
import logging

import pytest

from mcp_gateway.audit import MAX_ECHO, audit_event, safe_name
from mcp_gateway.auth import Principal
from mcp_gateway.config import Settings
from mcp_gateway.jsonrpc import HEADER_MISMATCH, RpcMessage, RpcReject
from mcp_gateway.policy import (
    ALLOWED_METHODS,
    Decision,
    authorize,
    check_mirrored_headers,
    is_admin_tool,
    protocol_era,
)

S = Settings()
VIEWER, ADMIN = Principal("v", "viewer"), Principal("a", "admin")


def call(name, id=1):
    return RpcMessage("request", "tools/call", id, {"name": name})


def hdrs(**kw):
    return [(k.replace("_", "-").encode(), v.encode("latin-1")) for k, v in kw.items()]


def test_viewer_denied_admin_tool():
    got = authorize(call("admin_reset_key"), VIEWER, S, "legacy")
    assert got == Decision(False, "admin_tool_requires_admin_role", "admin_reset_key")


def test_admin_allowed_admin_tool():
    assert authorize(call("admin_reset_key"), ADMIN, S, "legacy").allowed


def test_viewer_allowed_normal_tool_and_tools_list():
    got = authorize(call("get_status"), VIEWER, S, "legacy")
    assert got.allowed and got.reason == "allowed"
    lst = RpcMessage("request", "tools/list", 2, {})
    assert authorize(lst, VIEWER, S, "modern").allowed


def test_response_allowed():
    got = authorize(RpcMessage("response", None, 3, None), VIEWER, S, "legacy")
    assert got.allowed and got.reason == "client_response"


@pytest.mark.parametrize(
    "name", ["Admin_reset_key", "ADMIN_RESET", " admin_x", "\uff41dmin_x", "adm\u200bin_x"]
)
def test_literal_rule_lets_variants_through_to_exact_matching_downstream(name):
    assert not is_admin_tool(name)
    assert authorize(call(name), VIEWER, S, "legacy").allowed


def test_tools_call_without_id_rejected():
    msg = RpcMessage("notification", "tools/call", None, {"name": "get_status"})
    with pytest.raises(RpcReject) as ei:
        authorize(msg, ADMIN, S, "legacy")
    assert (ei.value.http_status, ei.value.code) == (400, -32600)
    assert ei.value.rpc_id is None


@pytest.mark.parametrize("params", [None, [], {"name": 5}, {"name": ""}, {}])
def test_bad_tools_call_params_32602(params):
    msg = RpcMessage("request", "tools/call", "9", params)
    with pytest.raises(RpcReject) as ei:
        authorize(msg, ADMIN, S, "legacy")
    assert (ei.value.http_status, ei.value.code) == (400, -32602)
    assert ei.value.rpc_id == "9"


@pytest.mark.parametrize("era,status", [("legacy", 200), ("modern", 404)])
@pytest.mark.parametrize("method", ["admin/reset", "Tools/Call"])
def test_unknown_method_denied(era, status, method):
    with pytest.raises(RpcReject) as ei:
        authorize(RpcMessage("request", method, 7, {}), ADMIN, S, era)
    assert (ei.value.http_status, ei.value.code, ei.value.rpc_id) == (status, -32601, 7)
    assert ei.value.reason == "unknown_method"


def test_unknown_method_notification_is_400_in_both_eras():
    for era in ("legacy", "modern"):
        with pytest.raises(RpcReject) as ei:
            authorize(RpcMessage("notification", "admin/reset", None, {}), VIEWER, S, era)
        assert (ei.value.http_status, ei.value.code, ei.value.rpc_id) == (400, -32601, None)
        assert ei.value.reason == "unknown_method"


def test_notifications_prefix_allowed():
    msg = RpcMessage("notification", "notifications/initialized", None, None)
    assert authorize(msg, VIEWER, S, "legacy").allowed
    assert len(ALLOWED_METHODS) == 19


def test_mirrored_header_mismatch():
    msg = call("admin_reset_key")
    with pytest.raises(RpcReject) as ei:
        check_mirrored_headers(hdrs(mcp_name="get_status"), msg)
    assert (ei.value.http_status, ei.value.code) == (400, HEADER_MISMATCH)
    assert ei.value.reason == "header_mismatch" and ei.value.rpc_id == 1
    with pytest.raises(RpcReject):
        check_mirrored_headers([(b"mcp-method", b"tools/call")] * 2, msg)
    with pytest.raises(RpcReject):
        check_mirrored_headers(hdrs(mcp_method="tools/list"), msg)
    assert (
        check_mirrored_headers(hdrs(mcp_method="tools/call", mcp_name="admin_reset_key"), msg)
        is None
    )
    assert check_mirrored_headers([], msg) is None


def test_mirrored_header_uri_and_prompts():
    read = RpcMessage("request", "resources/read", 1, {"uri": "file:///a"})
    assert check_mirrored_headers(hdrs(mcp_name="file:///a"), read) is None
    with pytest.raises(RpcReject):
        check_mirrored_headers(hdrs(mcp_name="file:///b"), read)
    get = RpcMessage("request", "prompts/get", 1, {"name": "p"})
    with pytest.raises(RpcReject):
        check_mirrored_headers(hdrs(mcp_name="q"), get)


def test_mirrored_header_base64_decoded():
    h = hdrs(mcp_name="=?base64?YWRtaW5fcmVzZXRfa2V5?=")
    assert check_mirrored_headers(h, call("admin_reset_key")) is None


@pytest.mark.parametrize("value", ["=?base64?!!!?=", "=?base64?/w==?="])
def test_mirrored_header_bad_encoding_is_mismatch(value):
    with pytest.raises(RpcReject):
        check_mirrored_headers(hdrs(mcp_name=value), call("x"))


def test_mirrored_header_non_ascii_bytes_is_mismatch_not_exception():
    with pytest.raises(RpcReject):
        check_mirrored_headers([(b"mcp-name", b"\xff\xfe")], call("x"))


def test_mirrored_header_bad_params_is_mismatch():
    msg = RpcMessage("request", "tools/call", 1, None)
    with pytest.raises(RpcReject):
        check_mirrored_headers(hdrs(mcp_name="x"), msg)


@pytest.mark.parametrize(
    ("headers", "era"),
    [
        ([], "legacy"),
        (hdrs(mcp_protocol_version="2025-11-25"), "legacy"),
        (hdrs(mcp_protocol_version="2025-06-18"), "legacy"),
        (hdrs(mcp_protocol_version="2026-07-28"), "modern"),
        # The SDK treats any value outside HANDSHAKE_PROTOCOL_VERSIONS as modern.
        (hdrs(mcp_protocol_version="2025-01-01"), "modern"),
        (hdrs(mcp_protocol_version="2027-01-01"), "modern"),
        ([(b"mcp-protocol-version", b"\xff")], "modern"),
        # Only the first header value counts, as in the SDK.
        (
            hdrs(mcp_protocol_version="2026-07-28") + hdrs(mcp_protocol_version="2025-11-25"),
            "modern",
        ),
        (
            hdrs(mcp_protocol_version="2025-11-25") + hdrs(mcp_protocol_version="2026-07-28"),
            "legacy",
        ),
    ],
)
def test_protocol_era(headers, era):
    assert protocol_era(headers) == era


def test_audit_event_is_single_escaped_json_line(caplog):
    with caplog.at_level(logging.INFO, logger="mcp_gateway.audit"):
        audit_event(decision="deny", tool=safe_name("x\nrefund decision=allow"))
    [r] = [r for r in caplog.records if r.name == "mcp_gateway.audit"]
    rec = json.loads(r.getMessage())
    assert "\n" not in r.getMessage() and rec["tool"].startswith("x\\u000a")
    assert rec["event"] == "authz.decision" and rec["ts"].endswith("+00:00")
    assert r.levelno == logging.INFO


def test_audit_event_never_raises_on_odd_values(caplog):
    with caplog.at_level(logging.INFO, logger="mcp_gateway.audit"):
        audit_event(obj=object(), s={1, 2}, bad="\ud800")
    [r] = [r for r in caplog.records if r.name == "mcp_gateway.audit"]
    assert json.loads(r.getMessage())["event"] == "authz.decision"


def test_safe_name_caps_at_64():
    assert MAX_ECHO == 64
    assert safe_name("a" * 500) == "a" * 64 + "\u2026"
    assert safe_name("a" * 64) == "a" * 64


def test_safe_name_escapes_control_and_format_chars():
    assert safe_name("a\x00b\u202ec\u200bd\x7f") == "a\\u0000b\\u202ec\\u200bd\\u007f"
    assert safe_name("\U000e0041") == "\\U000e0041"
    assert safe_name("caf\u00e9") == "caf\u00e9"
