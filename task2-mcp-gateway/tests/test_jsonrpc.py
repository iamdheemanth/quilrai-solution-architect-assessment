import json

import pydantic_core
import pytest

from mcp_gateway.jsonrpc import (
    AUTH_REQUIRED,
    HEADER_MISMATCH,
    UNAUTHORIZED_TOOL_CALL,
    NotIJSON,
    RpcMessage,
    RpcReject,
    classify,
    error_body,
    parse_message,
    parse_strict,
)

REJECT_PARSE = [
    (
        b'{"jsonrpc":"2.0","id":1,"method":"tools/call",'
        b'"params":{"name":"get_status","name":"admin_reset_key"}}',
        "duplicate_key",
    ),
    (
        b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"n\\u0061me":"x","name":"y"}}',
        "duplicate_key",
    ),
    (
        b'{"jsonrpc":"2.0","method":"tools/list","method":"tools/call","id":1}',
        "duplicate_key",
    ),
    (b'\xef\xbb\xbf{"jsonrpc":"2.0","id":1,"method":"ping"}', "byte_order_mark"),
    ('{"jsonrpc":"2.0","id":1,"method":"ping"}'.encode("utf-16"), "invalid_utf8"),
    ('{"jsonrpc":"2.0","id":1,"method":"ping"}'.encode("utf-32"), "invalid_utf8"),
    (b'{"jsonrpc":"2.0","id":1,"method":"ping","params":{"x":NaN}}', "non_finite_number"),
    (b'{"jsonrpc":"2.0","id":1,"method":"ping","params":{"x":-Infinity}}', "non_finite_number"),
    (b'{"jsonrpc":"2.0","id":1,"method":"ping","params":{"x":1e400}}', "non_finite_number"),
    (b'{"jsonrpc":"2.0","id":1,"method":"ping","params":{"x":"\\ud800"}}', "invalid_utf8"),
    (b'{"jsonrpc":"2.0","id":1,"method":"ping","params":{"\\ud800":1}}', "invalid_utf8"),
    (b'{"a":' * 65 + b"1" + b"}" * 65, "nesting_too_deep"),
    (b"[" * 100000 + b"]" * 100000, "nesting_too_deep"),
    (b"not json", "parse_error"),
    (b"", "parse_error"),
    (b"1" * 5000, "parse_error"),
]


@pytest.mark.parametrize(("body", "reason"), REJECT_PARSE)
def test_parse_rejects_with_32700(body, reason):
    with pytest.raises(RpcReject) as exc:
        parse_message(body)
    err = exc.value
    got = (err.http_status, err.code, err.message, err.rpc_id)
    assert got == (400, -32700, "Parse error", None)
    assert err.reason == reason


def test_depth_64_is_accepted():
    body = b'{"a":' * 63 + b"1" + b"}" * 63
    assert parse_strict(body)["a"] is not None


def test_parse_strict_raises_only_value_errors():
    for body, _ in REJECT_PARSE:
        with pytest.raises(ValueError):
            parse_strict(body)
    assert issubclass(NotIJSON, ValueError)


CLASSIFY_REJECT = [
    b'[{"jsonrpc":"2.0","id":1,"method":"ping"}]',
    b'"x"',
    b'{"id":1,"method":"ping"}',
    b'{"jsonrpc":"2.0","id":1,"method":"tools/call","result":{}}',
    b'{"jsonrpc":"2.0","id":null,"method":"ping"}',
    b'{"jsonrpc":"2.0","id":1.0,"method":"ping"}',
    b'{"jsonrpc":"2.0","id":true,"method":"ping"}',
    b'{"jsonrpc":"2.0","id":{},"method":"ping"}',
    b'{"jsonrpc":"2.0","id":1,"method":7}',
    b'{"jsonrpc":"2.0","id":1}',
    b'{"jsonrpc":"2.0","id":1,"result":{},"error":{}}',
    b'{"jsonrpc":"2.0","result":{}}',
]


@pytest.mark.parametrize("body", CLASSIFY_REJECT)
def test_classify_rejects_with_32600(body):
    with pytest.raises(RpcReject) as exc:
        parse_message(body)
    err = exc.value
    assert (err.http_status, err.code, err.rpc_id) == (400, -32600, None)


def test_escaped_name_is_decoded():
    m = parse_message(
        b'{"jsonrpc":"2.0","id":"7","method":"tools/call",'
        b'"params":{"name":"\\u0061dmin_reset_key"}}'
    )
    assert (m.kind, m.method, m.id, m.params["name"]) == (
        "request",
        "tools/call",
        "7",
        "admin_reset_key",
    )


def test_notification_and_response_kinds():
    n = parse_message(b'{"jsonrpc":"2.0","method":"notifications/initialized"}')
    assert (n.kind, n.method, n.id) == ("notification", "notifications/initialized", None)
    r = parse_message(b'{"jsonrpc":"2.0","id":3,"result":{}}')
    assert (r.kind, r.method, r.id) == ("response", None, 3)
    e = parse_message(b'{"jsonrpc":"2.0","id":"x","error":{"code":1,"message":"m"}}')
    assert (e.kind, e.id) == ("response", "x")


def test_classify_returns_frozen_message():
    m = classify({"jsonrpc": "2.0", "id": 1, "method": "ping"})
    assert isinstance(m, RpcMessage)
    with pytest.raises(AttributeError):
        m.method = "x"  # type: ignore[misc]


def test_error_body_shape():
    body = error_body(7, -32001, "Unauthorized Tool Call", {"reason": "forbidden_tool"})
    assert json.loads(body) == {
        "jsonrpc": "2.0",
        "id": 7,
        "error": {
            "code": -32001,
            "message": "Unauthorized Tool Call",
            "data": {"reason": "forbidden_tool"},
        },
    }
    assert json.loads(error_body(None, -32700, "Parse error"))["id"] is None
    assert b" " not in error_body("a", -1, "m")
    assert "data" not in json.loads(error_body(1, -1, "m"))["error"]


def test_constants():
    assert (UNAUTHORIZED_TOOL_CALL, AUTH_REQUIRED, HEADER_MISMATCH) == (-32001, -32005, -32020)


ACCEPTED = [
    b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_status",'
    b'"arguments":{"n":1E2,"s":"caf\\u00e9"}}}',
    b'{"jsonrpc":"2.0","id":"a","method":"initialize","params":{"protocolVersion":"2025-11-25",'
    b'"capabilities":{},"clientInfo":{"name":"c","version":"1"}}}',
    b'{"jsonrpc":"2.0","method":"notifications/initialized"}',
]


_BS = chr(92)  # one literal backslash


def _wrap(args: str) -> bytes:
    head = (
        '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_status","arguments":'
    )
    return (head + args + "}}").encode("utf-8")


HARD_ACCEPTED = {
    "escaped-key": _wrap('{"n' + _BS + 'u0061me_x":1}'),
    "surrogate-pair": _wrap('{"s":"' + _BS + "ud83d" + _BS + 'ude00"}'),
    "raw-astral": _wrap('{"s":"\U0001f600\U0010ffff"}'),
    "int-2^64": _wrap('{"n":18446744073709551616}'),
    "int-4300-digits": _wrap('{"n":' + "9" * 4300 + "}"),
    "neg-zero-float": _wrap('{"n":-0.0}'),
    "neg-zero-int": _wrap('{"n":-0}'),
    "int-vs-float-vs-bool": _wrap('{"a":1,"b":1.0,"c":true,"d":0,"e":false}'),
    "tiny-underflow": _wrap('{"n":1e-400}'),
    "subnormal": _wrap('{"n":5e-324}'),
    "mantissa-700-digits": _wrap('{"n":0.' + "1" * 700 + "}"),
    "raw-del-c1-ls-bom-ffff": _wrap('{"s":"\x7f\u0085\u2028\ufeff\uffff"}'),
    "nfc-nfd-sibling-keys": _wrap('{"caf\u00e9":1,"cafe\u0301":2}'),
    "depth-64": b'{"a":' * 63 + b"1" + b"}" * 63,
    "escaped-slash-method": (
        '{"jsonrpc":"2.0","id":1,"method":"tools' + _BS + '/call","params":{"name":"x"}}'
    ).encode(),
}


def _dumps(value) -> str:
    return json.dumps(value, sort_keys=False, ensure_ascii=True)


@pytest.mark.parametrize(
    "body",
    [*ACCEPTED, *HARD_ACCEPTED.values()],
    ids=[*(f"base{i}" for i in range(len(ACCEPTED))), *HARD_ACCEPTED],
)
def test_accepted_bodies_decode_identically_in_both_sdk_parsers(body):
    # Serialising compares types too: 1 vs 1.0 vs True and -0.0 vs 0.0 all differ.
    ours = _dumps(parse_strict(body))
    assert ours == _dumps(pydantic_core.from_json(body)) == _dumps(json.loads(body))
