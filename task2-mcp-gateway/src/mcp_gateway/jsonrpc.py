"""Strict (I-JSON, RFC 7493) JSON-RPC parsing and message classification.

The gateway authorizes on the decoded body, so it must reject every body that the downstream
SDK's parsers might read differently (duplicate keys, BOM/UTF-16/32, NaN, lone surrogates...).
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any, Literal

from mcp_types import INVALID_REQUEST, PARSE_ERROR

UNAUTHORIZED_TOOL_CALL = -32001
AUTH_REQUIRED = -32005
HEADER_MISMATCH = -32020

RpcId = str | int


class NotIJSON(ValueError):
    """Body falls outside the strict JSON profile the gateway accepts."""

    def __init__(self, reason: str, message: str | None = None) -> None:
        super().__init__(message or reason)
        self.reason = reason


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    obj: dict[str, Any] = {}
    for key, value in pairs:
        if key in obj:  # keys compare after \u-unescaping
            raise NotIJSON("duplicate_key", "duplicate key")
        obj[key] = value
    return obj


def _finite(literal: str) -> float:
    value = float(literal)  # "1e400" -> inf
    if not math.isfinite(value):
        raise NotIJSON("non_finite_number", "non-finite number")
    return value


def _constant(literal: str) -> Any:
    raise NotIJSON("non_finite_number", f"{literal} is not JSON")


def parse_strict(body: bytes, max_depth: int = 64) -> Any:
    """Parse ``body`` under the I-JSON profile; raises only ``ValueError`` subclasses."""
    try:
        return _parse_strict(body, max_depth)
    except RecursionError as exc:
        raise NotIJSON("nesting_too_deep", "nesting too deep") from exc


def _parse_strict(body: bytes, max_depth: int) -> Any:
    if body.startswith(b"\xef\xbb\xbf"):
        raise NotIJSON("byte_order_mark", "byte-order mark")
    try:
        # Decoding to str first disables json's UTF-16/32 auto-detection.
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise NotIJSON("invalid_utf8", "invalid UTF-8") from exc
    try:
        msg = json.loads(
            text, object_pairs_hook=_object, parse_constant=_constant, parse_float=_finite
        )
    except NotIJSON:
        raise
    except ValueError as exc:  # JSONDecodeError, int digit limit
        raise NotIJSON("parse_error", str(exc)) from exc
    stack: list[tuple[Any, int]] = [(msg, 1)]  # iterative walk: depth cap + lone surrogates
    while stack:
        node, depth = stack.pop()
        if depth > max_depth:
            raise NotIJSON("nesting_too_deep", "nesting too deep")
        try:
            if isinstance(node, dict):
                for key, value in node.items():
                    key.encode("utf-8")
                    stack.append((value, depth + 1))
            elif isinstance(node, list):
                stack.extend((item, depth + 1) for item in node)
            elif isinstance(node, str):
                node.encode("utf-8")  # "\ud800" raises UnicodeEncodeError
        except UnicodeEncodeError as exc:
            raise NotIJSON("invalid_utf8", "lone surrogate") from exc
    return msg


@dataclass(frozen=True)
class RpcMessage:
    kind: Literal["request", "notification", "response"]
    method: str | None
    id: RpcId | None
    params: Any


class RpcReject(Exception):
    def __init__(
        self,
        http_status: int,
        code: int,
        message: str,
        rpc_id: RpcId | None,
        reason: str,
        data: dict | None = None,
    ) -> None:
        super().__init__(f"{code} {message}: {reason}")
        self.http_status = http_status
        self.code = code
        self.message = message
        self.rpc_id = rpc_id
        self.reason = reason
        self.data = data


def _invalid(reason: str) -> RpcReject:
    return RpcReject(400, INVALID_REQUEST, "Invalid Request", None, reason)


def _valid_id(value: Any) -> bool:
    # bool is an int subclass, so check the exact type.
    return type(value) is int or type(value) is str


def classify(msg: Any) -> RpcMessage:
    if not isinstance(msg, dict):
        raise _invalid("not_an_object")
    if msg.get("jsonrpc") != "2.0":
        raise _invalid("bad_jsonrpc_version")
    has_result = "result" in msg
    has_error = "error" in msg
    if "method" in msg:
        method = msg["method"]
        if not isinstance(method, str):
            raise _invalid("method_not_string")
        if has_result or has_error:
            raise _invalid("method_with_result_or_error")
        if "id" not in msg:
            return RpcMessage("notification", method, None, msg.get("params"))
        if not _valid_id(msg["id"]):
            raise _invalid("invalid_id")
        return RpcMessage("request", method, msg["id"], msg.get("params"))
    if "id" not in msg or not _valid_id(msg["id"]):
        raise _invalid("invalid_id")
    if has_result == has_error:
        raise _invalid("response_needs_exactly_one_of_result_error")
    return RpcMessage("response", None, msg["id"], msg["result"] if has_result else msg["error"])


def parse_message(body: bytes) -> RpcMessage:
    try:
        parsed = parse_strict(body)
    except NotIJSON as exc:
        raise RpcReject(400, PARSE_ERROR, "Parse error", None, exc.reason) from exc
    except ValueError as exc:
        raise RpcReject(400, PARSE_ERROR, "Parse error", None, "parse_error") from exc
    return classify(parsed)


def error_body(
    rpc_id: RpcId | None, code: int, message: str, data: dict | None = None
) -> bytes:
    error: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    payload = {"jsonrpc": "2.0", "id": rpc_id, "error": error}
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")
