"""Method allowlist, admin-tool policy, mirrored-header consistency and protocol era."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from mcp.shared.inbound import decode_header_value
from mcp_types import INVALID_PARAMS, INVALID_REQUEST, METHOD_NOT_FOUND

from .auth import Principal
from .config import Settings
from .jsonrpc import HEADER_MISMATCH, RpcMessage, RpcReject

ALLOWED_METHODS = frozenset(
    {
        "initialize",
        "ping",
        "server/discover",
        "tools/list",
        "tools/call",
        "resources/list",
        "resources/templates/list",
        "resources/read",
        "resources/subscribe",
        "resources/unsubscribe",
        "subscriptions/listen",
        "prompts/list",
        "prompts/get",
        "logging/setLevel",
        "completion/complete",
        "tasks/get",
        "tasks/result",
        "tasks/list",
        "tasks/cancel",
    }
)

MODERN_SINCE = "2026-07-28"
_MIRRORED_FIELD = {"tools/call": "name", "prompts/get": "name", "resources/read": "uri"}


def _values(raw_headers, name: bytes) -> list[bytes]:
    return [v for k, v in raw_headers if k == name]


def protocol_era(raw_headers) -> Literal["legacy", "modern"]:
    values = _values(raw_headers, b"mcp-protocol-version")
    if len(values) != 1:
        return "legacy"
    try:
        version = values[0].decode("ascii").strip()
    except UnicodeDecodeError:
        return "legacy"
    return "modern" if version >= MODERN_SINCE else "legacy"


def is_admin_tool(name: str) -> bool:
    return name.startswith("admin_")


def _mismatch(msg: RpcMessage) -> RpcReject:
    return RpcReject(400, HEADER_MISMATCH, "Header mismatch", msg.id, "header_mismatch")


def check_mirrored_headers(raw_headers, msg: RpcMessage) -> None:
    methods = _values(raw_headers, b"mcp-method")
    names = _values(raw_headers, b"mcp-name")
    if len(methods) > 1 or len(names) > 1:
        raise _mismatch(msg)
    if methods and methods[0].decode("latin-1") != msg.method:
        raise _mismatch(msg)
    if names:
        field = _MIRRORED_FIELD.get(msg.method or "")
        if field is None:
            return
        expected = msg.params.get(field) if isinstance(msg.params, dict) else None
        if decode_header_value(names[0].decode("latin-1")) != expected or expected is None:
            raise _mismatch(msg)


@dataclass(frozen=True)
class Decision:
    allowed: bool
    reason: str
    tool: str | None = None


def authorize(msg: RpcMessage, principal: Principal, settings: Settings, era: str) -> Decision:
    if msg.kind == "response":
        return Decision(True, "client_response")
    method = msg.method or ""
    if method not in ALLOWED_METHODS and not method.startswith("notifications/"):
        status = 404 if era == "modern" else 200
        raise RpcReject(status, METHOD_NOT_FOUND, "Method not found", msg.id, "unknown_method")
    if method == "tools/call":
        if msg.kind != "request":
            raise RpcReject(400, INVALID_REQUEST, "tools/call requires an id", None, "missing_id")
        name = msg.params.get("name") if isinstance(msg.params, dict) else None
        if not isinstance(name, str) or not name:
            raise RpcReject(400, INVALID_PARAMS, "Invalid params", msg.id, "invalid_tool_name")
        if is_admin_tool(name) and principal.role != "admin":
            return Decision(False, "admin_tool_requires_admin_role", name)
        return Decision(True, "allowed", name)
    return Decision(True, "allowed")
