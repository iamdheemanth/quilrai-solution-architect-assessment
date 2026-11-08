"""Pure ASGI middlewares: bearer authentication, then JSON-RPC policy."""

from __future__ import annotations

import hashlib
import uuid

from mcp_types import INVALID_REQUEST

from .audit import audit_event, safe_name
from .auth import AuthError, Principal, build_verifier, extract_bearer
from .config import Settings
from .jsonrpc import (
    AUTH_REQUIRED,
    UNAUTHORIZED_TOOL_CALL,
    RpcMessage,
    RpcReject,
    error_body,
    parse_message,
)
from .policy import authorize, check_mirrored_headers, protocol_era

MCP_PATH = "/mcp"


async def send_json(send, status: int, body: bytes, extra=()) -> None:
    headers = [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(body)).encode()),
        *extra,
    ]
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": body})


def _subject(settings: Settings, principal: Principal | None) -> str | None:
    if principal is None:
        return None
    if settings.auth_mode == "static":  # already hashed by the verifier
        return principal.subject
    return "sha256:" + hashlib.sha256(principal.subject.encode("utf-8")).hexdigest()[:12]


def _rpc_id(value):
    return safe_name(value) if isinstance(value, str) else value


class BearerAuthMiddleware:
    def __init__(self, app, settings: Settings) -> None:
        self.app = app
        self.verifier = build_verifier(settings)

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        state = scope.setdefault("state", {})
        state["request_id"] = uuid.uuid4().hex
        state["era"] = protocol_era(scope["headers"])
        try:
            principal = self.verifier.verify(extract_bearer(scope["headers"]))
        except AuthError as e:
            audit_event(
                decision="reject",
                reason=e.reason,
                subject=None,
                role=None,
                era=state["era"],
                method=None,
                tool=None,
                rpc_id=None,
                request_id=state["request_id"],
                upstream_contacted=False,
            )
            body = error_body(None, AUTH_REQUIRED, "Authentication required", {"reason": e.reason})
            await send_json(
                send, e.status, body, [(b"www-authenticate", e.www_authenticate.encode("latin-1"))]
            )
            return
        state["principal"] = principal
        await self.app(scope, receive, send)


def _supported_content_type(raw_headers) -> bool:
    types = [v for k, v in raw_headers if k == b"content-type"]
    if len(types) != 1 or any(k == b"content-encoding" for k, _ in raw_headers):
        return False
    media, *params = (p.strip() for p in types[0].decode("latin-1").split(";"))
    if media.lower() != "application/json":
        return False
    return all(p.replace(" ", "").lower() in ("charset=utf-8", 'charset="utf-8"') for p in params)


async def _read_body(receive) -> bytes | None:
    chunks: list[bytes] = []
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":
            return None
        chunks.append(message.get("body", b""))
        if not message.get("more_body", False):
            return b"".join(chunks)


class JsonRpcPolicyMiddleware:
    def __init__(self, app, settings: Settings) -> None:
        self.app = app
        self.settings = settings

    def _audit(self, scope, decision, reason, msg=None, tool=None, rpc_id=None, contacted=False):
        state = scope["state"]
        principal = state.get("principal")
        audit_event(
            decision=decision,
            reason=reason,
            subject=_subject(self.settings, principal),
            role=principal.role if principal else None,
            era=state.get("era"),
            method=safe_name(msg.method) if msg is not None and msg.method else None,
            tool=safe_name(tool) if tool else None,
            rpc_id=_rpc_id(rpc_id),
            request_id=state.get("request_id"),
            upstream_contacted=contacted,
        )

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        if scope["path"] != MCP_PATH:  # exact path only: no redirects, never forwarded
            self._audit(scope, "reject", "unknown_path")
            await send_json(send, 404, error_body(None, INVALID_REQUEST, "Not Found"))
            return
        if scope["method"] not in ("POST", "GET", "DELETE"):  # incl. Starlette's implicit HEAD
            self._audit(scope, "reject", "method_not_allowed")
            body = error_body(None, INVALID_REQUEST, "Method Not Allowed")
            await send_json(send, 405, body, [(b"allow", b"POST, GET, DELETE")])
            return
        if scope["method"] != "POST":  # legacy stream / session end
            self._audit(scope, "allow", "legacy_passthrough", contacted=True)
            await self.app(scope, receive, send)
            return
        if not _supported_content_type(scope["headers"]):
            self._audit(scope, "reject", "unsupported_media_type")
            body = error_body(None, INVALID_REQUEST, "Unsupported Media Type")
            await send_json(send, 415, body)
            return
        body = await _read_body(receive)
        if body is None:
            return
        state = scope["state"]
        principal: Principal = state["principal"]
        msg: RpcMessage | None = None
        try:
            msg = parse_message(body)
            check_mirrored_headers(scope["headers"], msg)
            decision = authorize(msg, principal, self.settings, state["era"])
        except RpcReject as e:
            self._audit(scope, "reject", e.reason, msg=msg, rpc_id=e.rpc_id)
            await send_json(send, e.http_status, error_body(e.rpc_id, e.code, e.message, e.data))
            return
        if not decision.allowed:
            self._audit(scope, "deny", decision.reason, msg, decision.tool, msg.id)
            data = {
                "reason": "forbidden_tool",
                "tool": safe_name(decision.tool or ""),
                "requiredRole": "admin",
                "role": principal.role,
            }
            await send_json(
                send,
                self.settings.deny_http_status,
                error_body(msg.id, UNAUTHORIZED_TOOL_CALL, "Unauthorized Tool Call", data),
            )
            return
        self._audit(scope, "allow", decision.reason, msg, decision.tool, msg.id, contacted=True)
        state["rpc"] = msg
        state["body"] = body
        replayed = False

        async def replay():
            nonlocal replayed
            if not replayed:
                replayed = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        await self.app(scope, replay, send)
