"""Standardized gateway error contract.

Every error response has the single shape
``{"error": {"message", "type", "param", "code", "request_id"}}`` plus an ``x-request-id``
header. Messages are fixed strings per class: never ``str(exc)``, upstream bodies, URLs,
hostnames, keys or paths. Full exception detail goes to the server log only.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Mapping

from starlette.requests import Request
from starlette.responses import JSONResponse

logger = logging.getLogger("llm_router")


class GatewayError(Exception):
    status: int = 500
    code: str = "internal_error"
    type: str = "server_error"
    message: str = "The gateway encountered an internal error."
    retry_after: int | None = None
    headers: Mapping[str, str] | None = None

    def __init__(self, message: str | None = None, param: str | None = None) -> None:
        # Deliberately do not pass the message to Exception: str(err) stays empty.
        super().__init__()
        if message is not None:
            self.message = message
        self.param = param


class InvalidApiKey(GatewayError):
    status = 401
    code = "invalid_api_key"
    type = "authentication_error"
    message = "Missing or invalid API key."


class InvalidRequest(GatewayError):
    status = 400
    code = "invalid_request"
    type = "invalid_request_error"
    message = "The request is invalid."

    def __init__(self, message: str, param: str | None = None) -> None:
        super().__init__(message=message, param=param)


class PayloadTooLarge(GatewayError):
    status = 413
    code = "payload_too_large"
    type = "invalid_request_error"
    message = "The request body is too large."


class NotFound(GatewayError):
    status = 404
    code = "not_found"
    type = "invalid_request_error"
    message = "Not found."


class MethodNotAllowed(GatewayError):
    status = 405
    code = "method_not_allowed"
    type = "invalid_request_error"
    message = "Method not allowed."
    headers = {"Allow": "POST"}


class RequestExceedsRateLimit(GatewayError):
    status = 400
    code = "request_exceeds_rate_limit"
    type = "invalid_request_error"
    message = "The request is larger than the total token budget allowed for this API key."


class GatewayRateLimited(GatewayError):
    status = 429
    code = "gateway_rate_limited"
    type = "rate_limit_error"
    message = (
        "Rate limit exceeded for this API key. Retry after the time in the Retry-After header."
    )

    def __init__(self, retry_after: int) -> None:
        super().__init__()
        self.retry_after = max(1, int(retry_after))


class UpstreamRateLimited(GatewayError):
    status = 429
    code = "upstream_rate_limited"
    type = "rate_limit_error"
    message = "All upstream providers are rate limited. Please retry shortly."
    retry_after = 1


class UpstreamTimeout(GatewayError):
    status = 504
    code = "upstream_timeout"
    type = "api_error"
    message = "The upstream provider did not respond in time."


class UpstreamError(GatewayError):
    status = 502
    code = "upstream_error"
    type = "api_error"
    message = "The upstream provider returned an error."


class RateLimiterUnavailable(GatewayError):
    status = 503
    code = "rate_limiter_unavailable"
    type = "api_error"
    message = "The rate limiter is temporarily unavailable. Please retry shortly."


class InternalError(GatewayError):
    status = 500
    code = "internal_error"
    type = "server_error"
    message = "The gateway encountered an internal error."


def error_response(
    err: GatewayError, request_id: str, headers: Mapping[str, str] | None = None
) -> JSONResponse:
    out: dict[str, str] = {}
    if err.headers:
        out.update(err.headers)
    if headers:
        out.update(headers)
    if err.retry_after is not None:
        out["Retry-After"] = str(err.retry_after)
    out["x-request-id"] = request_id
    body = {
        "error": {
            "message": err.message,
            "type": err.type,
            "param": err.param,
            "code": err.code,
            "request_id": request_id,
        }
    }
    return JSONResponse(body, status_code=err.status, headers=out)


def get_request_id(request: Request) -> str:
    rid = getattr(request.state, "request_id", None)
    if not rid:
        rid = uuid.uuid4().hex
        request.state.request_id = rid
    return rid


async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    rid = get_request_id(request)
    logger.warning("unhandled exception request_id=%s", rid, exc_info=exc)
    return error_response(InternalError(), rid)
