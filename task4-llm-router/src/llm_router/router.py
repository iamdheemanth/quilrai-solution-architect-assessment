"""Primary/secondary failover with a total per-attempt deadline and a race-safe decision.

Each provider attempt (connect, send, response headers and the full body read) runs under a
single ``anyio.fail_after`` deadline. httpx2's per-phase timeouts are only a backstop
(``deadline + 0.5 s``), because a per-read timeout never fires on a body that trickles in.

The decision is "was a result produced?", not "did the deadline pass?": the attempt's result
is assigned as the last statement inside the deadline scope, with no checkpoint after it. If
the deadline fires first, nothing is assigned and the attempt is a timeout; if the result was
assigned, it is used even if the deadline expires immediately afterwards. So exactly one
provider's answer is ever used, and the secondary is never called once the primary has
produced a result.

Failover happens only on a primary 429 or timeout. Cancellation from outside (client
disconnect) is never caught here, so it propagates and never triggers failover. The primary
response is fully read and closed before the secondary starts.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Literal

import anyio
import anyio.lowlevel
import httpx2

from llm_router.config import Provider, Settings
from llm_router.errors import UpstreamError, UpstreamRateLimited, UpstreamTimeout

logger = logging.getLogger("llm_router")

_BACKSTOP_S = 0.5

ProviderName = Literal["primary", "secondary"]
FallbackReason = Literal["rate_limited", "timeout"]


@dataclass
class RouteResult:
    provider: ProviderName
    fallback_reason: FallbackReason | None
    status: int
    body: dict


@dataclass(frozen=True)
class _Outcome:
    kind: Literal["ok", "rate_limited", "timeout", "error"]
    status: int = 0
    body: dict | None = None


_TIMEOUT = _Outcome("timeout")
_ERROR = _Outcome("error")


def _build_request(provider: Provider, body: dict) -> httpx2.Request:
    # Built directly (not via client.build_request) so no client-level default headers,
    # cookies or auth can ride along: the upstream sees exactly these headers.
    payload = {**body, "model": provider.model}
    return httpx2.Request(
        "POST",
        provider.base_url.rstrip("/") + "/chat/completions",
        json=payload,
        headers={
            "Authorization": f"Bearer {provider.api_key}",
            "Content-Type": "application/json",
        },
        extensions={"timeout": httpx2.Timeout(provider.timeout_s + _BACKSTOP_S).as_dict()},
    )


def _classify(provider: Provider, status: int, raw: bytes) -> _Outcome:
    if status == 429:
        return _Outcome("rate_limited", status)
    if not 200 <= status < 300:
        logger.warning("upstream %s returned HTTP %d", provider.name, status)
        return _ERROR
    try:
        parsed: Any = json.loads(raw)
    except ValueError:  # JSONDecodeError and UnicodeDecodeError are both ValueErrors
        logger.warning("upstream %s returned a malformed JSON body", provider.name, exc_info=True)
        return _ERROR
    if not isinstance(parsed, dict):
        logger.warning("upstream %s returned a non-object JSON body", provider.name)
        return _ERROR
    return _Outcome("ok", status, parsed)


async def _attempt(client: httpx2.AsyncClient, provider: Provider, body: dict) -> _Outcome:
    request = _build_request(provider, body)
    result: tuple[int, bytes] | None = None
    try:
        with anyio.fail_after(provider.timeout_s):
            response = await client.send(request, stream=True, auth=None)
            try:
                raw = await response.aread()
            finally:
                await response.aclose()
            result = (response.status_code, raw)  # last statement: no checkpoint after it
    except (TimeoutError, httpx2.TimeoutException):
        if result is None:
            # An expected outcome with no useful detail: no traceback, just the fact.
            logger.warning(
                "upstream %s timed out after %d ms",
                provider.name,
                round(provider.timeout_s * 1000),
            )
            return _TIMEOUT
    except httpx2.HTTPError:
        logger.warning("upstream %s request failed", provider.name, exc_info=True)
        return _ERROR
    assert result is not None
    return _classify(provider, *result)


async def route(client: httpx2.AsyncClient, settings: Settings, body: dict) -> RouteResult:
    """Send body to the primary, failing over to the secondary on a 429 or timeout.

    Raises UpstreamRateLimited, UpstreamTimeout or UpstreamError with fixed public messages.
    """
    primary = await _attempt(client, settings.primary, body)
    if primary.kind == "ok":
        assert primary.body is not None
        return RouteResult("primary", None, primary.status, primary.body)
    if primary.kind == "error":
        raise UpstreamError() from None

    reason: FallbackReason = "rate_limited" if primary.kind == "rate_limited" else "timeout"
    # If the caller was cancelled while the primary failed, stop here rather than start the
    # secondary: outer cancellation never becomes failover.
    await anyio.lowlevel.checkpoint_if_cancelled()
    logger.info("failing over to %s reason=%s", settings.secondary.name, reason)

    secondary = await _attempt(client, settings.secondary, body)
    if secondary.kind == "ok":
        assert secondary.body is not None
        return RouteResult("secondary", reason, secondary.status, secondary.body)
    if secondary.kind == "rate_limited":
        raise UpstreamRateLimited() from None
    if secondary.kind == "timeout":
        raise UpstreamTimeout() from None
    raise UpstreamError() from None
