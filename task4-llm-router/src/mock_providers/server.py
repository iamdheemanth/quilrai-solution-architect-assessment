"""Mock OpenAI-compatible provider (non-streaming Chat Completions) with scripted behaviours.

Behaviours:

- ``ok``: a chat completion with ``usage.total_tokens = usage_tokens``;
- ``rate_limit``: HTTP 429 with an OpenAI-style JSON error body;
- ``slow``: sleeps ``delay_s`` and then answers like ``ok``;
- ``error500``: HTTP 500 with a JSON error body;
- ``trickle``: the ``ok`` body streamed a few bytes at a time, ``delay_s`` between chunks.

With ``leak=True`` the error bodies (429 and 500) carry a fake traceback, an ``sk-`` key, an
internal hostname and a filesystem path, so tests can prove none of it reaches a client.
"""

from __future__ import annotations

import json
import math
import os
import sys
from collections.abc import AsyncIterator
from typing import Literal, get_args

import anyio
import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

Behavior = Literal["ok", "rate_limit", "slow", "error500", "trickle"]

LEAK_CANARIES = ("Traceback", "sk-LEAK", "internal.corp.local", "/srv/secret")
_LEAK_DETAIL = (
    "Traceback (most recent call last):\n"
    '  File "/srv/secret/provider/handler.py", line 42, in complete\n'
    "    connect('llm-07.internal.corp.local', api_key='sk-LEAK-0123456789abcdef')\n"
    "RuntimeError: backend exploded"
)
_CREATED = 1_700_000_000
_TRICKLE_CHUNK = 16


def _completion(model: str, usage_tokens: int) -> dict:
    prompt = min(usage_tokens, 10)
    return {
        "id": "chatcmpl-mock",
        "object": "chat.completion",
        "created": _CREATED,
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": f"Hello from {model}"},
                "logprobs": None,
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": prompt,
            "completion_tokens": usage_tokens - prompt,
            "total_tokens": usage_tokens,
        },
    }


def _error(message: str, type_: str, code: str, leak: bool) -> dict:
    if leak:
        message = f"{message}\n{_LEAK_DETAIL}"
    return {"error": {"message": message, "type": type_, "param": None, "code": code}}


def build_provider_app(
    *,
    behavior: Behavior,
    delay_s: float = 0.0,
    model: str = "mock-x",
    usage_tokens: int = 50,
    leak: bool = False,
) -> Starlette:
    if behavior not in get_args(Behavior):
        raise ValueError(f"unknown behavior {behavior!r}")

    async def trickle(raw: bytes) -> AsyncIterator[bytes]:
        for i in range(0, len(raw), _TRICKLE_CHUNK):
            yield raw[i : i + _TRICKLE_CHUNK]
            await anyio.sleep(delay_s)

    async def completions(request: Request) -> Response:
        try:
            body = await request.json()
        except ValueError:
            body = None
        if not isinstance(body, dict):
            return JSONResponse(
                _error("invalid JSON body", "invalid_request_error", "invalid_body", False),
                status_code=400,
            )
        if behavior == "rate_limit":
            return JSONResponse(
                _error("Rate limit reached", "rate_limit_error", "rate_limit_exceeded", leak),
                status_code=429,
                headers={"retry-after": "1"},
            )
        if behavior == "error500":
            return JSONResponse(
                _error("The server had an error", "server_error", "internal_error", leak),
                status_code=500,
            )
        if behavior == "slow":
            await anyio.sleep(delay_s)
        completion = _completion(model, usage_tokens)
        if behavior == "trickle":
            raw = json.dumps(completion).encode()
            return StreamingResponse(trickle(raw), media_type="application/json")
        return JSONResponse(completion)

    return Starlette(routes=[Route("/v1/chat/completions", completions, methods=["POST"])])


def _fail(message: str) -> None:
    sys.stderr.write(f"mock-provider: {message}\n")
    raise SystemExit(2)


def main() -> None:
    """Run one mock provider: MOCK_PROVIDER_BEHAVIOR / _PORT / _DELAY (and _HOST)."""
    behavior = os.environ.get("MOCK_PROVIDER_BEHAVIOR", "ok")
    if behavior not in get_args(Behavior):
        _fail(f"MOCK_PROVIDER_BEHAVIOR must be one of: {', '.join(get_args(Behavior))}")
    try:
        port = int(os.environ.get("MOCK_PROVIDER_PORT", "9001"))
        if not 0 < port < 65536:
            raise ValueError("out of range")
    except ValueError:
        _fail("MOCK_PROVIDER_PORT must be an integer in 1..65535")
    try:
        delay = float(os.environ.get("MOCK_PROVIDER_DELAY", "0"))
        if not (delay >= 0 and math.isfinite(delay)):
            raise ValueError("invalid")
    except ValueError:
        _fail("MOCK_PROVIDER_DELAY must be a finite number >= 0")
    host = os.environ.get("MOCK_PROVIDER_HOST", "127.0.0.1")
    app = build_provider_app(behavior=behavior, delay_s=delay)  # type: ignore[arg-type]
    uvicorn.run(app, host=host, port=port, server_header=False)


if __name__ == "__main__":
    main()
