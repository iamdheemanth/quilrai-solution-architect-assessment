"""Scripted mock OpenAI-compatible provider (Chat Completions) with fault injection.

The default script splits an email, an SSN and a card number across token boundaries and
ends with an email, so a redacting gateway must hold back, match across chunks and flush
its tail.
"""

import contextlib
import json
import os
import sys
from collections.abc import AsyncIterator
from typing import Literal

import anyio
import anyio.lowlevel
import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

Fault = Literal["error_frame", "drop", "crlf", "cr", "bom"]

DEFAULT_TOKENS: list[str] = [
    "Sure. Contact john.d",
    "oe@exa",
    "mple.com",
    " or call about SSN 123-4",
    "5-6789",
    "; card 4111 11",
    "11 1111 1111",
    " is on file. Reach me at ",
    "ja",
    "ne@exa",
    "mple.org",
]

_BOM = b"\xef\xbb\xbf"
_FINGERPRINT = "fp_mock"
_CREATED = 1_700_000_000
_PROMPT_TOKENS = 5
_ERROR_FRAME = {
    "error": {"message": "mock upstream failure", "type": "server_error", "code": "mock_error"}
}


def _chunk(cid: str, model: str, choices: list[dict]) -> dict:
    return {
        "id": cid,
        "object": "chat.completion.chunk",
        "created": _CREATED,
        "model": model,
        "system_fingerprint": _FINGERPRINT,
        "choices": choices,
    }


def _choice(index: int, delta: dict, finish: str | None = None) -> dict:
    return {"index": index, "delta": delta, "logprobs": None, "finish_reason": finish}


def _scripts(body: dict, tokens: list[str], choices: list[list[str]] | None) -> list[list[str]]:
    if choices is not None:
        return [list(c) for c in choices]
    n = body.get("n", 1)
    n = n if isinstance(n, int) and n >= 1 else 1
    return [list(tokens) for _ in range(n)]


def build_mock_app(
    *,
    tokens: list[str] | None = None,
    choices: list[list[str]] | None = None,
    delay: float = 0.0,
    fault: Fault | None = None,
    events_per_write: int = 1,
) -> Starlette:
    base_tokens = list(DEFAULT_TOKENS if tokens is None else tokens)
    eol = {"crlf": b"\r\n", "cr": b"\r"}.get(fault or "", b"\n")
    batch = max(1, events_per_write)

    def frame(payload: dict | str) -> bytes:
        text = payload if isinstance(payload, str) else json.dumps(payload, separators=(",", ":"))
        return b"data: " + text.encode() + eol + eol

    async def events(body: dict, scripts: list[list[str]]) -> AsyncIterator[bytes]:
        model = body.get("model") or "mock-1"
        cid = "chatcmpl-mock"
        opts = body.get("stream_options")
        include_usage = isinstance(opts, dict) and bool(opts.get("include_usage"))
        steps = max((len(s) for s in scripts), default=0)
        cutoff = max(1, steps // 2) if fault in ("error_frame", "drop") else None

        for i in range(len(scripts)):
            yield frame(_chunk(cid, model, [_choice(i, {"role": "assistant", "content": ""})]))
        for step in range(steps):
            if cutoff is not None and step >= cutoff:
                if fault == "error_frame":
                    yield frame(_ERROR_FRAME)
                return
            await anyio.lowlevel.checkpoint()
            if delay > 0:
                await anyio.sleep(delay)
            for i, script in enumerate(scripts):
                if step < len(script):
                    yield frame(_chunk(cid, model, [_choice(i, {"content": script[step]})]))
        for i in range(len(scripts)):
            yield frame(_chunk(cid, model, [_choice(i, {}, "stop")]))
        if include_usage:
            done = sum(len(s) for s in scripts)
            usage = {
                "prompt_tokens": _PROMPT_TOKENS,
                "completion_tokens": done,
                "total_tokens": _PROMPT_TOKENS + done,
            }
            yield frame({**_chunk(cid, model, []), "usage": usage})
        yield frame("[DONE]")

    async def body_iter(body: dict, scripts: list[list[str]]) -> AsyncIterator[bytes]:
        pending: list[bytes] = []
        first = True
        async with contextlib.aclosing(events(body, scripts)) as gen:
            async for ev in gen:
                pending.append(ev)
                if len(pending) >= batch:
                    out = b"".join(pending)
                    pending = []
                    yield (_BOM + out) if (first and fault == "bom") else out
                    first = False
        if pending:
            out = b"".join(pending)
            yield (_BOM + out) if (first and fault == "bom") else out

    async def completions(request: Request) -> Response:
        try:
            body = await request.json()
        except ValueError:
            body = None
        if not isinstance(body, dict):
            return JSONResponse(
                {"error": {"message": "invalid JSON body", "type": "invalid_request_error"}},
                status_code=400,
            )
        scripts = _scripts(body, base_tokens, choices)
        if not body.get("stream"):
            model = body.get("model") or "mock-1"
            done = sum(len(s) for s in scripts)
            return JSONResponse(
                {
                    "id": "chatcmpl-mock",
                    "object": "chat.completion",
                    "created": _CREATED,
                    "model": model,
                    "system_fingerprint": _FINGERPRINT,
                    "choices": [
                        {
                            "index": i,
                            "message": {"role": "assistant", "content": "".join(s)},
                            "logprobs": None,
                            "finish_reason": "stop",
                        }
                        for i, s in enumerate(scripts)
                    ],
                    "usage": {
                        "prompt_tokens": _PROMPT_TOKENS,
                        "completion_tokens": done,
                        "total_tokens": _PROMPT_TOKENS + done,
                    },
                }
            )
        return StreamingResponse(
            body_iter(body, scripts),
            media_type="text/event-stream",
            headers={"cache-control": "no-cache"},
        )

    return Starlette(routes=[Route("/v1/chat/completions", completions, methods=["POST"])])


def main() -> None:
    host = os.environ.get("MOCK_HOST", "127.0.0.1")
    try:
        port = int(os.environ.get("MOCK_PORT", "8002"))
        if not 0 < port < 65536:
            raise ValueError("out of range")
    except ValueError:
        sys.stderr.write("MOCK_PORT must be an integer in 1..65535\n")
        raise SystemExit(2) from None
    try:
        delay = float(os.environ.get("MOCK_DELAY", "0"))
        if not delay >= 0 or delay == float("inf"):
            raise ValueError("negative")
    except ValueError:
        sys.stderr.write("MOCK_DELAY must be a finite float >= 0\n")
        raise SystemExit(2) from None
    uvicorn.run(build_mock_app(delay=delay), host=host, port=port, server_header=False)


if __name__ == "__main__":
    main()
