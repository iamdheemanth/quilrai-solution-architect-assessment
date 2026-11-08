"""Shared SSE fixture builders."""

import json

from llm_gateway.sse import SSEEvent, SSEParser

ENV = {"id": "chatcmpl-1", "object": "chat.completion.chunk", "created": 1700000000, "model": "m"}


def chunk(
    content=None,
    *,
    index=0,
    finish=None,
    tool_calls=None,
    refusal=None,
    logprobs=None,
    role=None,
    usage=None,
    choices=None,
    function_call=None,
) -> dict:
    obj = dict(ENV)
    if choices is not None:
        obj["choices"] = choices
    else:
        delta = {}
        if role is not None:
            delta["role"] = role
        if content is not None:
            delta["content"] = content
        if refusal is not None:
            delta["refusal"] = refusal
        if tool_calls is not None:
            delta["tool_calls"] = tool_calls
        if function_call is not None:
            delta["function_call"] = function_call
        obj["choices"] = [
            {"index": index, "delta": delta, "logprobs": logprobs, "finish_reason": finish}
        ]
    if usage is not None:
        obj["usage"] = usage
    return obj


def tool(k, args, **extra) -> dict:
    return {"index": k, "function": {"arguments": args}, **extra}


def sse(obj) -> bytes:
    if obj == "[DONE]":
        return b"data: [DONE]\n\n"
    return b"data: " + json.dumps(obj, separators=(",", ":")).encode() + b"\n\n"


def events(raw: bytes) -> list[SSEEvent]:
    return SSEParser().feed(raw)


def run(tx, *frames: bytes) -> bytes:
    """Feed whole frames through a transformer, returning the joined output."""
    out = b""
    for f in frames:
        for ev in events(f):
            out += tx.on_event(ev)
    return out


def parse_out(raw: bytes) -> list:
    """Parse output bytes into JSON objects (or the string '[DONE]')."""
    return [json.loads(e.data) if e.data != "[DONE]" else "[DONE]" for e in events(raw)]
