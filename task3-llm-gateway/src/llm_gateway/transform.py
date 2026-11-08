"""Per-choice SSE chunk transformer with fail-closed terminal handling.

One ``StreamRedactor`` per text channel: ``(i,"content")``, ``(i,"refusal")``,
``(i,"tool",k)`` and ``(i,"function_call")`` (the last two in args mode). Frames whose text is
unchanged are forwarded as the upstream's original bytes; changed frames are re-serialised.
Held text is only ever released by ``close()`` ahead of a finish chunk, ``[DONE]`` or a usage
chunk; ``discard()`` drops it. Content is never logged.
"""

import copy
import json
from collections import Counter

from llm_gateway.patterns import batch_redact
from llm_gateway.redactor import StreamRedactor
from llm_gateway.sse import SSEEvent

ERROR_FRAME = (
    b'data: {"error":{"message":"Upstream stream failed","type":"upstream_error",'
    b'"code":"upstream_error"}}\n\n'
)
_DONE = b"data: [DONE]\n\n"
_ENVELOPE = ("id", "object", "created", "model", "system_fingerprint")


def _frame(obj: dict) -> bytes:
    return b"data: " + json.dumps(obj, ensure_ascii=True, separators=(",", ":")).encode() + b"\n\n"


class StreamTransformer:
    def __init__(self) -> None:
        self.done = False
        self._red: dict[tuple, StreamRedactor] = {}
        self._all: list[StreamRedactor] = []  # every redactor ever made, for stats
        self._seen: set[int] = set()
        self._finished: set[int] = set()
        self._env: dict = {}

    # ------------------------------------------------------------------ helpers

    def _get(self, key: tuple, args: bool) -> StreamRedactor:
        r = self._red.get(key)
        if r is None:
            r = self._red[key] = StreamRedactor(args=args)
            self._all.append(r)
        return r

    def _close_choice(self, i: int, released: dict[tuple, str]) -> None:
        """Close every redactor of choice ``i``, appending tails to ``released``."""
        for key in [k for k in self._red if k[0] == i]:
            tail = self._red.pop(key).close()
            released[key] = released.get(key, "") + tail

    @staticmethod
    def _synthetic_choice(i: int, released: dict[tuple, str]) -> dict | None:
        delta: dict = {}
        for field in ("content", "refusal"):
            if released.get((i, field)):
                delta[field] = released[(i, field)]
        tools = sorted((k[2], t) for k, t in released.items() if k[1] == "tool" and t)
        if tools:
            delta["tool_calls"] = [{"index": k, "function": {"arguments": t}} for k, t in tools]
        if released.get((i, "function_call")):
            delta["function_call"] = {"arguments": released[(i, "function_call")]}
        if not delta:
            return None
        return {"index": i, "delta": delta, "finish_reason": None}

    def _synthetic_chunk(self, env: dict, choices: list[dict]) -> bytes:
        if not choices:
            return b""
        return _frame({**env, "choices": choices})

    def _close_unfinished(self) -> bytes:
        choices = []
        for i in sorted(self._seen):
            if i in self._finished:
                continue
            released: dict[tuple, str] = {}
            self._close_choice(i, released)
            ch = self._synthetic_choice(i, released)
            if ch:
                choices.append(ch)
        return self._synthetic_chunk(self._env, choices)

    # ------------------------------------------------------------------ public API

    def on_event(self, ev: SSEEvent) -> bytes:
        if self.done:
            return b""
        if ev.kind == "comment":
            return b": ping\n\n"
        if ev.data == "[DONE]":
            out = self._close_unfinished() + _DONE
            self.done = True
            return out
        try:
            obj = json.loads(ev.data)
        except ValueError:
            raise ValueError("upstream data frame is not valid JSON") from None
        if not isinstance(obj, dict):
            raise ValueError("upstream data frame is not a JSON object")
        if "error" in obj:
            self.discard()
            err = obj["error"]
            if isinstance(err, dict) and isinstance(err.get("message"), str):
                err["message"] = batch_redact(err["message"])
            elif isinstance(err, str):
                obj["error"] = batch_redact(err)
            return _frame(obj)
        env = {k: obj[k] for k in _ENVELOPE if k in obj}
        self._env = env
        choices = obj.get("choices")
        if not isinstance(choices, list):
            return ev.raw
        if not choices:
            return self._close_unfinished() + ev.raw

        changed = False
        synthetic: list[dict] = []
        for ch in choices:
            if not isinstance(ch, dict):
                raise ValueError("choice is not an object")
            c, syn = self._transform_choice(ch)
            changed = changed or c
            if syn:
                synthetic.append(syn)
        pre = self._synthetic_chunk(env, synthetic)
        return pre + (_frame(obj) if changed else ev.raw)

    def _transform_choice(self, ch: dict) -> tuple[bool, dict | None]:
        i = ch.get("index", 0)
        self._seen.add(i)
        changed = False
        if ch.get("logprobs") is not None:
            ch["logprobs"] = None
            changed = True
        delta = ch.get("delta")
        if not isinstance(delta, dict):
            delta = {}
        finishing = ch.get("finish_reason") is not None
        released: dict[tuple, str] = {}  # finish only: the chunk's own text moves to synthetic

        def run_field(container: dict, name: str, key: tuple, args: bool) -> None:
            nonlocal changed
            text = container.get(name)
            if not isinstance(text, str):
                return
            out = self._get(key, args).feed(text)
            if finishing:
                released[key] = out
                container[name] = ""
                changed = changed or text != ""
            else:
                container[name] = out
                changed = changed or out != text

        run_field(delta, "content", (i, "content"), False)
        run_field(delta, "refusal", (i, "refusal"), False)
        tcs = delta.get("tool_calls")
        if isinstance(tcs, list):
            for pos, tc in enumerate(tcs):
                fn = tc.get("function") if isinstance(tc, dict) else None
                if isinstance(fn, dict):
                    run_field(fn, "arguments", (i, "tool", tc.get("index", pos)), True)
        fc = delta.get("function_call")
        if isinstance(fc, dict):
            run_field(fc, "arguments", (i, "function_call"), True)

        syn = None
        if finishing:
            self._finished.add(i)
            self._close_choice(i, released)
            syn = self._synthetic_choice(i, released)
        return changed, syn

    def on_eof(self) -> bytes:
        if self.done:
            return b""
        if self._seen <= self._finished:
            self.done = True
            return _DONE
        self.discard()
        return ERROR_FRAME

    def discard(self) -> None:
        self._red.clear()
        self.done = True

    def stats(self) -> dict:
        counts: Counter[str] = Counter()
        for r in self._all:
            counts.update(r.counts)
        return {
            "counts": dict(counts),
            "max_held": max((r.max_held for r in self._all), default=0),
        }


def redact_completion(obj: dict) -> dict:
    out = copy.deepcopy(obj)
    for ch in out.get("choices") or ():
        if not isinstance(ch, dict):
            continue
        if "logprobs" in ch:
            ch["logprobs"] = None
        msg = ch.get("message")
        if not isinstance(msg, dict):
            continue
        for name in ("content", "refusal"):
            if isinstance(msg.get(name), str):
                msg[name] = batch_redact(msg[name])
        for tc in msg.get("tool_calls") or ():
            fn = tc.get("function") if isinstance(tc, dict) else None
            if isinstance(fn, dict) and isinstance(fn.get("arguments"), str):
                fn["arguments"] = batch_redact(fn["arguments"], args=True)
        fc = msg.get("function_call")
        if isinstance(fc, dict) and isinstance(fc.get("arguments"), str):
            fc["arguments"] = batch_redact(fc["arguments"], args=True)
    return out


def redact_error_body(obj: dict) -> dict:
    out = copy.deepcopy(obj)
    err = out.get("error")
    if isinstance(err, dict) and isinstance(err.get("message"), str):
        err["message"] = batch_redact(err["message"])
    return out
