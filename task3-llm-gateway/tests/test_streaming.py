"""Layer 3: drive the ASGI app directly to prove incremental relay, cleanup and bounded memory."""

from __future__ import annotations

import logging
import re
import tracemalloc
from collections.abc import Callable

import anyio
import httpx2
import pytest
from starlette.requests import ClientDisconnect

from llm_gateway.app import create_app
from llm_gateway.config import Settings
from llm_gateway.transform import ERROR_FRAME
from tests.helpers import chunk, parse_out, sse

pytestmark = pytest.mark.anyio

REQUEST_BODY = b'{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}'
PATH = "/v1/chat/completions"


@pytest.fixture
def anyio_backend():
    return "asyncio"


class GatedStream(httpx2.AsyncByteStream):
    """Yields frame k, then awaits ``gates[k]`` (if any); optionally raises after the last frame."""

    def __init__(
        self,
        frames: list[bytes],
        gates: list[anyio.Event] | None = None,
        fail: Exception | None = None,
    ) -> None:
        self.frames = frames
        self.gates = gates or []
        self.fail = fail
        self.closed = False

    async def __aiter__(self):
        for k, frame in enumerate(self.frames):
            yield frame
            if k < len(self.gates):
                await self.gates[k].wait()
        if self.fail is not None:
            raise self.fail

    async def aclose(self) -> None:
        self.closed = True


class LazyStream(httpx2.AsyncByteStream):
    """Generates ``total`` bytes of content lazily; never holds more than one frame."""

    def __init__(self, total: int) -> None:
        self.total = total
        self.closed = False

    async def __aiter__(self):
        filler = "lorem ipsum dolor sit amet " * 600  # ~16 KB of safe text
        sent = 0
        n = 0
        while sent < self.total:
            if n % 50 == 0:  # PII split across two chunks: forces a held candidate
                text = filler + "contact jo"
            elif n % 50 == 1:
                text = "hn@example.com now " + filler
            else:
                text = filler
            n += 1
            sent += len(text)
            yield sse(chunk(text))
        yield sse(chunk(None, finish="stop"))
        yield sse("[DONE]")

    async def aclose(self) -> None:
        self.closed = True


def make_app(stream: httpx2.AsyncByteStream):
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(
            200, headers={"content-type": "text/event-stream; charset=utf-8"}, stream=stream
        )

    upstream = httpx2.AsyncClient(
        transport=httpx2.MockTransport(handler), base_url="http://upstream.test"
    )
    return create_app(Settings(), upstream_client=upstream)


def make_scope(spec_version: str = "2.4") -> dict:
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": spec_version},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": PATH,
        "raw_path": PATH.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [(b"content-type", b"application/json"), (b"host", b"testserver")],
        "client": ("127.0.0.1", 1),
        "server": ("testserver", 80),
    }


class Harness:
    """Hand-built ASGI receive/send recording every body chunk."""

    def __init__(self) -> None:
        self.sent: list[dict] = []
        self.disconnect = anyio.Event()
        self._progress = anyio.Event()
        self._body_sent = False

    async def receive(self) -> dict:
        if not self._body_sent:
            self._body_sent = True
            return {"type": "http.request", "body": REQUEST_BODY, "more_body": False}
        await self.disconnect.wait()
        return {"type": "http.disconnect"}

    async def send(self, message: dict) -> None:
        if self.disconnect.is_set():
            raise OSError("client disconnected")  # uvicorn behaviour on ASGI spec >= 2.4
        self.sent.append(message)
        self._progress.set()

    def raw(self) -> bytes:
        return b"".join(m.get("body", b"") for m in self.sent if m["type"] == "http.response.body")

    def text(self) -> str:
        out = ""
        for obj in parse_out(self.raw()):
            if isinstance(obj, dict) and obj.get("choices"):
                out += obj["choices"][0].get("delta", {}).get("content") or ""
        return out

    async def until(self, pred: Callable[[], bool]) -> None:
        while not pred():
            self._progress = anyio.Event()
            await self._progress.wait()


async def run_tolerating_disconnect(app, scope, receive, send) -> None:
    try:
        await app(scope, receive, send)
    except ClientDisconnect:
        pass


async def test_released_text_reaches_client_before_next_upstream_chunk():
    gates = [anyio.Event() for _ in range(3)]
    frames = [sse(chunk(t)) for t in ("Hi ", "there ", "friend ")]
    frames += [sse(chunk(None, finish="stop")), sse("[DONE]")]
    stream = GatedStream(frames, gates)
    h = Harness()
    with anyio.fail_after(10):
        async with anyio.create_task_group() as tg:
            tg.start_soon(make_app(stream), make_scope(), h.receive, h.send)
            for k, expected in enumerate(("Hi ", "Hi there ", "Hi there friend ")):
                await h.until(lambda expected=expected: h.text() == expected)
                assert not gates[k].is_set()  # delivered while upstream is still blocked
                gates[k].set()
    assert h.text() == "Hi there friend "
    assert h.raw().endswith(b"data: [DONE]\n\n")
    assert stream.closed


async def test_held_candidate_released_only_when_final():
    gates = [anyio.Event() for _ in range(3)]
    frames = [sse(chunk(t)) for t in ("mail jo", "hn@x.com ", "ok")]
    frames += [sse(chunk(None, finish="stop")), sse("[DONE]")]
    stream = GatedStream(frames, gates)
    h = Harness()
    with anyio.fail_after(10):
        async with anyio.create_task_group() as tg:
            tg.start_soon(make_app(stream), make_scope(), h.receive, h.send)
            await h.until(lambda: h.text() == "mail ")
            assert "jo" not in h.text()
            assert not gates[0].is_set()
            gates[0].set()
            # The chunk carrying the rest of the email is relayed as an empty-content frame:
            # the candidate stays held (it could still grow) until a following character shows
            # it is final.
            await h.until(lambda: len(h.sent) >= 3)
            assert h.text() == "mail "
            assert b"john" not in h.raw()
            assert not gates[1].is_set()
            gates[1].set()
            await h.until(lambda: "[REDACTED]" in h.text())
            assert b"john" not in h.raw()
            gates[2].set()
    assert h.text() == "mail [REDACTED] ok"
    assert b"john@x.com" not in h.raw()


@pytest.mark.parametrize("spec_version", ["2.3", "2.4"])
async def test_client_disconnect_closes_upstream(spec_version):
    # 2.3: Starlette listens for http.disconnect while upstream is blocked.
    # 2.4: the hang-up surfaces as OSError from the next send, once upstream yields again.
    gates = [anyio.Event()]
    frames = [sse(chunk("Hi ")), sse(chunk("there "))]
    stream = GatedStream(frames, gates)
    h = Harness()
    with anyio.fail_after(10):
        async with anyio.create_task_group() as tg:
            tg.start_soon(
                run_tolerating_disconnect,
                make_app(stream),
                make_scope(spec_version),
                h.receive,
                h.send,
            )
            await h.until(lambda: h.text() == "Hi ")
            assert not stream.closed
            h.disconnect.set()
            if spec_version == "2.4":
                gates[0].set()
    assert stream.closed
    assert h.text() == "Hi "


async def test_upstream_drop_mid_pii_fails_closed():
    stream = GatedStream(
        [sse(chunk("write to "))] + [sse(chunk("john@exa"))],
        fail=httpx2.RemoteProtocolError("peer closed connection"),
    )
    h = Harness()
    with anyio.fail_after(10):
        await make_app(stream)(make_scope(), h.receive, h.send)
    raw = h.raw()
    assert raw.endswith(ERROR_FRAME)
    assert b"[DONE]" not in raw
    assert b"john" not in raw
    assert b"exa" not in raw
    assert h.sent[-1].get("more_body", False) is False
    assert stream.closed


async def _drain_counting(stream: LazyStream, caplog) -> tuple[int, int, int, int]:
    """Run the app with a discarding send; return (bytes, leaks, redactions, peak_traced)."""
    app = make_app(stream)
    counts = {"bytes": 0, "leaks": 0, "redactions": 0}
    window = b""
    done = anyio.Event()

    async def receive() -> dict:
        if not counts.get("req"):
            counts["req"] = 1
            return {"type": "http.request", "body": REQUEST_BODY, "more_body": False}
        await done.wait()
        return {"type": "http.disconnect"}

    async def send(message: dict) -> None:
        nonlocal window
        if message["type"] != "http.response.body":
            return
        body = message.get("body", b"")
        counts["bytes"] += len(body)
        joined = window + body
        counts["leaks"] += joined.count(b"john@example.com") + joined.count(b"example.com")
        counts["redactions"] += joined.count(b"[REDACTED]") - window.count(b"[REDACTED]")
        window = joined[-64:]

    tracemalloc.start()
    try:
        tracemalloc.reset_peak()
        base, _ = tracemalloc.get_traced_memory()
        with caplog.at_level(logging.INFO, logger="llm_gateway"):
            await app(make_scope(), receive, send)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
        done.set()
    return counts["bytes"], counts["leaks"], counts["redactions"], peak - base


@pytest.mark.slow
async def test_memory_bounded(caplog):
    results = {}
    with anyio.fail_after(120):  # ~45 s of tracemalloc-slowed scanning
        for label, total in (("small", 5_000_000), ("large", 50_000_000)):
            caplog.clear()
            stream = LazyStream(total)
            nbytes, leaks, redactions, peak = await _drain_counting(stream, caplog)
            held = [
                int(m.group(1))
                for r in caplog.records
                if (m := re.search(r"max_held=(\d+)", r.getMessage()))
            ]
            results[label] = (nbytes, leaks, redactions, peak, held)
            assert stream.closed
    small, large = results["small"], results["large"]
    assert large[0] > 40_000_000  # the big run really streamed ~50 MB through
    assert small[1] == 0 and large[1] == 0  # no PII literal ever reached the client
    assert small[2] > 0 and large[2] > small[2]  # redaction really happened
    assert abs(large[3] - small[3]) < 1_000_000, f"peaks {small[3]} vs {large[3]}"
    assert small[4] and large[4]
    assert max(small[4] + large[4]) < 100
