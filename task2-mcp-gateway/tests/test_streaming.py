"""Layer 3: drive the ASGI app directly to prove incremental SSE relay and upstream cleanup."""

from __future__ import annotations

import logging

import anyio
import httpx2
import pytest
from starlette.requests import ClientDisconnect

from mcp_gateway.app import create_app
from mcp_gateway.config import Settings
from tests.helpers import TEST_SECRET, rpc, token

pytestmark = pytest.mark.anyio

EVENT_1 = b'event: message\ndata: {"n": 1}\n\n'
EVENT_2 = b'event: message\ndata: {"n": 2}\n\n'


@pytest.fixture
def anyio_backend():
    return "asyncio"


class GatedStream(httpx2.AsyncByteStream):
    """Yields event 1, waits for ``gate``, then event 2 (or raises ``fail``)."""

    def __init__(self, gate: anyio.Event, fail: Exception | None = None) -> None:
        self.gate = gate
        self.fail = fail
        self.closed = False

    async def __aiter__(self):
        yield EVENT_1
        await self.gate.wait()
        if self.fail is not None:
            raise self.fail
        yield EVENT_2

    async def aclose(self) -> None:
        self.closed = True


def make_app(stream: GatedStream):
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(
            200,
            headers={"content-type": "text/event-stream", "cache-control": "no-cache"},
            stream=stream,
        )

    upstream = httpx2.AsyncClient(
        transport=httpx2.MockTransport(handler), base_url="http://127.0.0.1:8001"
    )
    return create_app(Settings(jwt_secret=TEST_SECRET), upstream_client=upstream)


def make_scope(spec_version: str = "2.4") -> dict:
    headers = [
        (b"authorization", f"Bearer {token('viewer')}".encode()),
        (b"content-type", b"application/json"),
        (b"accept", b"application/json, text/event-stream"),
        (b"host", b"gateway"),
    ]
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": spec_version},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/mcp",
        "raw_path": b"/mcp",
        "query_string": b"",
        "root_path": "",
        "headers": headers,
        "client": ("127.0.0.1", 50000),
        "server": ("gateway", 80),
    }


class Harness:
    """Hand-built ASGI receive/send; ``disconnect`` makes receive report a client hang-up."""

    def __init__(self) -> None:
        self.sent: list[dict] = []
        self.first_chunk = anyio.Event()
        self.disconnect = anyio.Event()
        self._body_sent = False

    async def receive(self) -> dict:
        if not self._body_sent:
            self._body_sent = True
            return {"type": "http.request", "body": rpc("tools/list"), "more_body": False}
        await self.disconnect.wait()
        return {"type": "http.disconnect"}

    async def send(self, message: dict) -> None:
        if self.disconnect.is_set():
            raise OSError("client disconnected")  # what uvicorn does on ASGI spec >= 2.4
        self.sent.append(message)
        if message["type"] == "http.response.body" and message.get("body"):
            self.first_chunk.set()

    def bodies(self) -> list[bytes]:
        return [m.get("body", b"") for m in self.sent if m["type"] == "http.response.body"]


async def run_tolerating_disconnect(app, scope, receive, send) -> None:
    """Starlette raises ClientDisconnect from the app on spec 2.4 hang-ups; that is expected."""
    try:
        await app(scope, receive, send)
    except ClientDisconnect:
        pass


async def test_sse_is_relayed_incrementally():
    gate = anyio.Event()
    stream = GatedStream(gate)
    app = make_app(stream)
    h = Harness()
    with anyio.fail_after(5):
        async with anyio.create_task_group() as tg:
            tg.start_soon(app, make_scope(), h.receive, h.send)
            await h.first_chunk.wait()
            # The first event arrived while upstream is still blocked on the gate.
            assert not gate.is_set()
            assert h.bodies() == [EVENT_1]
            start = next(m for m in h.sent if m["type"] == "http.response.start")
            assert (b"content-type", b"text/event-stream") in start["headers"]
            gate.set()
    assert b"".join(h.bodies()) == EVENT_1 + EVENT_2
    assert h.sent[-1]["type"] == "http.response.body"
    assert h.sent[-1].get("more_body", False) is False
    assert stream.closed


@pytest.mark.parametrize("spec_version", ["2.3", "2.4"])
async def test_client_disconnect_closes_upstream(spec_version):
    # 2.3: Starlette listens for http.disconnect while upstream is blocked.
    # 2.4: the disconnect surfaces as OSError from the next send, once upstream yields again.
    gate = anyio.Event()
    stream = GatedStream(gate)
    app = make_app(stream)
    h = Harness()
    with anyio.fail_after(5):
        async with anyio.create_task_group() as tg:
            tg.start_soon(
                run_tolerating_disconnect, app, make_scope(spec_version), h.receive, h.send
            )
            await h.first_chunk.wait()
            assert not stream.closed
            h.disconnect.set()
            if spec_version == "2.4":
                gate.set()
    assert stream.closed
    assert h.bodies() == [EVENT_1]


async def test_upstream_failure_mid_stream_closes_upstream(caplog):
    gate = anyio.Event()
    stream = GatedStream(gate, fail=httpx2.RemoteProtocolError("peer closed connection"))
    app = make_app(stream)
    h = Harness()
    with caplog.at_level(logging.WARNING, logger="mcp_gateway.proxy"):
        with anyio.fail_after(5):
            async with anyio.create_task_group() as tg:
                tg.start_soon(app, make_scope(), h.receive, h.send)
                await h.first_chunk.wait()
                gate.set()
    assert stream.closed
    assert h.bodies()[0] == EVENT_1
    assert h.sent[-1]["type"] == "http.response.body"
    assert h.sent[-1].get("more_body", False) is False
    warnings = [r for r in caplog.records if r.name == "mcp_gateway.proxy"]
    assert len(warnings) == 1
    assert "data" not in warnings[0].getMessage()
