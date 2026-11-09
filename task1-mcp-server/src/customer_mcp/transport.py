"""stdio transport: the SDK's stdio_server plus JSON-RPC errors for unparseable input.

SDK 2.3.0's stdio reader turns a line it cannot parse into an exception item on the
read stream, and the dispatcher then drops it, so the client never gets a reply. A thin
filter between the reader and the server answers those items as JSON-RPC 2.0 section 5
asks: -32700 for invalid JSON, -32600 for JSON that is not a valid message, both with
`id: null`. It uses only the SDK's public stream protocol and never logs the raw line.
"""

import logging
from collections.abc import AsyncIterable
from typing import Protocol

import anyio
import mcp_types as types
from anyio.streams.memory import MemoryObjectSendStream
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server
from mcp.shared.message import SessionMessage
from pydantic import ValidationError

log = logging.getLogger("customer_mcp.transport")

Inbound = SessionMessage | Exception


class _MessageSender(Protocol):
    async def send(self, item: SessionMessage, /) -> None: ...


DRAIN_TIMEOUT_S = 5.0  # upper bound on waiting for in-flight replies after stdin closes


class _InFlight:
    """Counts requests handed to the server and replies the server has written back.

    Lets the transport finish answering everything already received when stdin reaches
    EOF (for example ``printf '...' | customer-mcp``) instead of exiting mid-request.
    """

    def __init__(self, write_stream: _MessageSender) -> None:
        self._write = write_stream
        self.requests = 0
        self.replies = 0

    async def __aenter__(self) -> "_InFlight":
        await self._write.__aenter__()
        return self

    async def __aexit__(self, *exc_info) -> bool | None:
        return await self._write.__aexit__(*exc_info)

    def __getattr__(self, name: str):  # aclose(), statistics(), ...: behave as the real stream
        return getattr(self._write, name)

    @property
    def pending(self) -> int:
        return self.requests - self.replies

    async def send(self, item: SessionMessage, /) -> None:
        await self._write.send(item)
        if isinstance(item.message, types.JSONRPCResponse | types.JSONRPCError):
            self.replies += 1

    async def drain(self) -> None:
        with anyio.move_on_after(DRAIN_TIMEOUT_S):
            while self.pending > 0:
                await anyio.sleep(0.005)


def rejection_for(exc: Exception) -> types.ErrorData:
    """Map a transport parse failure to the JSON-RPC 2.0 error it should produce."""
    if isinstance(exc, ValidationError) and any(
        e["type"] == "json_invalid"
        for e in exc.errors(include_url=False, include_input=False, include_context=False)
    ):
        return types.ErrorData(code=types.PARSE_ERROR, message="Parse error")
    return types.ErrorData(code=types.INVALID_REQUEST, message="Invalid Request")


async def _answer_unparseable(
    source: AsyncIterable[Inbound],
    sink: MemoryObjectSendStream[Inbound],
    write_stream: _MessageSender,
    in_flight: _InFlight,
) -> None:
    async with sink:
        async for item in source:
            if isinstance(item, Exception):
                error = rejection_for(item)
                log.info("rejected inbound message code=%d", error.code)
                reply = types.JSONRPCError(jsonrpc="2.0", id=None, error=error)
                await write_stream.send(SessionMessage(reply))
                continue
            if isinstance(item.message, types.JSONRPCRequest):
                in_flight.requests += 1
            await sink.send(item)
        # stdin closed: let requests already received finish before the server shuts down.
        await in_flight.drain()


async def serve_stdio(server: Server) -> None:
    """Serve `server` over stdin/stdout until stdin closes."""
    async with stdio_server() as (sdk_read, write_stream):
        sink, filtered = anyio.create_memory_object_stream[Inbound](0)
        in_flight = _InFlight(write_stream)
        async with anyio.create_task_group() as tg:
            tg.start_soon(_answer_unparseable, sdk_read, sink, write_stream, in_flight)
            await server.run(filtered, in_flight, server.create_initialization_options())
            tg.cancel_scope.cancel()
