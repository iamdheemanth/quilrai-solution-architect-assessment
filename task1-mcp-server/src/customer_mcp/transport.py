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
) -> None:
    async with sink:
        async for item in source:
            if isinstance(item, Exception):
                error = rejection_for(item)
                log.info("rejected inbound message code=%d", error.code)
                reply = types.JSONRPCError(jsonrpc="2.0", id=None, error=error)
                await write_stream.send(SessionMessage(reply))
                continue
            await sink.send(item)


async def serve_stdio(server: Server) -> None:
    """Serve `server` over stdin/stdout until stdin closes."""
    async with stdio_server() as (sdk_read, write_stream):
        sink, filtered = anyio.create_memory_object_stream[Inbound](0)
        async with anyio.create_task_group() as tg:
            tg.start_soon(_answer_unparseable, sdk_read, sink, write_stream)
            await server.run(filtered, write_stream, server.create_initialization_options())
            tg.cancel_scope.cancel()
