"""Byte-level, WHATWG-correct SSE parser.

Splits only on raw CR / LF bytes (never U+2028/U+2029/U+0085), tracks the exact
original bytes of every event so unchanged frames can be forwarded verbatim.
"""

import re
from dataclasses import dataclass, replace
from typing import Literal

_EOL = re.compile(rb"[\r\n]")
_BOM = b"\xef\xbb\xbf"


class SSEError(Exception):
    """Raised when a line or pending event exceeds its configured limit."""


@dataclass(frozen=True)
class SSEEvent:
    kind: Literal["data", "comment"]
    event: str
    data: str
    raw: bytes


class SSEParser:
    def __init__(self, max_line: int = 1 << 20, max_event: int = 1 << 20) -> None:
        self._max_line = max_line
        self._max_event = max_event
        self._buf = bytearray()  # partial line (no terminator yet)
        self._raw = bytearray()  # raw bytes of the pending (undispatched) event
        self._data: list[str] = []
        self._event = ""
        self._started = False  # BOM decision made
        self._skip_lf = False  # previous line ended with CR at end of a feed
        self._held: SSEEvent | None = None  # event whose terminator was a trailing CR

    def feed(self, chunk: bytes) -> list[SSEEvent]:
        out: list[SSEEvent] = []
        if not chunk:
            return out
        if not self._started:
            self._buf += chunk
            if len(self._buf) < len(_BOM) and _BOM.startswith(bytes(self._buf)):
                return out  # might still be a BOM
            chunk = bytes(self._buf.removeprefix(_BOM))
            self._buf.clear()
            self._started = True
            if not chunk:
                return out
        start = 0
        if self._skip_lf:
            # An LF right after a feed-final CR belongs to that CRLF terminator.
            self._skip_lf = False
            if chunk[0] == 0x0A:
                if self._held is not None:
                    self._held = replace(self._held, raw=self._held.raw + b"\n")
                elif self._raw:
                    self._raw += b"\n"
                start = 1
        if self._held is not None:
            out.append(self._held)
            self._held = None
        produced = False
        last_end = -1
        while (m := _EOL.search(chunk, start)) is not None:
            end = m.start()
            self._buf += chunk[start:end]
            if len(self._buf) > self._max_line:
                raise SSEError("SSE line exceeds limit")
            line = self._buf.decode("utf-8", "replace")
            raw_line = bytes(self._buf) + chunk[end : end + 1]
            self._buf.clear()
            start = last_end = end + 1
            # A CR followed by LF inside this chunk is one terminator.
            if chunk[end] == 0x0D and chunk[start : start + 1] == b"\n":
                raw_line += b"\n"
                start = last_end = start + 1
            ev = self._line(line, raw_line)
            produced = ev is not None
            if ev is not None:
                out.append(ev)
        if start < len(chunk):
            self._buf += chunk[start:]
            if len(self._buf) > self._max_line:
                raise SSEError("SSE line exceeds limit")
            if len(self._raw) + len(self._buf) > self._max_event:
                raise SSEError("SSE event exceeds limit")
        elif last_end == len(chunk) and chunk[-1] == 0x0D:
            self._skip_lf = True
            if produced:
                self._held = out.pop()
        return out

    def _line(self, line: str, raw_line: bytes) -> SSEEvent | None:
        if line.startswith(":"):
            return SSEEvent("comment", "", "", raw_line)
        self._raw += raw_line
        if len(self._raw) > self._max_event:
            raise SSEError("SSE event exceeds limit")
        if not line:
            if not self._data:
                self._event = ""
                self._raw.clear()
                return None
            ev = SSEEvent("data", self._event, "\n".join(self._data), bytes(self._raw))
            self._data, self._event = [], ""
            self._raw.clear()
            return ev
        name, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]
        if name == "data":
            self._data.append(value)
        elif name == "event":
            self._event = value
        return None

    def close(self) -> list[SSEEvent]:
        out = [self._held] if self._held is not None else []
        self._held = None
        self._buf.clear()
        self._raw.clear()
        self._data, self._event = [], ""
        return out
