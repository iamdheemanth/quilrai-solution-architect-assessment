"""Layer 5 benchmark: TTFT, per-chunk latency, throughput and memory of the redacting gateway.

Run from ``task3-llm-gateway/``: ``uv run python bench/bench.py [--runs N] [--mem-mb 5 50]``.
Starts the mock provider and the gateway under real uvicorn on free ports and prints a
Markdown table plus the platform details the numbers depend on.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import socket
import statistics
import sys
import threading
import time
import tracemalloc

import httpx2
import uvicorn

from llm_gateway.app import create_app
from llm_gateway.config import load_settings
from llm_gateway.sse import SSEParser
from llm_gateway.transform import StreamTransformer
from mock_llm.server import build_mock_app

BODY = {"model": "m", "stream": True, "messages": [{"role": "user", "content": "hi"}]}
PATH = "/v1/chat/completions"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Servers:
    """Real uvicorn servers in daemon threads; stopped and joined on exit."""

    def __init__(self) -> None:
        self.started: list[tuple[uvicorn.Server, threading.Thread]] = []

    def start(self, app) -> int:
        port = free_port()
        server = uvicorn.Server(
            uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", lifespan="on")
        )
        thread = threading.Thread(target=server.run, daemon=True)
        self.started.append((server, thread))
        thread.start()
        deadline = time.monotonic() + 10
        while not server.started:
            if time.monotonic() > deadline or not thread.is_alive():
                raise RuntimeError("uvicorn server did not start")
            time.sleep(0.05)
        return port

    def pair(self, **mock_kwargs) -> tuple[int, int]:
        mock = self.start(build_mock_app(**mock_kwargs))
        settings = load_settings(
            {"UPSTREAM_BASE_URL": f"http://127.0.0.1:{mock}/v1", "UPSTREAM_API_KEY": "sk-bench"}
        )
        return mock, self.start(create_app(settings))

    def __enter__(self) -> Servers:
        return self

    def __exit__(self, *exc) -> None:
        for server, _ in self.started:
            server.should_exit = True
        for _server, thread in reversed(self.started):
            thread.join(timeout=10)


async def stream_times(client: httpx2.AsyncClient, port: int) -> tuple[float, list[float], int]:
    """Return (ttft, arrival times of every non-empty content chunk, content chars)."""
    t0 = time.perf_counter()
    first = None
    arrivals: list[float] = []
    chars = 0
    async with client.stream("POST", f"http://127.0.0.1:{port}{PATH}", json=BODY) as resp:
        resp.raise_for_status()
        async for line in resp.aiter_lines():
            if not line.startswith("data: ") or line.endswith("[DONE]"):
                continue
            obj = json.loads(line[6:])
            for ch in obj.get("choices", []):
                text = ch.get("delta", {}).get("content")
                if text:
                    now = time.perf_counter() - t0
                    first = now if first is None else first
                    arrivals.append(now)
                    chars += len(text)
    if first is None:
        raise RuntimeError("no content received")
    return first, arrivals, chars


def pct(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * len(ordered)))]


async def ttft_median(port: int, runs: int) -> float:
    async with httpx2.AsyncClient(timeout=30) as client:
        await stream_times(client, port)  # warm the connection
        return statistics.median([(await stream_times(client, port))[0] for _ in range(runs)])


async def chunk_gaps(port: int, runs: int) -> list[float]:
    gaps: list[float] = []
    async with httpx2.AsyncClient(timeout=30) as client:
        await stream_times(client, port)
        for _ in range(runs):
            _, arrivals, _ = await stream_times(client, port)
            gaps += [b - a for a, b in zip(arrivals, arrivals[1:], strict=False)]
    return gaps


async def throughput(port: int, runs: int) -> float:
    rates = []
    async with httpx2.AsyncClient(timeout=60) as client:
        await stream_times(client, port)
        for _ in range(runs):
            t0 = time.perf_counter()
            _, _, chars = await stream_times(client, port)
            rates.append(chars / (time.perf_counter() - t0))
    return statistics.median(rates)


def frame(obj: dict | str) -> bytes:
    text = obj if isinstance(obj, str) else json.dumps(obj, separators=(",", ":"))
    return b"data: " + text.encode() + b"\n\n"


def transformer_peak(total_chars: int) -> tuple[int, float]:
    """tracemalloc peak (bytes) of parser + transformer over ``total_chars`` of content."""
    filler = "lorem ipsum dolor sit amet " * 600  # ~16 KB of safe text
    env = {"id": "c", "object": "chat.completion.chunk", "created": 1, "model": "m"}

    def event(text: str | None, finish: str | None = None) -> bytes:
        delta = {} if text is None else {"content": text}
        choice = {"index": 0, "delta": delta, "logprobs": None, "finish_reason": finish}
        return frame({**env, "choices": [choice]})

    parser = SSEParser()
    tx = StreamTransformer()
    out_bytes = 0
    tracemalloc.start()
    try:
        tracemalloc.reset_peak()
        base, _ = tracemalloc.get_traced_memory()
        t0 = time.perf_counter()
        sent = n = 0
        while sent < total_chars:
            text = filler + ("contact jo" if n % 50 == 0 else "")
            if n % 50 == 1:
                text = "hn@example.com now " + filler
            n += 1
            sent += len(text)
            for ev in parser.feed(event(text)):
                out_bytes += len(tx.on_event(ev))
        for ev in parser.feed(event(None, "stop") + frame("[DONE]")):
            out_bytes += len(tx.on_event(ev))
        _, peak = tracemalloc.get_traced_memory()
        elapsed = time.perf_counter() - t0
    finally:
        tracemalloc.stop()
    assert tx.done and out_bytes > 0
    return peak - base, elapsed


def table(rows: list[tuple[str, str, str, str]]) -> str:
    lines = ["| Metric | Direct to mock | Through gateway | Note |", "|---|---|---|---|"]
    lines += [f"| {a} | {b} | {c} | {d} |" for a, b, c, d in rows]
    return "\n".join(lines)


def ms(seconds: float) -> str:
    return f"{seconds * 1000:.2f} ms"


def us(seconds: float) -> str:
    return f"{seconds * 1e6:,.0f} us"


async def run(args: argparse.Namespace) -> list[tuple[str, str, str, str]]:
    rows: list[tuple[str, str, str, str]] = []
    delay = args.delay
    with Servers() as srv:
        mock, gw = srv.pair(tokens=["Hi ", "there ", "friend ", "ok "], delay=delay)
        d, g = await ttft_median(mock, args.runs), await ttft_median(gw, args.runs)
        rows.append(
            (
                f"TTFT median, first token `Hi ` ({args.runs} runs, {delay * 1000:.0f} ms delay)",
                ms(d),
                ms(g),
                f"+{(g - d) * 1000:.2f} ms; released immediately",
            )
        )
        mock, gw = srv.pair(tokens=["Sur", "e. ", "ok ", "fine "], delay=delay)
        d, g = await ttft_median(mock, args.runs), await ttft_median(gw, args.runs)
        rows.append(
            (
                "TTFT median, first token `Sur` (redaction effect)",
                ms(d),
                ms(g),
                f"+{(g - d) * 1000:.2f} ms; one token held for exactness",
            )
        )
        mock, gw = srv.pair(tokens=["word "] * args.chunks, delay=0.0)
        dg = await chunk_gaps(mock, args.runs)
        gg = await chunk_gaps(gw, args.runs)
        rows.append(
            (
                f"Per-chunk inter-arrival p50 / p99 (delay 0, {args.chunks} chunks)",
                f"{us(pct(dg, 0.5))} / {us(pct(dg, 0.99))}",
                f"{us(pct(gg, 0.5))} / {us(pct(gg, 0.99))}",
                f"added p50 {us(pct(gg, 0.5) - pct(dg, 0.5))}, "
                f"p99 {us(pct(gg, 0.99) - pct(dg, 0.99))}",
            )
        )
        mock, gw = srv.pair(tokens=["lorem ipsum dolor sit amet "] * args.chunks, delay=0.0)
        td, tg = await throughput(mock, args.runs), await throughput(gw, args.runs)
        rows.append(
            (
                "Throughput, content chars/s (delay 0)",
                f"{td:,.0f}",
                f"{tg:,.0f}",
                f"{tg / td:.0%} of direct",
            )
        )
    peaks = []
    for mb in args.mem_mb:
        peak, secs = transformer_peak(mb * 1_000_000)
        peaks.append((mb, peak, secs))
    base = peaks[0][1]
    for mb, peak, secs in peaks:
        rows.append(
            (
                f"tracemalloc peak, transformer path, {mb} MB content",
                "-",
                f"{peak / 1e6:.2f} MB",
                f"{peak - base:+,} B vs {peaks[0][0]} MB run ({secs:.1f} s)",
            )
        )
    return rows


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--runs", type=int, default=15, help="runs per latency measurement")
    p.add_argument("--delay", type=float, default=0.03, help="mock inter-token delay (s)")
    p.add_argument("--chunks", type=int, default=400, help="chunks per stream, chunk/throughput")
    p.add_argument("--mem-mb", type=int, nargs="+", default=[5, 50], help="content sizes (MB)")
    args = p.parse_args()
    rows = asyncio.run(run(args))
    loop = type(asyncio.get_event_loop_policy()).__name__
    print(table(rows))  # noqa: T201
    print()  # noqa: T201
    print(f"- OS: {platform.platform()}")  # noqa: T201
    print(f"- Python: {platform.python_implementation()} {platform.python_version()}")  # noqa: T201
    print(f"- Event loop: asyncio ({loop}), uvicorn {uvicorn.__version__}")  # noqa: T201
    print(f"- HTTP: h11 server (uvicorn), httpx2 {httpx2.__version__} client")  # noqa: T201
    print(f"- Runs per latency metric: {args.runs}; loopback only")  # noqa: T201


if __name__ == "__main__":
    sys.exit(main())
