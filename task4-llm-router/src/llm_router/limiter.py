"""Exact token-aware sliding-window rate limiter on on-disk SQLite.

Sliding log: every admitted request is one row ``(tenant, ts_ms, tokens, request_id)``.
The window is ``(now - W, now]``. A reservation runs in one ``BEGIN IMMEDIATE`` transaction:

1. delete this tenant's rows with ``ts_ms <= now - W`` (eviction),
2. sum the remaining tokens,
3. admit iff ``sum + tokens <= limit`` and insert the row.

``BEGIN IMMEDIATE`` takes SQLite's single RESERVED write lock *before* the read, so the
check-then-insert is serialized across threads, connections and processes sharing the file.
A deferred transaction would let two writers read the same sum and both insert (over-admission).

``reconcile`` and ``refund`` are single-statement UPDATEs, atomic under autocommit.
The tenant passed in is already an HMAC hex digest; raw API keys never reach this module.
"""

from __future__ import annotations

import logging
import math
import sqlite3
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import anyio
import anyio.to_thread

from .errors import GatewayRateLimited, RateLimiterUnavailable, RequestExceedsRateLimit

logger = logging.getLogger("llm_router")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS usage(
    id INTEGER PRIMARY KEY,
    tenant TEXT NOT NULL,
    ts_ms INTEGER NOT NULL,
    tokens INTEGER NOT NULL CHECK (tokens >= 0),
    request_id TEXT NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS ix_usage_tenant_ts ON usage(tenant, ts_ms);
"""


@dataclass(frozen=True)
class Reservation:
    request_id: str
    tokens: int
    limit: int
    remaining: int
    reset_s: int


class SqliteTokenLimiter:
    """Synchronous core. Thread-safe: one connection guarded by a lock."""

    def __init__(
        self,
        path: str,
        limit: int,
        window_s: int,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.limit = limit
        self.window_s = window_s
        self._window_ms = window_s * 1000
        self._clock = clock
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, autocommit=True, check_same_thread=False, timeout=5)
        try:
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.executescript(_SCHEMA)
        except BaseException:
            self._conn.close()
            raise

    def _now_ms(self) -> int:
        # round, not int(): float seconds*1000 can land a hair below the true millisecond.
        return round(self._clock() * 1000)

    def _ceil_s(self, ms: int) -> int:
        return max(1, math.ceil(ms / 1000))

    def reserve(self, tenant: str, request_id: str, tokens: int) -> Reservation:
        if tokens < 0:
            raise ValueError("tokens must be >= 0")
        if tokens > self.limit:
            raise RequestExceedsRateLimit()
        with self._lock:
            c = self._conn
            c.execute("BEGIN IMMEDIATE")
            try:
                # Sample the clock only once the write lock is held: a request that waited on
                # the lock or busy_timeout is evicted and timestamped at its admission time.
                now_ms = self._now_ms()
                cutoff = now_ms - self._window_ms
                c.execute("DELETE FROM usage WHERE tenant=? AND ts_ms<=?", (tenant, cutoff))
                (used,) = c.execute(
                    "SELECT COALESCE(SUM(tokens), 0) FROM usage WHERE tenant=?", (tenant,)
                ).fetchone()
                if used + tokens > self.limit:
                    retry_after = self._retry_after(tenant, used, tokens, now_ms)
                    c.execute("COMMIT")  # keep the eviction
                    raise GatewayRateLimited(retry_after)
                c.execute(
                    "INSERT INTO usage(tenant, ts_ms, tokens, request_id) VALUES (?, ?, ?, ?)",
                    (tenant, now_ms, tokens, request_id),
                )
                (oldest,) = c.execute(
                    "SELECT MIN(ts_ms) FROM usage WHERE tenant=?", (tenant,)
                ).fetchone()
                c.execute("COMMIT")
            except BaseException:
                if c.in_transaction:
                    try:
                        c.execute("ROLLBACK")
                    except sqlite3.Error:
                        logger.warning("rate limiter rollback failed", exc_info=True)
                raise  # always the original error, never the rollback's
        return Reservation(
            request_id=request_id,
            tokens=tokens,
            limit=self.limit,
            remaining=self.limit - (used + tokens),
            reset_s=self._ceil_s(oldest + self._window_ms - now_ms),
        )

    def _retry_after(self, tenant: str, used: int, tokens: int, now_ms: int) -> int:
        """Seconds until enough of the oldest rows age out for this request to fit."""
        freed = 0
        cur = self._conn.execute(
            "SELECT ts_ms, tokens FROM usage WHERE tenant=? ORDER BY ts_ms, id", (tenant,)
        )
        for ts_ms, row_tokens in cur:
            freed += row_tokens
            if used - freed + tokens <= self.limit:
                return self._ceil_s(ts_ms + self._window_ms - now_ms)
        return self.window_s  # unreachable while tokens <= limit; safe upper bound

    def reconcile(self, request_id: str, actual_tokens: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE usage SET tokens=? WHERE request_id=?",
                (max(0, int(actual_tokens)), request_id),
            )

    def refund(self, request_id: str) -> None:
        with self._lock:
            self._conn.execute("UPDATE usage SET tokens=0 WHERE request_id=?", (request_id,))

    def close(self) -> None:
        with self._lock:
            self._conn.close()


class AsyncLimiter:
    """Async facade: runs the sync core in a worker thread, one call at a time.

    ``sqlite3.Error`` is converted to ``RateLimiterUnavailable`` (fail closed) with the
    detail logged server-side only. ``reconcile``/``refund`` are shielded from cancellation so
    a client disconnect can never leave the ledger wrong.
    """

    def __init__(self, limiter: Any) -> None:
        self._limiter = limiter
        self._cap: anyio.CapacityLimiter | None = None

    def _capacity(self) -> anyio.CapacityLimiter:
        if self._cap is None:  # created lazily inside the running event loop
            self._cap = anyio.CapacityLimiter(1)
        return self._cap

    async def _run(self, fn: Callable[..., Any], *args: Any) -> Any:
        try:
            return await anyio.to_thread.run_sync(fn, *args, limiter=self._capacity())
        except sqlite3.Error:
            logger.warning("rate limiter storage error", exc_info=True)
            raise RateLimiterUnavailable() from None

    async def reserve(self, tenant: str, request_id: str, tokens: int) -> Reservation:
        return await self._run(self._limiter.reserve, tenant, request_id, tokens)

    async def reconcile(self, request_id: str, actual_tokens: int) -> None:
        with anyio.CancelScope(shield=True):
            await self._run(self._limiter.reconcile, request_id, actual_tokens)

    async def refund(self, request_id: str) -> None:
        with anyio.CancelScope(shield=True):
            await self._run(self._limiter.refund, request_id)

    def close(self) -> None:
        """Close the underlying connection (called at app shutdown, after requests drain)."""
        self._limiter.close()
