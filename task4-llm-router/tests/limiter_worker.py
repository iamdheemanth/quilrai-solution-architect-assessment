"""Module-level worker for the multiprocessing limiter test (spawn must be able to import it)."""

from __future__ import annotations

from llm_router.errors import GatewayRateLimited
from llm_router.limiter import SqliteTokenLimiter


def reserve_worker(path, limit, window_s, now, tenant, worker_id, n, tokens, barrier, results):
    limiter = SqliteTokenLimiter(path, limit, window_s, clock=lambda: now)
    admitted = 0
    try:
        barrier.wait(timeout=30)
        for i in range(n):
            try:
                limiter.reserve(tenant, f"p{worker_id}-{i}", tokens)
                admitted += 1
            except GatewayRateLimited:
                pass
    finally:
        limiter.close()
    results.put(admitted)
