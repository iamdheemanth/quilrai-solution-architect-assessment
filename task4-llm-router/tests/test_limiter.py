import logging
import multiprocessing
import sqlite3
import threading

import anyio
import pytest

from llm_router.auth import tenant_id
from llm_router.errors import GatewayRateLimited, RateLimiterUnavailable, RequestExceedsRateLimit
from llm_router.limiter import AsyncLimiter, SqliteTokenLimiter
from tests.limiter_worker import reserve_worker

LIMIT = 50_000
WINDOW = 60
T0_MS = 1_700_000_000_000


class FakeClock:
    """Injected clock with exact integer-millisecond control."""

    def __init__(self, ms: int = T0_MS) -> None:
        self.ms = ms

    def __call__(self) -> float:
        return self.ms / 1000

    def advance(self, ms: int) -> None:
        self.ms += ms


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "limiter.db")


@pytest.fixture
def lim(db_path, clock):
    limiter = SqliteTokenLimiter(db_path, LIMIT, WINDOW, clock=clock)
    yield limiter
    limiter.close()


def rows(path, sql, args=()):
    with sqlite3.connect(path) as c:
        out = c.execute(sql, args).fetchall()
    c.close()
    return out


# ---------------------------------------------------------------- exact accounting


def test_exact_limit_admits_up_to_50000_then_rejects_next_token(lim):
    r1 = lim.reserve("t", "a", 30_000)
    assert r1.remaining == 20_000
    r2 = lim.reserve("t", "b", 20_000)
    assert r2.remaining == 0
    with pytest.raises(GatewayRateLimited) as ei:
        lim.reserve("t", "c", 1)
    assert ei.value.retry_after == 60


def test_reservation_reset_s_full_window_for_first_row_then_oldest_row(lim, clock):
    assert lim.reserve("t", "a", 100).reset_s == 60
    clock.advance(15_500)
    r = lim.reserve("t", "b", 100)
    assert r.remaining == LIMIT - 200
    assert r.reset_s == 45  # oldest row expires in 44.5 s -> ceil 45


def test_window_boundary_59999_rejected_60000_admitted(lim, clock):
    lim.reserve("t", "a", LIMIT)
    clock.advance(59_999)
    with pytest.raises(GatewayRateLimited) as ei:
        lim.reserve("t", "b", 1)
    assert ei.value.retry_after == 1  # 1 ms left -> ceil, min 1
    clock.advance(1)
    r = lim.reserve("t", "c", 1)
    assert r.remaining == LIMIT - 1


def test_window_slides_rather_than_fixed_reset(lim, clock):
    lim.reserve("t", "a", 30_000)
    clock.advance(30_000)
    lim.reserve("t", "b", 20_000)
    clock.advance(30_000)  # t0+60s: "a" ages out, "b" (t0+30s) still counts
    with pytest.raises(GatewayRateLimited):
        lim.reserve("t", "c", 30_001)  # a fixed window would have reset to 0 used
    r = lim.reserve("t", "d", 30_000)
    assert r.remaining == 0


def test_retry_after_is_time_until_enough_old_rows_age_out(lim, clock):
    lim.reserve("t", "a", 20_000)  # t0
    clock.advance(10_000)
    lim.reserve("t", "b", 20_000)  # t0+10s
    clock.advance(10_000)
    lim.reserve("t", "c", 10_000)  # t0+20s -> full
    clock.advance(5_500)  # now t0+25.5s
    with pytest.raises(GatewayRateLimited) as ei:
        lim.reserve("t", "d", 25_000)  # needs a+b freed -> b expires at t0+70s
    assert ei.value.retry_after == 45  # 44.5 s -> 45
    with pytest.raises(GatewayRateLimited) as ei:
        lim.reserve("t", "d", 15_000)  # only a needs freeing -> t0+60s
    assert ei.value.retry_after == 35  # 34.5 s -> 35
    clock.advance(44_499)  # t0+69.999s: still rejected
    with pytest.raises(GatewayRateLimited) as ei:
        lim.reserve("t", "d", 25_000)
    assert ei.value.retry_after == 1
    clock.advance(1)  # t0+70s: admitted, exactly as promised
    assert lim.reserve("t", "d", 25_000).remaining == 50_000 - 10_000 - 25_000


def test_rejected_request_writes_no_row(lim, db_path):
    lim.reserve("t", "a", LIMIT)
    with pytest.raises(GatewayRateLimited):
        lim.reserve("t", "b", 1)
    assert rows(db_path, "SELECT request_id FROM usage") == [("a",)]


# ---------------------------------------------------------------- reconcile / refund


def test_reconcile_down_frees_tokens(lim, db_path):
    lim.reserve("t", "a", 40_000)
    lim.reconcile("a", 15_000)
    assert rows(db_path, "SELECT tokens FROM usage WHERE request_id='a'") == [(15_000,)]
    assert lim.reserve("t", "b", 35_000).remaining == 0
    with pytest.raises(GatewayRateLimited):
        lim.reserve("t", "c", 1)


def test_reconcile_up_consumes_more(lim):
    lim.reserve("t", "a", 10_000)
    lim.reconcile("a", 30_000)
    with pytest.raises(GatewayRateLimited):
        lim.reserve("t", "b", 20_001)
    assert lim.reserve("t", "b", 20_000).remaining == 0


def test_reconcile_negative_clamps_to_zero_and_unknown_is_noop(lim, db_path):
    lim.reserve("t", "a", 10_000)
    lim.reconcile("a", -5)
    lim.reconcile("missing", 999)
    lim.refund("missing")
    assert rows(db_path, "SELECT request_id, tokens FROM usage") == [("a", 0)]


def test_refund_frees_tokens(lim, db_path):
    lim.reserve("t", "a", LIMIT)
    with pytest.raises(GatewayRateLimited):
        lim.reserve("t", "b", 1)
    lim.refund("a")
    assert rows(db_path, "SELECT tokens FROM usage WHERE request_id='a'") == [(0,)]
    assert lim.reserve("t", "b", LIMIT).remaining == 0


# ---------------------------------------------------------------- validation, isolation, storage


def test_oversized_request_raises_and_writes_no_row(lim, db_path):
    with pytest.raises(RequestExceedsRateLimit):
        lim.reserve("t", "big", LIMIT + 1)
    assert rows(db_path, "SELECT COUNT(*) FROM usage") == [(0,)]
    assert lim.reserve("t", "exact", LIMIT).remaining == 0


def test_tenant_isolation(lim):
    lim.reserve("tenant-a", "a1", LIMIT)
    with pytest.raises(GatewayRateLimited):
        lim.reserve("tenant-a", "a2", 1)
    assert lim.reserve("tenant-b", "b1", LIMIT).remaining == 0


def test_restart_persistence_same_file(db_path, clock):
    first = SqliteTokenLimiter(db_path, LIMIT, WINDOW, clock=clock)
    first.reserve("t", "a", 45_000)
    first.close()
    second = SqliteTokenLimiter(db_path, LIMIT, WINDOW, clock=clock)
    try:
        with pytest.raises(GatewayRateLimited):
            second.reserve("t", "b", 5_001)
        assert second.reserve("t", "b", 5_000).remaining == 0
    finally:
        second.close()


def test_raw_key_never_in_db_file(db_path, clock):
    raw_key = "sk-tenant-RAW-SECRET-7f3a9c"
    tenant = tenant_id("pepper-0123456789abcdef", raw_key)
    limiter = SqliteTokenLimiter(db_path, LIMIT, WINDOW, clock=clock)
    try:
        limiter.reserve(tenant, "r1", 123)
        data = b""
        for suffix in ("", "-wal"):
            try:
                with open(db_path + suffix, "rb") as f:
                    data += f.read()
            except FileNotFoundError:
                pass
        assert tenant.encode() in data  # the scan really sees stored rows
        assert raw_key.encode() not in data
    finally:
        limiter.close()


def test_eviction_deletes_old_rows_for_that_tenant_only(lim, clock, db_path):
    lim.reserve("a", "a1", 100)
    lim.reserve("b", "b1", 100)
    clock.advance(30_000)
    lim.reserve("a", "a2", 100)
    clock.advance(30_000)  # a1 and b1 are now out of the window
    lim.reserve("a", "a3", 100)
    got = rows(db_path, "SELECT request_id FROM usage ORDER BY id")
    assert got == [("b1",), ("a2",), ("a3",)]  # a1 deleted; b1 waits for b's next reserve
    lim.reserve("b", "b2", 100)
    got = rows(db_path, "SELECT request_id FROM usage ORDER BY id")
    assert got == [("a2",), ("a3",), ("b2",)]


# ---------------------------------------------------------------- clock sampled under the lock


def test_clock_is_read_only_inside_the_write_transaction(db_path):
    """Regression: now_ms must be sampled after BEGIN IMMEDIATE, not before waiting for it."""
    holder = {}
    reads = []

    def clock():
        reads.append(holder["lim"]._conn.in_transaction)
        return T0_MS / 1000

    limiter = SqliteTokenLimiter(db_path, LIMIT, WINDOW, clock=clock)
    holder["lim"] = limiter
    try:
        limiter.reserve("t", "a", LIMIT)  # admitted path
        with pytest.raises(GatewayRateLimited):
            limiter.reserve("t", "b", 1)  # rejected path
    finally:
        limiter.close()
    assert reads == [True, True]


def test_waiter_is_timestamped_at_admission_not_at_arrival(db_path):
    """A reserve blocked on another connection's write lock uses the post-wait time."""
    clock = FakeClock()
    limiter = SqliteTokenLimiter(db_path, LIMIT, WINDOW, clock=clock)
    other = sqlite3.connect(db_path, autocommit=True, timeout=5)
    errors = []

    def run():
        try:
            limiter.reserve("t", "late", 100)
        except Exception as exc:  # surfaced below
            errors.append(exc)

    worker = threading.Thread(target=run)
    try:
        other.execute("BEGIN IMMEDIATE")  # hold the write lock
        worker.start()
        worker.join(timeout=0.3)  # worker is now blocked on BEGIN IMMEDIATE
        assert worker.is_alive()
        clock.advance(4_000)  # time passes while it waits
        other.execute("COMMIT")
        worker.join(timeout=10)
        assert not worker.is_alive()
    finally:
        other.close()
        limiter.close()
    assert errors == []
    assert rows(db_path, "SELECT ts_ms FROM usage WHERE request_id='late'") == [(T0_MS + 4_000,)]


class _FailOnInsert:
    """Connection proxy that raises a sqlite3.Error on INSERT, mid-transaction."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, *args):
        if sql.lstrip().upper().startswith("INSERT"):
            raise sqlite3.OperationalError("disk I/O error")
        return self._conn.execute(sql, *args)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def test_error_mid_transaction_rolls_back_and_connection_stays_usable(lim, db_path, clock):
    lim.reserve("t", "old", 100)
    clock.advance(60_000)  # "old" is evictable; the failed tx's DELETE must be rolled back
    real = lim._conn
    lim._conn = _FailOnInsert(real)
    try:
        with pytest.raises(sqlite3.OperationalError):
            lim.reserve("t", "x", 100)
    finally:
        lim._conn = real
    assert real.in_transaction is False
    assert rows(db_path, "SELECT request_id FROM usage") == [("old",)]  # DELETE rolled back
    assert lim.reserve("t", "y", 100).remaining == LIMIT - 100


# ---------------------------------------------------------------- concurrency


def test_threads_32x20_reserves_of_700_admit_exactly_71(db_path):
    now = T0_MS / 1000
    n_threads, per_thread, est = 32, 20, 700
    limiters = [
        SqliteTokenLimiter(db_path, LIMIT, WINDOW, clock=lambda: now) for _ in range(n_threads)
    ]
    barrier = threading.Barrier(n_threads)
    counts = [0] * n_threads
    errors = []

    def run(idx):
        try:
            barrier.wait(timeout=30)
            for i in range(per_thread):
                try:
                    limiters[idx].reserve("t", f"th{idx}-{i}", est)
                    counts[idx] += 1
                except GatewayRateLimited:
                    pass
        except Exception as exc:  # surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=run, args=(i,)) for i in range(n_threads)]
    try:
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        assert not any(t.is_alive() for t in threads)
    finally:
        for limiter in limiters:
            limiter.close()
    assert errors == []
    assert sum(counts) == LIMIT // est == 71
    assert rows(db_path, "SELECT COUNT(*), SUM(tokens) FROM usage") == [(71, 71 * 700)]


def test_processes_admit_exactly_floor_limit_over_est(db_path):
    now = T0_MS / 1000
    n_procs, per_proc, est = 6, 20, 700
    SqliteTokenLimiter(db_path, LIMIT, WINDOW, clock=lambda: now).close()  # create schema
    ctx = multiprocessing.get_context("spawn")
    barrier = ctx.Barrier(n_procs)
    results = ctx.Queue()
    procs = [
        ctx.Process(
            target=reserve_worker,
            args=(db_path, LIMIT, WINDOW, now, "t", w, per_proc, est, barrier, results),
        )
        for w in range(n_procs)
    ]
    try:
        for p in procs:
            p.start()
        counts = [results.get(timeout=60) for _ in procs]
        for p in procs:
            p.join(timeout=30)
        assert [p.exitcode for p in procs] == [0] * n_procs
    finally:
        for p in procs:
            if p.is_alive():
                p.terminate()
                p.join(timeout=5)
        results.close()
        results.join_thread()
    assert sum(counts) == LIMIT // est == 71
    assert rows(db_path, "SELECT COUNT(*), SUM(tokens) FROM usage") == [(71, 71 * 700)]


def test_async_tasks_admit_exactly_floor_limit_over_est(db_path):
    # Proves the async facade under many concurrent tasks. CapacityLimiter(1) already serializes
    # the calls, so DB atomicity itself is proven by the thread and process tests above.
    now = T0_MS / 1000
    n_tasks, est = 200, 700
    sync = SqliteTokenLimiter(db_path, LIMIT, WINDOW, clock=lambda: now)
    limiter = AsyncLimiter(sync)
    admitted = []
    rejected = []

    async def one(i):
        try:
            await limiter.reserve("t", f"as{i}", est)
            admitted.append(i)
        except GatewayRateLimited:
            rejected.append(i)

    async def main():
        async with anyio.create_task_group() as tg:
            for i in range(n_tasks):
                tg.start_soon(one, i)

    try:
        anyio.run(main)
    finally:
        sync.close()
    assert len(admitted) == LIMIT // est == 71
    assert len(rejected) == n_tasks - 71
    assert rows(db_path, "SELECT COUNT(*) FROM usage") == [(71,)]


def test_async_reconcile_and_refund_pass_through(db_path, clock):
    sync = SqliteTokenLimiter(db_path, LIMIT, WINDOW, clock=clock)
    limiter = AsyncLimiter(sync)

    async def main():
        res = await limiter.reserve("t", "a", 40_000)
        assert res.remaining == 10_000
        await limiter.reconcile("a", 1_000)
        await limiter.reserve("t", "b", 49_000)
        await limiter.refund("b")
        return await limiter.reserve("t", "c", 49_000)

    try:
        assert anyio.run(main).remaining == 0
    finally:
        sync.close()


def test_async_refund_completes_even_when_caller_is_cancelled(db_path, clock):
    sync = SqliteTokenLimiter(db_path, LIMIT, WINDOW, clock=clock)
    limiter = AsyncLimiter(sync)

    async def main():
        await limiter.reserve("t", "a", LIMIT)
        with anyio.CancelScope() as scope:
            scope.cancel()  # already cancelled when refund starts
            await limiter.refund("a")
        assert scope.cancelled_caught is False  # shield let the refund run to completion
        with anyio.CancelScope() as scope:
            scope.cancel()
            await limiter.reconcile("a", 7)
        await anyio.sleep(0)

    try:
        anyio.run(main)
    finally:
        sync.close()
    assert rows(db_path, "SELECT tokens FROM usage WHERE request_id='a'") == [(7,)]


# ---------------------------------------------------------------- fail closed


def test_sqlite_error_becomes_rate_limiter_unavailable_without_path(db_path, clock, caplog):
    sync = SqliteTokenLimiter(db_path, LIMIT, WINDOW, clock=clock)
    limiter = AsyncLimiter(sync)
    sync.close()  # any later use raises sqlite3.ProgrammingError (a sqlite3.Error)

    async def main():
        with pytest.raises(RateLimiterUnavailable) as ei:
            await limiter.reserve("t", "a", 1)
        return ei.value

    with caplog.at_level(logging.WARNING, logger="llm_router"):
        err = anyio.run(main)
    assert err.__cause__ is None and err.__suppress_context__ is True
    assert str(err) == ""
    assert db_path not in err.message and "limiter.db" not in err.message
    assert err.status == 503 and err.code == "rate_limiter_unavailable"
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1 and warnings[0].exc_info is not None


def test_operational_error_with_path_in_text_is_sanitized(db_path):
    class Broken:
        def reserve(self, *a):
            raise sqlite3.OperationalError(f"unable to open database file {db_path}")

    limiter = AsyncLimiter(Broken())

    async def main():
        with pytest.raises(RateLimiterUnavailable) as ei:
            await limiter.reserve("t", "a", 1)
        return ei.value

    err = anyio.run(main)
    assert db_path not in err.message and str(err) == ""
    assert err.__cause__ is None
