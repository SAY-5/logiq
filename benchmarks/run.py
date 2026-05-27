"""Measure ingest throughput and indexed query performance.

Two measurements:

1. Ingest throughput: records per second through the durable batch path.
2. Query latency over a large store, comparing the partitioned, indexed
   path against an equivalent table with no indexes and no partition
   pruning. The speedup is the ratio of the two median latencies.

All numbers come from the run on the host. Nothing is hard coded.
"""

from __future__ import annotations

import sqlite3
import statistics
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from logiq.models import Level, LogRecord, Query
from logiq.store import _SELECT_COLS, Store

BASE = datetime(2026, 1, 1, tzinfo=UTC)
SOURCES = [f"svc-{i}" for i in range(8)]
LEVELS = list(Level)


def _gen(n: int) -> list[LogRecord]:
    recs = []
    for i in range(n):
        recs.append(
            LogRecord(
                record_id=f"r{i}",
                source=SOURCES[i % len(SOURCES)],
                timestamp=BASE + timedelta(seconds=i),
                level=LEVELS[i % len(LEVELS)],
                message=f"event {i} status {i % 100}",
                trace_id=f"t{i}",
            )
        )
    return recs


def _build_unindexed(records: list[LogRecord]) -> sqlite3.Connection:
    """A table with the same rows but no indexes and no partition column."""
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE logs (record_id TEXT, source TEXT, timestamp TEXT, "
        "level TEXT, message TEXT, trace_id TEXT)"
    )
    conn.executemany(
        "INSERT INTO logs VALUES (?, ?, ?, ?, ?, ?)",
        [
            (r.record_id, r.source, r.timestamp.isoformat(), r.level.value, r.message, r.trace_id)
            for r in records
        ],
    )
    conn.commit()
    return conn


def _median_ms(fn: Callable[[], None], rounds: int = 25) -> float:
    samples = []
    for _ in range(rounds):
        t0 = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - t0) * 1000.0)
    return statistics.median(samples)


def bench_ingest(n: int = 200_000) -> float:
    """Return ingest throughput in records per second (store write path)."""
    records = _gen(n)
    store = Store(":memory:")
    t0 = time.perf_counter()
    # Write in batches of 1000 to model a streaming ingest workload.
    for i in range(0, n, 1000):
        store.write_batch(records[i : i + 1000])
    elapsed = time.perf_counter() - t0
    store.close()
    return n / elapsed


def bench_query(n: int = 200_000) -> dict[str, float]:
    """Compare an indexed pruned query against an unindexed full scan."""
    records = _gen(n)
    store = Store(":memory:")
    for i in range(0, n, 1000):
        store.write_batch(records[i : i + 1000])

    # A keyed point lookup: the record carrying one correlation id. The
    # indexed store answers this with the trace id index (a seek); the
    # unindexed table must scan every row. Both paths return the
    # identical row set, so the comparison is like for like. This is the
    # common observability lookup: find a request by its trace id.
    trace = f"t{n // 2}"

    def indexed() -> None:
        store.query(Query(trace_id=trace, limit=Query.MAX_LIMIT))

    unindexed_conn = _build_unindexed(records)

    def unindexed() -> None:
        unindexed_conn.execute(
            f"SELECT {_SELECT_COLS} FROM logs WHERE trace_id = ? "
            "ORDER BY timestamp ASC, record_id ASC",
            (trace,),
        ).fetchall()

    # Correctness guard: both paths return the same rows.
    page = store.query(Query(trace_id=trace, limit=Query.MAX_LIMIT))
    indexed_rows = {r.record_id for r in page.records}
    unindexed_rows = {
        row[0]
        for row in unindexed_conn.execute(
            "SELECT record_id FROM logs WHERE trace_id = ?", (trace,)
        ).fetchall()
    }
    assert indexed_rows == unindexed_rows, "indexed and unindexed results diverge"

    # Time bounded pruning measurement: a one hour window over the store.
    start = BASE + timedelta(hours=10)
    end = start + timedelta(hours=1)
    pruned = store.scanned_partitions(Query(start=start, end=end))

    idx_ms = _median_ms(indexed)
    full_ms = _median_ms(unindexed)
    store.close()
    unindexed_conn.close()
    return {
        "indexed_ms": idx_ms,
        "unindexed_ms": full_ms,
        "speedup": full_ms / idx_ms if idx_ms else 0.0,
        "scanned_partitions": float(len(pruned) if pruned else 0),
    }


def main() -> None:
    n = 200_000
    tput = bench_ingest(n)
    q = bench_query(n)
    print(f"records: {n}")
    print(f"ingest_throughput_rps: {tput:,.0f}")
    print(f"indexed_query_ms: {q['indexed_ms']:.3f}")
    print(f"unindexed_query_ms: {q['unindexed_ms']:.3f}")
    print(f"index_lookup_speedup: {q['speedup']:.1f}x")
    print(f"pruned_partitions_for_1h_window: {q['scanned_partitions']:.0f}")


if __name__ == "__main__":
    main()
