"""Partition pruning correctness and indexed lookup performance.

These tests prove the differentiating behaviour: a time bounded query
reads only the partitions overlapping its range, pruning never changes
the result set, and the trace id index materially speeds up a keyed
lookup over a large store.
"""

from __future__ import annotations

import sqlite3
import time
from datetime import timedelta

from logiq.models import Level, LogRecord, Query
from logiq.partition import WINDOW, keys_for_range, partition_key
from logiq.store import Store

from .conftest import BASE


def _spread(n: int, *, span_hours: int) -> list[LogRecord]:
    """n records spread evenly across span_hours of time."""
    step = timedelta(hours=span_hours) / n
    return [
        LogRecord(
            record_id=f"r{i}",
            source=f"svc-{i % 4}",
            timestamp=BASE + step * i,
            level=list(Level)[i % len(Level)],
            message=f"event {i}",
            trace_id=f"t{i}",
        )
        for i in range(n)
    ]


def test_bounded_query_scans_only_overlapping_partitions(store: Store):
    store.write_batch(_spread(2000, span_hours=48))
    # A two hour window should map to exactly two one hour partitions.
    start = BASE + timedelta(hours=10)
    end = start + timedelta(hours=2)
    keys = store.scanned_partitions(Query(start=start, end=end))
    assert keys is not None
    assert len(keys) == 2
    assert keys == [partition_key(start), partition_key(start + WINDOW)]
    # Far fewer than the 48 partitions the full store spans.
    assert store.partition_count() > len(keys)


def test_unbounded_query_reports_full_scan(store: Store):
    store.write_batch(_spread(100, span_hours=10))
    assert store.scanned_partitions(Query(source="svc-0")) is None


def test_pruning_returns_same_records_as_full_scan(store: Store):
    store.write_batch(_spread(3000, span_hours=72))
    start = BASE + timedelta(hours=20)
    end = start + timedelta(hours=5)
    q = Query(start=start, end=end, limit=Query.MAX_LIMIT)
    pruned = [r.record_id for r in store.query(q).records]
    full = [r.record_id for r in store.full_scan(q)]
    assert pruned == full
    # And the pruned query genuinely touched fewer partitions than exist.
    keys = store.scanned_partitions(q)
    assert keys is not None and len(keys) < store.partition_count()


def test_index_speeds_up_keyed_lookup(store: Store):
    records = _spread(50_000, span_hours=200)
    store.write_batch(records)

    # Indexed lookup of one trace id through the store.
    target = f"t{len(records) // 2}"
    t0 = time.perf_counter()
    for _ in range(50):
        store.query(Query(trace_id=target, limit=Query.MAX_LIMIT))
    indexed = time.perf_counter() - t0

    # Same rows from an equivalent table with no indexes.
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE logs (record_id TEXT, trace_id TEXT)"
    )
    conn.executemany(
        "INSERT INTO logs VALUES (?, ?)",
        [(r.record_id, r.trace_id) for r in records],
    )
    conn.commit()
    t0 = time.perf_counter()
    for _ in range(50):
        conn.execute("SELECT record_id FROM logs WHERE trace_id = ?", (target,)).fetchall()
    unindexed = time.perf_counter() - t0
    conn.close()

    # The index should give a large, unambiguous speedup. The threshold
    # is deliberately well below the observed ratio so the test is not
    # flaky on slower machines.
    assert indexed * 5 < unindexed


def test_keys_for_range_is_inclusive_of_boundary_partition():
    start = BASE
    end = BASE + timedelta(minutes=30)
    # A sub hour range still maps to its single containing partition.
    assert keys_for_range(start, end) == [partition_key(start)]
