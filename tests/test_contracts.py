"""Contract properties the ingest and query paths must always hold.

These build random records and queries and assert invariants rather than
fixed outputs: every acknowledged ingest is queryable, a query returns
exactly the matching records, pruning never drops in-range records, and
pagination is stable and complete.
"""

from __future__ import annotations

import random
from datetime import timedelta

from logiq.ingest import Ingestor
from logiq.models import Level, LogRecord, Query
from logiq.store import Store
from logiq.wal import WriteAheadLog

from .conftest import BASE

LEVELS = list(Level)
SOURCES = ["svc-a", "svc-b", "svc-c"]


def _random_records(n: int, seed: int) -> list[LogRecord]:
    rng = random.Random(seed)
    recs = []
    for i in range(n):
        recs.append(
            LogRecord(
                record_id=f"r{i}",
                source=rng.choice(SOURCES),
                timestamp=BASE + timedelta(minutes=rng.randint(0, 600)),
                level=rng.choice(LEVELS),
                message=f"event {i} code {rng.randint(0, 9)}",
                trace_id=rng.choice([None, "tA", "tB"]),
            )
        )
    return recs


def _all_pages(store: Store, q: Query) -> list[LogRecord]:
    out: list[LogRecord] = []
    offset = q.offset
    while True:
        page = store.query(
            Query(
                start=q.start, end=q.end, source=q.source, level=q.level,
                trace_id=q.trace_id, text=q.text, limit=q.limit, offset=offset,
            )
        )
        out.extend(page.records)
        if page.next_offset is None:
            break
        offset = page.next_offset
    return out


def test_every_acknowledged_record_is_queryable(store: Store, tmp_path):
    wal = WriteAheadLog(str(tmp_path / "c.wal"))
    ing = Ingestor(store, wal)
    recs = _random_records(200, seed=1)
    ing.ingest(recs)
    got = _all_pages(store, Query(limit=50))
    assert {r.record_id for r in got} == {r.record_id for r in recs}


def test_query_returns_exactly_matching_records(store: Store):
    recs = _random_records(300, seed=2)
    store.write_batch(recs)
    for seed in range(5):
        rng = random.Random(100 + seed)
        source = rng.choice(SOURCES)
        level = rng.choice(LEVELS)
        expected = {
            r.record_id for r in recs if r.source == source and r.level == level
        }
        got = {r.record_id for r in _all_pages(store, Query(source=source, level=level, limit=40))}
        assert got == expected


def test_pruning_never_drops_in_range_records(store: Store):
    recs = _random_records(400, seed=3)
    store.write_batch(recs)
    for seed in range(5):
        rng = random.Random(200 + seed)
        start = BASE + timedelta(minutes=rng.randint(0, 300))
        end = start + timedelta(minutes=rng.randint(30, 300))
        pruned = {r.record_id for r in _all_pages(store, Query(start=start, end=end, limit=40))}
        full = {r.record_id for r in store.full_scan(Query(start=start, end=end, limit=40))}
        assert pruned == full


def test_pagination_is_stable_and_complete(store: Store):
    recs = _random_records(250, seed=4)
    store.write_batch(recs)
    walked = [r.record_id for r in _all_pages(store, Query(limit=17))]
    # Complete: every record once.
    assert sorted(walked) == sorted(r.record_id for r in recs)
    assert len(walked) == len(set(walked))
    # Stable: a re-walk yields the identical order.
    again = [r.record_id for r in _all_pages(store, Query(limit=17))]
    assert walked == again
