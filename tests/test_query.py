"""Query path: filters, pagination, and bound validation."""

from __future__ import annotations

from datetime import timedelta

import pytest

from logiq.models import Level, Query
from logiq.store import Store

from .conftest import BASE, make_record


def test_filter_by_source_and_level(store: Store):
    store.write_batch([
        make_record(1, source="a", level=Level.INFO),
        make_record(2, source="a", level=Level.ERROR),
        make_record(3, source="b", level=Level.ERROR),
    ])
    page = store.query(Query(source="a", level=Level.ERROR))
    assert [r.record_id for r in page.records] == ["r2"]


def test_time_range_is_half_open(store: Store):
    store.write_batch([make_record(i, offset_minutes=i * 30) for i in range(4)])
    page = store.query(Query(start=BASE, end=BASE + timedelta(minutes=60)))
    # offsets 0 and 30 are in range; 60 is excluded by the half-open end.
    assert {r.record_id for r in page.records} == {"r0", "r1"}


def test_text_filter(store: Store):
    store.write_batch([make_record(1), make_record(2)])
    page = store.query(Query(text="event 1"))
    assert [r.record_id for r in page.records] == ["r1"]


def test_pagination_walks_all_records(store: Store):
    store.write_batch([make_record(i, offset_minutes=i) for i in range(10)])
    seen: list[str] = []
    offset = 0
    while True:
        page = store.query(Query(limit=3, offset=offset))
        seen.extend(r.record_id for r in page.records)
        if page.next_offset is None:
            break
        offset = page.next_offset
    assert len(seen) == 10
    assert len(set(seen)) == 10


def test_query_rejects_inverted_range():
    with pytest.raises(ValueError):
        Query(start=BASE + timedelta(hours=1), end=BASE)


def test_query_rejects_oversized_limit():
    with pytest.raises(ValueError):
        Query(limit=Query.MAX_LIMIT + 1)
