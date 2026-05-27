"""Crash recovery durability: replay is exact, lossless, and idempotent.

Each test injects a crash at a chosen point, then restarts with a fresh
store against the same write-ahead log and asserts the store ends in the
exact expected state: no acknowledged record is lost and no record is
duplicated.
"""

from __future__ import annotations

from logiq.ingest import Ingestor
from logiq.models import Query
from logiq.store import Store
from logiq.wal import WriteAheadLog

from .conftest import make_record


def _all() -> Query:
    return Query(limit=Query.MAX_LIMIT)


def _restart(wal_path: str) -> tuple[Store, Ingestor]:
    store = Store(":memory:")
    ing = Ingestor(store, WriteAheadLog(wal_path))
    return store, ing


def test_crash_before_commit_replays_whole_batch(tmp_path):
    wal_path = str(tmp_path / "w.wal")
    store = Store(":memory:")
    ing = Ingestor(store, WriteAheadLog(wal_path))
    batch = [make_record(i) for i in range(6)]
    ing.ingest(batch, crash_point="after_wal")
    assert store.count() == 0  # nothing reached the store

    store2, ing2 = _restart(wal_path)
    report = ing2.recover()
    assert report.replayed_records == 6
    assert report.newly_committed_records == 6
    assert store2.count() == 6
    ids = {r.record_id for r in store2.query(_all()).records}
    assert ids == {f"r{i}" for i in range(6)}


def test_partial_commit_recovers_without_duplication(tmp_path):
    wal_path = str(tmp_path / "w.wal")
    store = Store(":memory:")
    ing = Ingestor(store, WriteAheadLog(wal_path))
    batch = [make_record(i) for i in range(10)]
    # Crash after half the rows landed but before the commit marker.
    ing.ingest(batch, crash_point="partial_commit")
    assert store.count() == 5

    store2, ing2 = _restart(wal_path)
    report = ing2.recover()
    # The whole batch is replayed; the five already stored are ignored.
    assert report.replayed_records == 10
    assert store2.count() == 10
    ids = {r.record_id for r in store2.query(_all()).records}
    assert ids == {f"r{i}" for i in range(10)}


def test_double_recovery_is_idempotent(tmp_path):
    wal_path = str(tmp_path / "w.wal")
    store = Store(":memory:")
    ing = Ingestor(store, WriteAheadLog(wal_path))
    ing.ingest([make_record(i) for i in range(4)], crash_point="after_wal")

    store2, ing2 = _restart(wal_path)
    ing2.recover()
    # A second recovery pass over the same WAL changes nothing.
    again = ing2.recover()
    assert again.pending_batches == 0
    assert store2.count() == 4
