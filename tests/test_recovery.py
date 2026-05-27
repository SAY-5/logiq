"""Recovery path: a crash before commit is replayed on restart."""

from __future__ import annotations

from logiq.ingest import Ingestor
from logiq.store import Store
from logiq.wal import WriteAheadLog

from .conftest import make_record


def test_recover_replays_uncommitted_batch(tmp_path):
    wal_path = str(tmp_path / "r.wal")
    store = Store(":memory:")
    ing = Ingestor(store, WriteAheadLog(wal_path))

    # Acknowledge a batch but crash before the store commit.
    ing.ingest([make_record(1), make_record(2)], crash_point="after_wal")
    assert store.count() == 0

    # Restart: fresh store, same WAL.
    store2 = Store(":memory:")
    ing2 = Ingestor(store2, WriteAheadLog(wal_path))
    report = ing2.recover()

    assert report.pending_batches == 1
    assert report.replayed_records == 2
    assert store2.count() == 2


def test_recover_is_noop_when_all_committed(tmp_path):
    wal_path = str(tmp_path / "r.wal")
    store = Store(":memory:")
    ing = Ingestor(store, WriteAheadLog(wal_path))
    ing.ingest([make_record(1)])

    ing2 = Ingestor(Store(":memory:"), WriteAheadLog(wal_path))
    report = ing2.recover()
    assert report.pending_batches == 0
