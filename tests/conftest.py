"""Shared fixtures and helpers for the test suite."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from logiq.ingest import Ingestor
from logiq.models import Level, LogRecord
from logiq.store import Store
from logiq.wal import WriteAheadLog

BASE = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)


def make_record(i: int, *, source: str = "svc-a", level: Level = Level.INFO,
                offset_minutes: int = 0, trace_id: str | None = None) -> LogRecord:
    return LogRecord(
        record_id=f"r{i}",
        source=source,
        timestamp=BASE + timedelta(minutes=offset_minutes),
        level=level,
        message=f"event {i}",
        trace_id=trace_id,
    )


@pytest.fixture()
def store() -> Store:
    s = Store(":memory:")
    yield s
    s.close()


@pytest.fixture()
def ingestor(store: Store, tmp_path) -> Ingestor:
    wal = WriteAheadLog(str(tmp_path / "test.wal"))
    return Ingestor(store, wal)
