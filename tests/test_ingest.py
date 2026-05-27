"""Ingest path: durable batch, idempotency, and the file tailer."""

from __future__ import annotations

from logiq.ingest import Ingestor, parse_line, tail_file
from logiq.models import Level
from logiq.store import Store
from logiq.wal import WriteAheadLog

from .conftest import make_record


def test_ingest_commits_and_acks(ingestor: Ingestor, store: Store):
    seq = ingestor.ingest([make_record(1), make_record(2)])
    assert seq == 1
    assert store.count() == 2


def test_ingest_is_idempotent_on_record_id(ingestor: Ingestor, store: Store):
    ingestor.ingest([make_record(1)])
    ingestor.ingest([make_record(1)])
    assert store.count() == 1


def test_tail_file_ingests_parsed_lines(tmp_path, store: Store):
    wal = WriteAheadLog(str(tmp_path / "t.wal"))
    ing = Ingestor(store, wal)
    log = tmp_path / "app.log"
    log.write_text(
        "2026-01-01T00:00:00+00:00 INFO started up\n"
        "2026-01-01T00:01:00+00:00 ERROR boom trace=abc123\n"
    )
    n = tail_file(str(log), "app", ing)
    assert n == 2
    assert store.count() == 2


def test_parse_line_extracts_trace_id():
    rec = parse_line("2026-01-01T00:00:00+00:00 ERROR db failed trace=xyz", "app", 0)
    assert rec is not None
    assert rec.level is Level.ERROR
    assert rec.trace_id == "xyz"
    assert rec.message == "db failed"


def test_parse_line_skips_malformed():
    assert parse_line("", "app", 0) is None
    assert parse_line("not a log line", "app", 0) is None
