"""Ingest pipeline: durable batch path, recovery, and a file tailer.

Ingestor.ingest is the single acknowledged write path used by both the
HTTP push endpoint and the file tailer. It writes the batch to the WAL
first (durability point), then commits to the store, then marks the
batch committed in the WAL. A crash at any point between the WAL append
and the commit marker leaves a pending batch that recover() replays.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from dataclasses import dataclass

from .models import Level, LogRecord, normalize_ts
from .store import Store
from .wal import WriteAheadLog


@dataclass
class RecoveryReport:
    """Outcome of a recovery pass."""

    pending_batches: int = 0
    replayed_records: int = 0
    newly_committed_records: int = 0


class Ingestor:
    """Multi-source ingest with a durable batch path.

    crash_point is a test hook: when set to "after_wal" the commit step
    is skipped, simulating a process that died after acknowledging a
    batch but before persisting it to the store.
    """

    def __init__(self, store: Store, wal: WriteAheadLog) -> None:
        self._store = store
        self._wal = wal

    def ingest(self, records: Iterable[LogRecord], crash_point: str | None = None) -> int:
        """Durably ingest a batch. Returns the WAL sequence number.

        Steps: append to WAL (acknowledge), commit to store, mark
        committed. If crash_point == "after_wal" the method returns right
        after the durable append, modelling a crash before commit.
        """
        batch = list(records)
        seq = self._wal.append_batch(batch)
        if crash_point == "after_wal":
            return seq
        self._store.write_batch(batch)
        self._wal.mark_committed(seq)
        return seq

    def recover(self) -> RecoveryReport:
        """Replay any durable batch that was never marked committed.

        Idempotent: store writes ignore duplicate record_ids, so a batch
        that was partially committed before the crash is completed
        without duplicating already-stored records.
        """
        report = RecoveryReport()
        for pending in self._wal.pending_batches():
            report.pending_batches += 1
            report.replayed_records += len(pending.records)
            newly = self._store.write_batch(pending.records)
            report.newly_committed_records += newly
            self._wal.mark_committed(pending.seq)
        return report


def parse_line(line: str, source: str, line_no: int) -> LogRecord | None:
    """Parse one tailed log line of the form 'TS LEVEL message [trace=ID]'.

    The record_id is derived deterministically from source and line
    number so re-tailing the same file does not create duplicates.
    Returns None for blank lines.
    """
    from datetime import datetime

    line = line.rstrip("\n")
    if not line.strip():
        return None
    parts = line.split(" ", 2)
    if len(parts) < 3:
        return None
    ts_raw, level_raw, rest = parts
    try:
        ts = normalize_ts(datetime.fromisoformat(ts_raw))
    except ValueError:
        return None
    try:
        level = Level(level_raw.upper())
    except ValueError:
        return None
    trace_id = None
    message = rest
    if " trace=" in rest:
        message, _, trace_id = rest.rpartition(" trace=")
    return LogRecord(
        record_id=f"{source}:{line_no}",
        source=source,
        timestamp=ts,
        level=level,
        message=message,
        trace_id=trace_id or None,
    )


def tail_file(path: str, source: str, ingestor: Ingestor, batch_size: int = 100) -> int:
    """Read a file to end of stream and ingest its lines in batches.

    Returns the number of records ingested. Suitable for a stream that
    is fully written; the same parse_line / ingest path serves a live
    tailer that polls for appended lines.
    """
    if not os.path.exists(path):
        return 0
    ingested = 0
    batch: list[LogRecord] = []
    with open(path, encoding="utf-8") as fh:
        for line_no, line in enumerate(fh):
            rec = parse_line(line, source, line_no)
            if rec is None:
                continue
            batch.append(rec)
            if len(batch) >= batch_size:
                ingestor.ingest(batch)
                ingested += len(batch)
                batch = []
    if batch:
        ingestor.ingest(batch)
        ingested += len(batch)
    return ingested
