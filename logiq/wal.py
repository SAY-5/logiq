"""Durable write-ahead log for in-flight ingest batches.

An ingest batch is acknowledged to the caller only after it is appended
and flushed to the WAL. Commit into the store happens afterward. Each
batch is framed as one JSON line tagged with a monotonically increasing
batch sequence and a marker telling whether it was committed.

On restart the recovery routine reads the WAL, finds batches that were
durably recorded but not marked committed, and replays them into the
store. Because store writes are idempotent on record_id, replaying a
batch that was in fact already partly committed neither loses nor
duplicates records.

Frame format (one JSON object per line):
    {"seq": int, "kind": "batch", "records": [...]}
    {"seq": int, "kind": "commit"}
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .models import Level, LogRecord

Frame = dict[str, Any]


def _record_to_json(r: LogRecord) -> dict[str, object]:
    return {
        "record_id": r.record_id,
        "source": r.source,
        "timestamp": r.timestamp.isoformat(),
        "level": r.level.value,
        "message": r.message,
        "trace_id": r.trace_id,
    }


def _record_from_json(d: dict[str, object]) -> LogRecord:
    return LogRecord(
        record_id=str(d["record_id"]),
        source=str(d["source"]),
        timestamp=datetime.fromisoformat(str(d["timestamp"])),
        level=Level(str(d["level"])),
        message=str(d["message"]),
        trace_id=None if d.get("trace_id") is None else str(d["trace_id"]),
    )


@dataclass
class _PendingBatch:
    seq: int
    records: list[LogRecord]


class WriteAheadLog:
    """Append-only durable log of ingest batches."""

    def __init__(self, path: str) -> None:
        self._path = path
        self._seq = self._scan_max_seq()
        # Open in append mode so existing frames survive a reopen.
        self._fh = open(path, "a", encoding="utf-8")

    @property
    def path(self) -> str:
        return self._path

    def _scan_max_seq(self) -> int:
        if not os.path.exists(self._path):
            return 0
        mx = 0
        for frame in self._read_frames():
            mx = max(mx, int(frame["seq"]))
        return mx

    def append_batch(self, records: list[LogRecord]) -> int:
        """Durably record a batch and flush. Returns its sequence number.

        Returning from this call is the acknowledgement point: once it
        returns the batch is recoverable even if the process dies before
        the store commit.
        """
        self._seq += 1
        seq = self._seq
        frame = {"seq": seq, "kind": "batch", "records": [_record_to_json(r) for r in records]}
        self._write_frame(frame)
        return seq

    def mark_committed(self, seq: int) -> None:
        """Record that the store commit for a batch completed."""
        self._write_frame({"seq": seq, "kind": "commit"})

    def _write_frame(self, frame: Frame) -> None:
        self._fh.write(json.dumps(frame, separators=(",", ":")) + "\n")
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def _read_frames(self) -> Iterator[Frame]:
        if not os.path.exists(self._path):
            return
        with open(self._path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    # A torn trailing frame from a crash mid-write is
                    # ignored: it was never acknowledged.
                    continue

    def pending_batches(self) -> list[_PendingBatch]:
        """Batches recorded but never marked committed, in order."""
        batches: dict[int, list[LogRecord]] = {}
        committed: set[int] = set()
        for frame in self._read_frames():
            kind = frame.get("kind")
            seq = int(frame["seq"])
            if kind == "batch":
                recs = [_record_from_json(d) for d in frame["records"]]
                batches[seq] = recs
            elif kind == "commit":
                committed.add(seq)
        pending = [
            _PendingBatch(seq=seq, records=recs)
            for seq, recs in sorted(batches.items())
            if seq not in committed
        ]
        return pending

    def close(self) -> None:
        self._fh.close()
