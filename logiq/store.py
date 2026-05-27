"""Partitioned SQL store with indexed query and partition pruning.

The store keeps all records in a single table carrying a partition_key
column. Partitioning is logical: the partition_key is computed from the
timestamp at write time and stored alongside the row. A bounded query
restricts the partition_key set up front (pruning) and the SQL engine
then uses the partition_key index plus per-column indexes to satisfy the
remaining predicates.

This layout is portable between SQLite (hermetic tests, CI) and Postgres
(compose). On Postgres the partition_key column can additionally back
declarative or list partitioning; the query logic is identical because
pruning is expressed as a partition_key IN (...) predicate either way.

All query predicates are bound through placeholders. No user-supplied
value is ever concatenated into SQL text.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import datetime

from .models import Level, LogRecord, Page, Query
from .partition import keys_for_range, partition_key

SCHEMA = """
CREATE TABLE IF NOT EXISTS logs (
    record_id     TEXT PRIMARY KEY,
    partition_key TEXT NOT NULL,
    source        TEXT NOT NULL,
    timestamp     TEXT NOT NULL,
    level         TEXT NOT NULL,
    message       TEXT NOT NULL,
    trace_id      TEXT
);
CREATE INDEX IF NOT EXISTS idx_logs_partition ON logs (partition_key);
CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs (timestamp);
CREATE INDEX IF NOT EXISTS idx_logs_source ON logs (source);
CREATE INDEX IF NOT EXISTS idx_logs_level ON logs (level);
CREATE INDEX IF NOT EXISTS idx_logs_trace ON logs (trace_id);
"""

# Columns used to reconstruct a LogRecord, in order.
_SELECT_COLS = "record_id, source, timestamp, level, message, trace_id"


class Store:
    """SQLite-backed partitioned log store."""

    def __init__(self, path: str = ":memory:") -> None:
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    @contextmanager
    def _cursor(self) -> Iterator[sqlite3.Cursor]:
        cur = self._conn.cursor()
        try:
            yield cur
        finally:
            cur.close()

    def write_batch(self, records: Iterable[LogRecord]) -> int:
        """Insert records idempotently. Returns count of new rows.

        A repeated record_id is ignored (INSERT OR IGNORE), which makes
        replay during recovery safe: re-applying an already-committed
        batch adds nothing.
        """
        rows = []
        for r in records:
            record_id, source, ts, level, message, trace_id = r.to_row()
            rows.append(
                (record_id, partition_key(r.timestamp), source, ts, level, message, trace_id)
            )
        with self._cursor() as cur:
            before = self._conn.total_changes
            cur.executemany(
                "INSERT OR IGNORE INTO logs "
                "(record_id, partition_key, source, timestamp, level, message, trace_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            self._conn.commit()
            return self._conn.total_changes - before

    def _build_where(self, q: Query) -> tuple[str, list[object], list[str] | None]:
        """Build a parameterized WHERE clause and the pruned partition set."""
        clauses: list[str] = []
        params: list[object] = []

        prune_keys = keys_for_range(q.start, q.end)
        if prune_keys is not None:
            if not prune_keys:
                # Empty range: a clause that matches nothing.
                clauses.append("1 = 0")
            else:
                placeholders = ",".join("?" for _ in prune_keys)
                clauses.append(f"partition_key IN ({placeholders})")
                params.extend(prune_keys)

        if q.start is not None:
            clauses.append("timestamp >= ?")
            params.append(q.start.isoformat())
        if q.end is not None:
            clauses.append("timestamp < ?")
            params.append(q.end.isoformat())
        if q.source is not None:
            clauses.append("source = ?")
            params.append(q.source)
        if q.level is not None:
            clauses.append("level = ?")
            params.append(q.level.value)
        if q.trace_id is not None:
            clauses.append("trace_id = ?")
            params.append(q.trace_id)
        if q.text is not None:
            clauses.append("message LIKE ?")
            params.append(f"%{q.text}%")

        where = " AND ".join(clauses) if clauses else "1 = 1"
        return where, params, prune_keys

    def query(self, q: Query) -> Page:
        """Run a bounded query and return one stable page of results.

        Ordering is (timestamp, record_id) ascending, which is total and
        deterministic, so offset-based pagination is stable across calls.
        """
        where, params, prune_keys = self._build_where(q)
        sql = (
            f"SELECT {_SELECT_COLS} FROM logs WHERE {where} "
            "ORDER BY timestamp ASC, record_id ASC LIMIT ? OFFSET ?"
        )
        # Fetch one extra row to know whether a next page exists.
        params_with_page = [*params, q.limit + 1, q.offset]
        with self._cursor() as cur:
            cur.execute(sql, params_with_page)
            fetched = cur.fetchall()

        has_more = len(fetched) > q.limit
        rows = fetched[: q.limit]
        records = [self._row_to_record(r) for r in rows]
        scanned = self._partition_count() if prune_keys is None else len(prune_keys)
        return Page(
            records=records,
            next_offset=(q.offset + q.limit) if has_more else None,
            total_scanned_partitions=scanned,
        )

    def scanned_partitions(self, q: Query) -> list[str] | None:
        """Partition keys a query would touch, or None for a full scan.

        Exposed so tests can assert pruning behaviour directly.
        """
        return keys_for_range(q.start, q.end)

    def full_scan(self, q: Query) -> list[LogRecord]:
        """Reference path: apply the same predicates with no pruning.

        Used by correctness tests to prove pruning never changes results.
        """
        clauses: list[str] = []
        params: list[object] = []
        if q.start is not None:
            clauses.append("timestamp >= ?")
            params.append(q.start.isoformat())
        if q.end is not None:
            clauses.append("timestamp < ?")
            params.append(q.end.isoformat())
        if q.source is not None:
            clauses.append("source = ?")
            params.append(q.source)
        if q.level is not None:
            clauses.append("level = ?")
            params.append(q.level.value)
        if q.trace_id is not None:
            clauses.append("trace_id = ?")
            params.append(q.trace_id)
        if q.text is not None:
            clauses.append("message LIKE ?")
            params.append(f"%{q.text}%")
        where = " AND ".join(clauses) if clauses else "1 = 1"
        sql = (
            f"SELECT {_SELECT_COLS} FROM logs WHERE {where} "
            "ORDER BY timestamp ASC, record_id ASC"
        )
        with self._cursor() as cur:
            cur.execute(sql, params)
            return [self._row_to_record(r) for r in cur.fetchall()]

    def count(self) -> int:
        with self._cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM logs")
            return int(cur.fetchone()[0])

    def _partition_count(self) -> int:
        with self._cursor() as cur:
            cur.execute("SELECT COUNT(DISTINCT partition_key) FROM logs")
            return int(cur.fetchone()[0])

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> LogRecord:
        return LogRecord(
            record_id=row["record_id"],
            source=row["source"],
            timestamp=datetime.fromisoformat(row["timestamp"]),
            level=Level(row["level"]),
            message=row["message"],
            trace_id=row["trace_id"],
        )
