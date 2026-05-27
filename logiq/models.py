"""Core data models for log records and queries."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum


class Level(StrEnum):
    """Standard log severity levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(UTC)


def normalize_ts(ts: datetime) -> datetime:
    """Coerce a datetime to timezone-aware UTC."""
    if ts.tzinfo is None:
        return ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC)


@dataclass(frozen=True)
class LogRecord:
    """A single log line ingested from a source.

    record_id is a caller-supplied idempotency key. Two records sharing
    the same record_id are treated as the same event, which lets the
    recovery path replay a durable batch without creating duplicates.
    """

    record_id: str
    source: str
    timestamp: datetime
    level: Level
    message: str
    trace_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "timestamp", normalize_ts(self.timestamp))

    def to_row(self) -> tuple[str, str, str, str, str, str | None]:
        """Flatten into the column order used by the store."""
        return (
            self.record_id,
            self.source,
            self.timestamp.isoformat(),
            self.level.value,
            self.message,
            self.trace_id,
        )


@dataclass(frozen=True)
class Query:
    """A bounded, validated query against the store.

    Time range is half-open [start, end). Pagination uses limit/offset
    with a hard ceiling on limit so a caller cannot request an unbounded
    scan.
    """

    start: datetime | None = None
    end: datetime | None = None
    source: str | None = None
    level: Level | None = None
    trace_id: str | None = None
    text: str | None = None
    limit: int = 100
    offset: int = 0

    MAX_LIMIT = 1000

    def __post_init__(self) -> None:
        if self.start is not None:
            object.__setattr__(self, "start", normalize_ts(self.start))
        if self.end is not None:
            object.__setattr__(self, "end", normalize_ts(self.end))
        if self.start is not None and self.end is not None and self.end < self.start:
            raise ValueError("query end must not precede start")
        if self.limit < 1 or self.limit > self.MAX_LIMIT:
            raise ValueError(f"limit must be in 1..{self.MAX_LIMIT}")
        if self.offset < 0:
            raise ValueError("offset must be non-negative")


@dataclass
class Page:
    """A page of query results plus its continuation cursor."""

    records: list[LogRecord] = field(default_factory=list)
    next_offset: int | None = None
    total_scanned_partitions: int = 0
