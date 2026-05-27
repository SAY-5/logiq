"""Time-window partitioning scheme.

Records are placed into partitions keyed by a fixed-width time window
(default one hour). A partition key is derived purely from a record's
timestamp, so the mapping is deterministic and a query with a time range
can compute exactly the set of partitions it must scan. Partitions that
fall outside the range are pruned and never touched.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from .models import normalize_ts

# One hour windows by default. Small enough that a bounded query prunes
# most of a large store, large enough to keep partition count manageable.
WINDOW = timedelta(hours=1)


def partition_key(ts: datetime) -> str:
    """Map a timestamp to its partition key (window start, UTC)."""
    ts = normalize_ts(ts)
    floored = ts.replace(minute=0, second=0, microsecond=0)
    return floored.strftime("%Y%m%d%H")


def partition_start(key: str) -> datetime:
    """Inverse of partition_key: the window start for a key."""
    return datetime.strptime(key, "%Y%m%d%H").replace(tzinfo=UTC)


def keys_for_range(start: datetime | None, end: datetime | None) -> list[str] | None:
    """Return partition keys overlapping the half-open range [start, end).

    Returns None when the range is unbounded on either side, signalling
    the caller to scan all partitions. When both bounds are present the
    returned list is exactly the partitions that can hold matching
    records, which is the basis of pruning.
    """
    if start is None or end is None:
        return None
    start = normalize_ts(start)
    end = normalize_ts(end)
    cur = start.replace(minute=0, second=0, microsecond=0)
    keys: list[str] = []
    while cur < end:
        keys.append(cur.strftime("%Y%m%d%H"))
        cur += WINDOW
    return keys
