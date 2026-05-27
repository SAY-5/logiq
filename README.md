# LogIQ

LogIQ is a distributed log aggregation and query service. It ingests log
records from multiple sources into a partitioned SQL store, exposes a REST
query API with indexed lookups and partition pruning, and recovers any
in-flight ingest batch after a crash without loss or duplication.

## What it does

- Multi-source ingest: an HTTP push endpoint and a file/stream tailer feed
  the same durable batch path.
- Durable write-ahead path: a batch is acknowledged only after it is written
  and flushed to a write-ahead log, so an in-flight batch survives a crash.
- Partitioned SQL store: records carry a partition key derived from their
  timestamp window, with indexes on timestamp, source, level, and trace id.
- REST query API: filter by time range, source, level, trace id, and free
  text, with bounded, stable pagination.
- Recovery routine: on restart, batches recorded in the write-ahead log but
  not yet committed are replayed into the store idempotently.

## Distribution model

The service models distribution as sharded time-window partitions plus
independent ingest workers. Each partition key is computed deterministically
from a record timestamp, so workers can write to disjoint partitions without
coordination and a bounded query can compute exactly the partition set it
must read. This runs in one process for tests and compose; the same scheme
maps onto multiple workers and a Postgres partitioned table without changing
the query logic.

## Partitioning and pruning

A partition key is the start of a fixed time window (one hour by default),
formatted as `YYYYMMDDHH`. A query with both a start and end bound is
restricted to the partitions overlapping that range with a
`partition_key IN (...)` predicate. Partitions outside the range are never
read. Pruning never changes results: the pruned query returns the same
records a full predicate scan would (see the v3 tests and benchmark).

## Query API

`POST /ingest`

```json
{
  "records": [
    {
      "record_id": "a1",
      "source": "web",
      "timestamp": "2026-01-01T00:00:00+00:00",
      "level": "ERROR",
      "message": "request failed",
      "trace_id": "t1"
    }
  ]
}
```

`GET /query` accepts `start`, `end`, `source`, `level`, `trace_id`, `text`,
`limit`, and `offset`. The time range is half-open `[start, end)`. `limit` is
capped at 1000. All filter values are bound through SQL placeholders.

`GET /health` reports record count.

## Running

Local, SQLite:

```bash
pip install -e ".[dev]"
logiq serve --db logiq.db --wal logiq.wal
```

Tail a file into the store:

```bash
logiq tail app.log --source app --db logiq.db --wal logiq.wal
```

With compose (service plus Postgres):

```bash
docker compose up --build
```

## Recovery

On startup the service opens the write-ahead log and replays any batch that
was acknowledged but not committed. Replay is idempotent: store writes ignore
a record id that already exists, so a batch that was partly committed before
a crash is completed without duplicates. See `tests/test_crash_recovery.py`
for an injected-crash proof.

## Tests and benchmarks

```bash
pytest --cov=logiq
python -m benchmarks.run
```

The benchmark reports ingest throughput and compares an indexed, pruned query
against a full scan over a large store. Numbers come from the run on your
machine; see `benchmarks/`.

## How this differs

LogIQ is an ingest and query service: a partitioned SQL store, an indexed
REST query API, and crash-recovery durability. It is distinct from `tracesift`,
which does offline failure clustering analysis and is not a store or query
service, and from log pipeline projects that move data without owning a
queryable partitioned store. The angle here is the partitioned store plus
indexed query plus the durable recovery path.

## License

MIT.
