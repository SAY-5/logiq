# Benchmark results

Measured with `python -m benchmarks.run` on the development host
(Python 3.13, SQLite in memory, store of 200,000 records). Numbers vary
with hardware; rerun locally to reproduce.

| Metric | Value |
| --- | --- |
| Ingest throughput | ~135,000 records/sec |
| Trace id lookup, indexed | ~0.012 ms |
| Trace id lookup, full scan (no index) | ~8.6 ms |
| Index lookup speedup | ~700x |
| Partitions read for a one hour window | 1 |

## Reading the numbers

The lookup compares the same query two ways over the same 200,000 rows:
the partitioned, indexed store answers a trace id point lookup with an
index seek, while an equivalent table with no indexes scans every row.
The ratio of the two median latencies is the speedup. Both paths are
asserted to return the identical row set before timing, so the
comparison is like for like.

The one hour window figure shows partition pruning: a bounded query is
restricted to the partitions overlapping its range, so a one hour query
over a multi day store reads a single partition rather than all of them.

`benchmarks/regress.py` runs a smaller version of these measurements as a
CI gate with a 30 percent tolerance on ingest throughput and a minimum
index lookup speedup, so a performance regression fails the build.
