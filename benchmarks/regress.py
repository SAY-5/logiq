"""Benchmark regression smoke gate.

Runs a small ingest and indexed lookup, then checks the measured numbers
against a committed baseline. The gate fails if ingest throughput drops
more than the allowed fraction below baseline, or if the indexed lookup
loses its advantage over a full scan. The baseline is set conservatively
low so the gate catches a real regression, not normal CI variance.
"""

from __future__ import annotations

import sys

from benchmarks.run import bench_ingest, bench_query

# Allowed regression: measured throughput may fall to (1 - TOLERANCE) of
# baseline before the gate fails.
TOLERANCE = 0.30

# Baselines are intentionally below observed local numbers so shared CI
# runners, which are slower and noisier, stay above the gate.
BASELINE_INGEST_RPS = 20_000
MIN_INDEX_SPEEDUP = 5.0


def main() -> int:
    n = 50_000
    tput = bench_ingest(n)
    q = bench_query(n)
    floor = BASELINE_INGEST_RPS * (1 - TOLERANCE)
    print(f"ingest_throughput_rps: {tput:,.0f} (floor {floor:,.0f})")
    print(f"index_lookup_speedup: {q['speedup']:.1f}x (min {MIN_INDEX_SPEEDUP})")
    print(f"pruned_partitions_for_1h_window: {q['scanned_partitions']:.0f}")

    ok = True
    if tput < floor:
        print("FAIL: ingest throughput below floor")
        ok = False
    if q["speedup"] < MIN_INDEX_SPEEDUP:
        print("FAIL: index lookup speedup below minimum")
        ok = False
    if not ok:
        return 1
    print("bench-regress OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
