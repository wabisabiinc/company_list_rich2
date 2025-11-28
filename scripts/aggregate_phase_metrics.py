"""
logs/phase_metrics.csv を集計して平均/標準偏差/p95などを出力する簡易スクリプト。

使い方:
    python scripts/aggregate_phase_metrics.py --path logs/phase_metrics.csv
"""

import argparse
import csv
import math
from collections import defaultdict
from typing import Dict, List


def percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    values = sorted(values)
    k = (len(values) - 1) * pct
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return values[int(k)]
    return values[int(f)] * (c - k) + values[int(c)] * (k - f)


def main() -> None:
    parser = argparse.ArgumentParser(description="Aggregate phase metrics (avg/stddev/p50/p95).")
    parser.add_argument("--path", default="logs/phase_metrics.csv", help="Path to metrics CSV")
    args = parser.parse_args()

    metrics: Dict[str, List[float]] = defaultdict(list)
    with open(args.path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                phase = row["phase"]
                elapsed = float(row["elapsed_sec"])
            except Exception:
                continue
            metrics[phase].append(elapsed)

    if not metrics:
        print("No metrics found.")
        return

    print("phase,count,avg,stdev,p50,p95,max")
    for phase, values in sorted(metrics.items()):
        n = len(values)
        avg = sum(values) / n
        var = sum((v - avg) ** 2 for v in values) / n if n > 0 else 0.0
        stdev = math.sqrt(var)
        p50 = percentile(values, 0.5)
        p95 = percentile(values, 0.95)
        max_val = max(values)
        print(f"{phase},{n},{avg:.3f},{stdev:.3f},{p50:.3f},{p95:.3f},{max_val:.3f}")


if __name__ == "__main__":
    main()
