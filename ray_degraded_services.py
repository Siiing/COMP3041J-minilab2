#!/usr/bin/env python3
"""
Task 3: Ray extension — degraded service detection (parallel partial stats, then merge).

Rules (per service_name):
  - degraded if slow_rate > 20%   (slow: response_time_ms > 800)
  - or server_error_rate > 10%   (server error: status_code >= 500)
  - or Timeout count >= 5        (error_type == Timeout)

Output lines: service_name,reason
"""
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict

import ray

Stats = DefaultDict[str, dict[str, int]]


def _empty_service_stats() -> dict[str, int]:
    return {"total": 0, "slow": 0, "server_err": 0, "timeout": 0}


@ray.remote
def partial_stats(lines: list[str]) -> dict[str, dict[str, int]]:
    """Parse a chunk of CSV body lines (no header) and return per-service counters."""
    out: Stats = defaultdict(_empty_service_stats)
    for line in lines:
        row = next(csv.reader([line]))
        if len(row) < 8:
            continue
        service = row[3].strip()
        if not service:
            continue
        try:
            status = int(row[6])
            rtime = int(row[7])
        except ValueError:
            continue
        err = row[9].strip() if len(row) > 9 else ""
        s = out[service]
        s["total"] += 1
        if rtime > 800:
            s["slow"] += 1
        if status >= 500:
            s["server_err"] += 1
        if err == "Timeout":
            s["timeout"] += 1
    return dict(out)


def merge_stats(partials: list[dict[str, dict[str, int]]]) -> dict[str, dict[str, int]]:
    merged: Stats = defaultdict(_empty_service_stats)
    for part in partials:
        for svc, c in part.items():
            m = merged[svc]
            for k in ("total", "slow", "server_err", "timeout"):
                m[k] += c.get(k, 0)
    return dict(merged)


def degraded_reasons(stats: dict[str, int]) -> list[str]:
    reasons: list[str] = []
    total = stats["total"]
    if total <= 0:
        return reasons
    slow_r = stats["slow"] / total
    err_r = stats["server_err"] / total
    if stats["timeout"] >= 5:
        reasons.append("repeated timeout errors")
    if err_r > 0.10:
        reasons.append("high server error rate")
    if slow_r > 0.20:
        reasons.append("high slow request rate")
    return reasons


def main() -> None:
    p = argparse.ArgumentParser(description="Ray degraded-service detection over log CSV.")
    p.add_argument(
        "csv_path",
        type=Path,
        nargs="?",
        default=Path("Comp3041J MiniProject 2 Dataset.csv"),
        help="Path to dataset CSV",
    )
    p.add_argument("--chunks", type=int, default=8, help="Number of parallel tasks")
    args = p.parse_args()

    csv_path = args.csv_path
    if not csv_path.is_file():
        raise SystemExit(f"CSV not found: {csv_path.resolve()}")

    text = csv_path.read_text(encoding="utf-8", errors="replace").splitlines()
    if not text:
        raise SystemExit("empty file")
    body = text[1:]

    n = max(1, min(args.chunks, len(body)))
    size = (len(body) + n - 1) // n
    chunks = [body[i : i + size] for i in range(0, len(body), size)]

    ray.init(ignore_reinit_error=True)
    try:
        futures = [partial_stats.remote(chunk) for chunk in chunks]
        partials = ray.get(futures)
    finally:
        ray.shutdown()

    merged = merge_stats(partials)
    rows: list[tuple[str, str]] = []
    for service in sorted(merged.keys()):
        st = merged[service]
        for reason in degraded_reasons(st):
            rows.append((service, reason))

    for service, reason in rows:
        print(f"{service},{reason}")


if __name__ == "__main__":
    main()
