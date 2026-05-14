#!/usr/bin/env python3
"""
Local MapReduce-style pipeline: mapper -> shuffle (sort by key) -> reducer.
Works on Windows and Unix without Hadoop.

Usage:
  python tools/run_local_mapreduce.py Comp3041J MiniProject 2 Dataset.csv mapreduce/mapper_request_count_by_service.py mapreduce/reducer_sum.py
  python tools/run_local_mapreduce.py ... mapreduce/mapper_slow_endpoint_count.py ... --top10
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("csv_path", type=Path)
    p.add_argument("mapper", type=Path)
    p.add_argument("reducer", type=Path)
    p.add_argument("--top10", action="store_true", help="Print top 10 by count (descending)")
    args = p.parse_args()

    repo = Path(__file__).resolve().parent.parent
    csv_path = args.csv_path if args.csv_path.is_absolute() else repo / args.csv_path
    mapper = args.mapper if args.mapper.is_absolute() else repo / args.mapper
    reducer = args.reducer if args.reducer.is_absolute() else repo / args.reducer

    with csv_path.open("rb") as inf:
        m = subprocess.run(
            [sys.executable, str(mapper)],
            stdin=inf,
            capture_output=True,
            check=True,
        )
    mapped_lines = m.stdout.decode("utf-8", errors="replace").splitlines()
    mapped_lines = [ln for ln in mapped_lines if ln.strip()]
    mapped_lines.sort(key=lambda ln: ln.split("\t", 1)[0])

    r = subprocess.run(
        [sys.executable, str(reducer)],
        input="\n".join(mapped_lines) + "\n",
        capture_output=True,
        text=True,
        check=True,
    )
    lines = [ln for ln in r.stdout.splitlines() if ln.strip()]

    if not args.top10:
        for ln in lines:
            print(ln)
        return

    parsed: list[tuple[str, int]] = []
    for ln in lines:
        key, _, val = ln.partition("\t")
        if not val:
            continue
        try:
            parsed.append((key, int(val)))
        except ValueError:
            continue
    parsed.sort(key=lambda x: x[1], reverse=True)
    for key, val in parsed[:10]:
        print(f"{key}\t{val}")


if __name__ == "__main__":
    main()
