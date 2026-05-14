#!/usr/bin/env python3
"""Hadoop Streaming reducer: sums integer values per key (key\\tvalue lines on stdin)."""
import sys

current_key = None
total = 0

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue
    parts = line.split("\t", 1)
    if len(parts) != 2:
        continue
    key, val = parts[0], parts[1]
    try:
        n = int(val)
    except ValueError:
        continue
    if current_key is None:
        current_key = key
        total = n
    elif key == current_key:
        total += n
    else:
        print(f"{current_key}\t{total}")
        current_key = key
        total = n

if current_key is not None:
    print(f"{current_key}\t{total}")
