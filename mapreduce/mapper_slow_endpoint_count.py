#!/usr/bin/env python3
"""Output 3: slow requests (response_time_ms > 800) -> 'service,endpoint'\\t1"""
import csv
import sys

reader = csv.reader(sys.stdin)
first = True
for row in reader:
    if first:
        first = False
        continue
    if len(row) < 8:
        continue
    try:
        rt = int(row[7])
    except ValueError:
        continue
    if rt <= 800:
        continue
    service = row[3].strip()
    endpoint = row[4].strip()
    if service and endpoint:
        key = f"{service},{endpoint}"
        print(f"{key}\t1")
