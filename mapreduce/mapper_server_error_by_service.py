#!/usr/bin/env python3
"""Output 2: rows with status_code >= 500 -> service_name\\t1"""
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
        code = int(row[6])
    except ValueError:
        continue
    if code < 500:
        continue
    service = row[3].strip()
    if service:
        print(f"{service}\t1")
