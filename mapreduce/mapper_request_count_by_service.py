#!/usr/bin/env python3
"""Output 1: one line per log row -> service_name\\t1"""
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
    service = row[3].strip()
    if service:
        print(f"{service}\t1")
