#!/usr/bin/env python3

import os
import csv
from pathlib import Path

DATA_DIR = Path.cwd() / "data-20210520-chf"

headers = [x.removesuffix(".csv") for x in os.listdir(DATA_DIR)]
# Remove "guide.csv", and "*.pearson.all.csv".
headers = list(filter(lambda x: x != "guide" and "." not in x, headers))

with open(DATA_DIR / "guide.csv", "wt") as fout:
    writer = csv.writer(fout)
    writer.writerow(headers)
    for v in s:
        writer.writerow([v] * len(headers))

