#!/usr/bin/env python
import logging
from argparse import ArgumentParser
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(process)d - %(message)s",
)
LOGGER = logging.getLogger("post_pearson")

WORKING_DIR = Path.cwd()


def post_pearson(filename: Path, threshold: float = 0.99):
    LOGGER.info("filter, filename=%s, threshold=%.4f", filename, threshold)
    pearson = pd.read_csv(filename, index_col=0)
    # Filter 1: out any column with a corr-value >= threshold.
    filtered = pearson[pearson.abs().ge(threshold).any(1)]
    # Filter 2: all the columns should have at least 2 values largher than the threshold.
    filtered = filtered[
        filtered.columns[filtered[filtered.abs() >= threshold].count() > 0]
    ]

    output_file = WORKING_DIR / f"{filename.stem}.filtered.{threshold}.csv"
    filtered.to_csv(output_file)
    LOGGER.info("filtered result saved at %s", output_file)
    # ans = pd.DataFrame(0, index=filtered.index, columns=filtered.columns, dtype=str)


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("-f", "--files", nargs="+", default=[])
    parser.add_argument("-t", "--threshold", type=float, default=0.95)
    return parser.parse_args()


def main():
    opts = parse_args()
    assert len(opts.files) >= 2, "require at least 2 files"
    assert all(x.endswith(".csv") for x in opts.files), "require .csv files"
    filenames = [Path.absolute(Path(x)) for x in opts.files]

    for filename in filenames:
        if "filtered" in str(filename):
            LOGGER.warning("skip, filename=%s", filename)
            continue
        post_pearson(filename, threshold=opts.threshold)


if __name__ == "__main__":
    main()
