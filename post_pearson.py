#!/usr/bin/env python

import logging

from pathlib import Path
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(process)d - %(message)s",
)
LOGGER = logging.getLogger("post_pearson")

DATA_DIR = Path.cwd() / "data-20210523-emb"


def process_cell_group(cell_group: str, threshold: float = 0.6):
    LOGGER.info(
        "process cell group, cell_group=%s, threshold=%f", cell_group, threshold
    )
    assert threshold > 0, "threshold should > 0"
    input_file = DATA_DIR / f"{cell_group}.pearson.all.csv"
    pearson = pd.read_csv(input_file, index_col=0)
    # Filter 1: out any column with a corr-value >= threshold.
    filtered = pearson[pearson.abs().ge(threshold).any(1)]
    # Filter 2: all the columns should have at least 2 values largher than the threshold.
    filtered = filtered[
        filtered.columns[filtered[filtered.abs() >= threshold].count() > 1]
    ]
    ans = pd.DataFrame(0, index=filtered.index, columns=filtered.columns, dtype=str)
    for r in filtered.index:
        for c in filtered.columns:
            corr = filtered.at[r, c]
            if abs(corr) < threshold:
                ans.at[r, c] = "0"
            else:
                ans.at[r, c] = f"{corr:.4f}"

    output_file = DATA_DIR / f"{cell_group}.pearson.filtered.{threshold}.csv"
    ans.to_csv(output_file)


def main():
    meta = pd.read_csv(DATA_DIR / "guide.csv")

    for cell_group in meta.columns:
        process_cell_group(cell_group, threshold=0.6)


if __name__ == "__main__":
    main()
