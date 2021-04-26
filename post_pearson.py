#!/usr/bin/env python

import logging

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(process)d - %(message)s",
)
LOGGER = logging.getLogger("post_pearson")


def process_cell_group(cell_group: str, threshold: float = 0.6):
    LOGGER.info(
        "process cell group, cell_group=%s, threshold=%f", cell_group, threshold
    )
    assert threshold > 0, "threshold should > 0"
    input_file = f"./{cell_group}.pearson.all.csv"
    pearson = pd.read_csv(input_file, index_col=0)
    filtered = pearson[
        pearson.abs().ge(threshold).any(1) & pearson.abs().lt(0.99).all(1)
    ]
    ans = pd.DataFrame(0, index=filtered.index, columns=filtered.columns, dtype=str)
    for r in filtered.index:
        for c in filtered.columns:
            corr = filtered.at[r, c]
            if abs(corr) < threshold:
                ans.at[r, c] = "-"
            else:
                ans.at[r, c] = f"{corr:.4f}"

    output_file = f"./{cell_group}.pearson.filtered.{threshold}.csv"
    ans.to_csv(output_file)


def main():
    meta = pd.read_csv("./data-20210422/Correlated Genes.csv")
    for cell_group in meta.columns:
        process_cell_group(cell_group, threshold=0.4)


if __name__ == "__main__":
    main()
