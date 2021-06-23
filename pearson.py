#!/usr/bin/env python

import logging
import math
from pathlib import Path
from typing import List

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(process)d - %(message)s",
)
LOGGER = logging.getLogger("pearson")


DATA_DIR = Path.cwd() / "data-20210523-emb"


def process_cell_group(cell_group: str, genes: List[str]):
    LOGGER.info("process cell group, cell_group=%s, genes=%s", cell_group, genes)

    output_file = DATA_DIR / f"{cell_group}.pearson.all.csv"
    if output_file.exists():
        LOGGER.info("skip cus output file exists, file=%s", output_file)
        return

    input_file = DATA_DIR / f"{cell_group}.csv"
    LOGGER.info("read cell group gene data, file=%s", input_file)
    gene_data = pd.read_csv(input_file, index_col=0)
    left_set = set(gene_data.index.tolist())
    right_set = set(genes)
    left_data = gene_data.drop(index=[x for x in gene_data.index if x not in left_set])
    right_data = gene_data.drop(
        index=[x for x in gene_data.index if x not in right_set]
    )
    ans = pd.DataFrame(0, index=left_data.index, columns=right_data.index, dtype=float)
    LOGGER.info("compute pearson corr, shape=%s", ans.shape)
    total = ans.size
    count = 0
    milestones = [math.ceil(x * total / 100) for x in range(1, 101)]
    next_milestone = 0
    for r in ans.index:
        for c in ans.columns:
            ans.at[r, c] = left_data.loc[r].corr(right_data.loc[c])
            count += 1
            if count == milestones[next_milestone]:
                LOGGER.info("in progress, percentage=%d%%", next_milestone + 1)
                next_milestone += 1

    ans.to_csv(output_file)


def main():
    meta = pd.read_csv(DATA_DIR / "guide.csv")
    for cell_group in meta.columns:
        gene_series = meta[[cell_group]]
        gene_series = gene_series[gene_series[cell_group].notnull()]
        genes = gene_series[cell_group].tolist()
        process_cell_group(cell_group, genes)


if __name__ == "__main__":
    main()
