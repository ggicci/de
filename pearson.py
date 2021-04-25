#!/usr/bin/env python

import logging
import math
import os
from multiprocessing import Manager, Pool
from typing import List

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(process)d - %(message)s",
)
LOGGER = logging.getLogger("pearson")

shared = Manager().Namespace()


def parallelize_dataframe(df, func, n_cores=4):
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


def compute_pearson_corr(df: pd.DataFrame):
    LOGGER.info("compute pearson corr, shape=%s", df.shape)
    for r in df.index:
        for c in df.columns:
            df.at[r, c] = shared.left_data.loc[r].corr(shared.right_data.loc[c])


def process_cell_group_parallel(cell_group: str, genes: List[str]):
    gene_data = pd.read_csv(f"./data-20210418/{cell_group}.csv", index_col=0)
    left_set = set(gene_data.index.tolist())
    right_set = set(genes)
    left_data = gene_data.drop(index=[x for x in gene_data.index if x not in left_set])
    right_data = gene_data.drop(
        index=[x for x in gene_data.index if x not in right_set]
    )
    ans = pd.DataFrame(0, index=left_data.index, columns=right_data.index, dtype=float)
    shared.left_data = left_data
    shared.right_data = right_data

    ans = parallelize_dataframe(ans, compute_pearson_corr, n_cores=8)
    ans.to_csv(f"./{cell_group}.pearson.all.csv")


def process_cell_group(cell_group: str, genes: List[str]):
    LOGGER.info("process cell group, cell_group=%s, genes=%s", cell_group, genes)

    output_file = f"./{cell_group}.pearson.all.csv"
    if os.path.exists(output_file):
        LOGGER.info("skip cus output file exists, file=%s", output_file)
        return

    input_file = f"./data-20210418/{cell_group}.csv"
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
    meta = pd.read_csv("./data-20210422/Correlated Genes.csv")
    for cell_group in meta.columns:
        gene_series = meta[[cell_group]]
        gene_series = gene_series[gene_series[cell_group].notnull()]
        genes = gene_series[cell_group].tolist()
        process_cell_group(cell_group, genes)


if __name__ == "__main__":
    main()
