#!/usr/bin/env python

import logging
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


def process_cell_group(cell_group: str, genes: List[str]):
    """Process a cell group."""
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


def main():
    meta = pd.read_csv("./data-20210422/Correlated Genes.csv")
    for cell_group in meta.columns:
        if cell_group != "Myeloid":
            continue
        gene_series = meta[[cell_group]]
        gene_series = gene_series[gene_series[cell_group].notnull()]
        genes = gene_series[cell_group].tolist()
        LOGGER.info("process cell group, cell_group=%s, genes=%s", cell_group, genes)
        process_cell_group(cell_group, genes)


if __name__ == "__main__":
    main()
