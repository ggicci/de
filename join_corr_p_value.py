#!/usr/bin/env python

import logging
import re
from argparse import ArgumentParser
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(process)d - %(message)s",
)

WORKING_DIR = Path.cwd()


def join_corr_with_p_value(corr_file, p_value_file, threshold):
    LOGGER = logging.getLogger("join_corr_with_p_value")
    LOGGER.info(
        "join corr with p-value files, corr_file=%s, p_value_file=%s, threshold=%.2f",
        corr_file,
        p_value_file,
        threshold,
    )
    corr_df = pd.read_csv(corr_file, header=0, index_col=0)
    p_value_df = pd.read_csv(p_value_file, header=0, index_col=0)

    # Get all rows that have at least one column with p-value < threshold.
    p_value_df = p_value_df.loc[(p_value_df < threshold).any(axis=1)]

    # Map to the corr_df.
    ans_matrix = pd.DataFrame(
        0, index=p_value_df.index, columns=p_value_df.columns, dtype=float
    )

    for r in p_value_df.index:
        for c in p_value_df.columns:
            if p_value_df.loc[r, c] < threshold:
                ans_matrix.loc[r, c] = corr_df.loc[r, c]

    # Rename rows and columns, remove the gene prefix.
    pat = re.compile(r"^\w+_")
    ans_matrix.rename(
        index={x: re.sub(pat, "", x) for x in ans_matrix.index}, inplace=True
    )
    ans_matrix.rename(
        columns={x: re.sub(pat, "", x) for x in ans_matrix.columns}, inplace=True
    )
    ans_matrix.to_csv(corr_file.with_suffix(".join.csv"))


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("-c", "--cfile", required=True, help="correlation file")
    parser.add_argument("-p", "--pfile", required=True, help="p-value file")
    parser.add_argument("-t", "--threshold", type=float, help="threshold", default=0.05)
    return parser.parse_args()


def main():
    opts = parse_args()

    corr_file = WORKING_DIR / opts.cfile
    p_value_file = WORKING_DIR / opts.pfile
    join_corr_with_p_value(corr_file, p_value_file, opts.threshold)


if __name__ == "__main__":
    main()
