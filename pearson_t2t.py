#!/usr/bin/env python

"""
Table to table pearson.
"""

import itertools
import logging
import math
from argparse import ArgumentParser
from pathlib import Path

import pandas as pd
from scipy.stats import pearsonr

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(process)d - %(message)s",
)
LOGGER = logging.getLogger("pearson")

WORKING_DIR = Path.cwd()


def compute_pearson_of_two_tables(file_a: Path, file_b: Path):
    name_a = file_a.stem
    name_b = file_b.stem
    LOGGER.info(
        "compute pearson, file_a=%s, file_b=%s, name_a=%s, name_b=%s",
        file_a,
        file_b,
        name_a,
        name_b,
    )

    mat_a = pd.read_csv(file_a, index_col=0)
    mat_b = pd.read_csv(file_b, index_col=0)

    # Rename indexes, prefixed with filename.
    mat_a.rename(index={x: f"{name_a}_{x}" for x in mat_a.index}, inplace=True)
    mat_b.rename(index={x: f"{name_b}_{x}" for x in mat_b.index}, inplace=True)

    # Remove duplicated rows. (duplicate index name, keep the first row)
    mat_a = mat_a[~mat_a.index.duplicated(keep="first")]
    mat_b = mat_b[~mat_b.index.duplicated(keep="first")]

    # Remove rows only having zero values.
    mat_a = mat_a.loc[(mat_a != 0).any(axis=1)]
    mat_b = mat_b.loc[(mat_b != 0).any(axis=1)]

    # Rename columns, use index no. instead of names. Otherwise, pearson won't work.
    mat_a.rename(columns={x: str(i) for i, x in enumerate(mat_a.columns)}, inplace=True)
    mat_b.rename(columns={x: str(i) for i, x in enumerate(mat_b.columns)}, inplace=True)

    ans = pd.DataFrame(0, index=mat_a.index, columns=mat_b.index, dtype=float)
    ans_p_value = pd.DataFrame(0, index=mat_a.index, columns=mat_b.index, dtype=float)
    LOGGER.info("compute pearson corr, shape=%s", ans.shape)

    total = ans.size
    count = 0
    milestones = [math.ceil(x * total / 100) for x in range(1, 101)]
    next_milestone = 0
    for r in ans.index:
        for c in ans.columns:
            series_r = mat_a.loc[r]
            series_c = mat_b.loc[c]

            # Normalize.
            # NOTE: standardize your data first and then do normalization.

            # Z-score/mean normalization.
            # series_r = (series_r - series_r.mean()) / series_r.std()

            # Min-max normalization.
            # series_r = (series_r - series_r.min()) / series_r.max() - series_r.min())
            # series_c = (series_c - series_c.min()) / series_c.max() - series_c.min())

            # Custom normalization.
            series_r = series_r / series_r.max()
            series_c = series_c / series_c.max()

            # Only pearson value.
            # ans.at[r, c] = series_r.corr(series_c)

            # Get pearson value and p-value.
            try:
                r_value, p_value = pearsonr(series_r, series_c)
                ans.at[r, c] = r_value
                ans_p_value.at[r, c] = p_value
            except ValueError as ex:
                LOGGER.exception(ex)
                LOGGER.error(series_r)
                LOGGER.error(series_c)
                raise

            count += 1
            if count == milestones[next_milestone]:
                LOGGER.info("in progress, percentage=%d%%", next_milestone + 1)
                next_milestone += 1
    output_file = WORKING_DIR / f"pearson_{name_a}_x_{name_b}.csv"
    ans.to_csv(output_file)
    LOGGER.info("correlation value result saved, output=%s", output_file)

    output_file = WORKING_DIR / f"pearson_{name_a}_x_{name_b}_p_value.csv"
    ans_p_value.to_csv(output_file)
    LOGGER.info("p-value result saved, output=%s", output_file)


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("-f", "--files", nargs="+", default=[])
    return parser.parse_args()


def main():
    opts = parse_args()
    assert len(opts.files) >= 2, "require at least 2 files"
    assert all(x.endswith(".csv") for x in opts.files), "require .csv files"
    filenames = [Path.absolute(Path(x)) for x in opts.files]

    for (file_a, file_b) in itertools.combinations(filenames, 2):
        compute_pearson_of_two_tables(file_a, file_b)


if __name__ == "__main__":
    main()
