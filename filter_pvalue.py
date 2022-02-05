#!/usr/bin/env python
import logging
from argparse import ArgumentParser
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(process)d - %(message)s",
)
LOGGER = logging.getLogger("post_pearson")

WORKING_DIR = Path.cwd()


def filter_pvalue(filename: Path, threshold: float, output_filename: Path):
    pvalue_filename = Path(
        filename.parent, Path(filename.stem + "_p_value" + filename.suffix)
    )
    assert (
        pvalue_filename.exists() and pvalue_filename.is_file()
    ), f"P value file {pvalue_filename} does not exist"

    LOGGER.info(
        "filter, filename=%s, p_value_filename=%s, threshold=%.4f",
        filename,
        pvalue_filename,
        threshold,
    )

    df = pd.read_csv(filename, index_col=0)
    df_pvalue = pd.read_csv(pvalue_filename, index_col=0)
    df_filtered = pd.DataFrame(0, index=df.index, columns=df.columns, dtype=float)
    for r in df.index:
        for c in df.columns:
            # https://stackoverflow.com/questions/37216485/pandas-at-versus-loc
            df_filtered.at[r, c] = (
                df.at[r, c] if df_pvalue.at[r, c] < threshold else np.nan
            )
    df_filtered.to_csv(output_filename)
    LOGGER.info("filtered result saved at %s", output_filename)


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("-f", "--files", nargs="+", default=[])
    parser.add_argument("-t", "--threshold", type=float, default=0.05)
    return parser.parse_args()


def main():
    opts = parse_args()
    assert len(opts.files) > 0, "require at least 1 file"
    assert all(x.endswith(".csv") for x in opts.files), "require .csv files"
    filenames = [Path.absolute(Path(x)) for x in opts.files]
    tag = f"pfiltered.{opts.threshold}".replace(".", "_")

    for filename in filenames:
        if tag in str(filename):
            LOGGER.warning("skip, filename=%s", filename)
            continue
        if filename.stem.endswith("_p_value"):
            LOGGER.warning("skip, filename=%s", filename)
            continue
        output_filename = filename.with_suffix("." + tag + ".csv")
        if output_filename.exists():
            LOGGER.warning("skip, filename=%s", filename)
            continue

        filter_pvalue(
            filename,
            threshold=opts.threshold,
            output_filename=output_filename,
        )


if __name__ == "__main__":
    main()
