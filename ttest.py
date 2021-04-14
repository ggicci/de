#!/usr/bin/env python

import logging
import os
from argparse import ArgumentParser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(process)d - %(message)s",
)

WORKING_DIR = os.getcwd()
LOGGER = logging.getLogger("ttest")

def abspath(relative_path: str) -> str:
    if os.path.isabs(relative_path):
        return relative_path
    return os.path.abspath(os.path.join(WORKING_DIR, relative_path))

def get_output_filename(filename: str, suffix: str) -> str:
    dirname = os.path.dirname(filename)
    basename = os.path.basename(filename)
    title = os.path.splitext(basename)[0]
    return os.path.join(dirname, title+suffix)

def ttest(filename: str):
    import anndata
    import diffxpy.api as de
    import pandas
    df = pandas.read_csv(filename, header=0, index_col=0)
    # cols = df.columns.tolist()
    # pca1 = [x for x in cols if x.startswith("pca1")]
    # pca2 = [x for x in cols if x.startswith("pca2")]
    # new_cols = pca2 + pca1
    # assert len(cols) == len(new_cols)
    # df = df[new_cols]
    data = anndata.AnnData(df.transpose())
    grouping = ["1-NOR" if x.startswith("pca1") else "2-HF" for x in data.obs.index.tolist()]
    test = de.test.t_test(data, grouping)

    summary_output_file = get_output_filename(filename, ".out.csv")
    test.summary().to_csv(summary_output_file)
    LOGGER.info("summary saved, output=%s", summary_output_file)

    volcano_output_file = get_output_filename(filename, ".volcano.jpg")
    test.plot_volcano(
        corrected_pval=True,
        alpha=0.05,
        size=20,
        show=False,
        save=volcano_output_file,
        # highlight_ids=["NPPA"],
    )
    LOGGER.info("volcano saved, output=%s", volcano_output_file)

def parse_args():
    parser = ArgumentParser()
    parser.add_argument("-f", "--file", type=str, required=True, help="Input CSV")
    return parser.parse_args()

def main():
    opts = parse_args()
    assert opts.file.endswith('.csv'), "require .csv files"
    filename = abspath(opts.file)
    LOGGER.info("start ttest, input=%s", filename)
    ttest(filename)

if __name__ == "__main__":
    main()
