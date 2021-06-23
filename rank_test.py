#!/usr/bin/env python

import logging
import os
from argparse import ArgumentParser
from collections import defaultdict
from typing import List

import pandas

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(process)d - %(message)s",
)

WORKING_DIR = os.getcwd()
LOGGER = logging.getLogger("rank")


def abspath(relative_path: str) -> str:
    if os.path.isabs(relative_path):
        return relative_path
    return os.path.abspath(os.path.join(WORKING_DIR, relative_path))


def get_output_filename(filename: str, suffix: str) -> str:
    dirname = os.path.dirname(filename)
    basename = os.path.basename(filename)
    title = os.path.splitext(basename)[0]
    return os.path.join(dirname, title + suffix)


def find_groupings(
    df: pandas.DataFrame, default_group: str = "nor", exclude_groups: List[str] = None
) -> List:
    exclude_groups = [] if exclude_groups is None else exclude_groups
    groups = defaultdict(list)
    for name in df.index:
        tag = name.lower().split("_")[0]
        groups[tag].append(name)

    groupings = []
    if default_group not in groups:
        raise Exception(f"default group not found: {default_group}")
    for tag in groups.keys():
        if tag == default_group or tag in exclude_groups:
            continue
        groupings.append(((default_group, groups[default_group]), (tag, groups[tag])))
    return groupings


def rank_test(filename: str):
    import anndata
    import diffxpy.api as de

    df = pandas.read_csv(filename, header=0, index_col=0).transpose()

    groupings = find_groupings(df, default_group="control", exclude_groups=None)
    LOGGER.info(
        "group, filename=%s, groups=%d, groupings=%s",
        filename,
        len(groupings),
        [(x[0], y[0]) for x, y in groupings],
    )

    for grouping in groupings:
        tag_1, indices_1 = grouping[0]
        tag_2, indices_2 = grouping[1]
        indices_1 = set(indices_1)
        indices_2 = set(indices_2)
        remained_df = df.drop(
            index=[x for x in df.index if (x not in indices_1 and x not in indices_2)]
        )
        data = anndata.AnnData(remained_df)
        new_grouping = [
            f"1-{tag_1}" if x in indices_1 else f"2-{tag_2}"
            for x in data.obs.index.tolist()
        ]
        test = de.test.rank_test(data, new_grouping)

        summary_output_file = get_output_filename(filename, f".{tag_1}_{tag_2}.out.csv")
        test.summary().to_csv(summary_output_file)
        LOGGER.info("summary saved, output=%s", summary_output_file)

        volcano_output_file = get_output_filename(
            filename, f".{tag_1}_{tag_2}.volcano.jpg"
        )
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
    assert opts.file.endswith(".csv"), "require .csv files"
    filename = abspath(opts.file)
    LOGGER.info("start rank test, input=%s", filename)
    rank_test(filename)


if __name__ == "__main__":
    main()
