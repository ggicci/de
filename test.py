#!/usr/bin/env python

import anndata
import diffxpy.api as de

data = anndata.read_csv("ven.csv").transpose()
grouping = ['NOR' if x.startswith('pca1') else 'HF' for x in data.var.index.tolist()]
test = de.test.t_test(data, grouping)
test.summary().to_csv("ven.out.csv")
test.plot_volcano(corrected_pval=True, min_fc=1.05, alpha=0.05, size=20)
