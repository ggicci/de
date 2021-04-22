#!/usr/bin/env python

import pandas as pd

left = ["Abl1", "Anapc1", "Anapc10", "Anapc11", "Anapc13", "Anapc2", "Anapc4", "Anapc5", "Anapc7", "Atm", "Atr", "Bub1", "Bub1B", "Bub3", "Ccna1", "Ccna2", "Ccnb1", "Ccnb2", "Ccnb3", "Ccnd1", "Ccnd2", "Ccnd3", "Ccne1", "Ccne2", "Ccnh", "Cdc14A", "Cdc14B", "Cdc16", "Cdc20", "Cdc23", "Cdc25A", "Cdc25B", "Cdc25C", "Cdc26", "Cdc27", "Cdc45", "Cdc6", "Cdc7", "Cdk1", "Cdk2", "Cdk4", "Cdk6", "Cdk7", "Cdkn1A", "Cdkn1B", "Cdkn1C", "Cdkn2A", "Cdkn2B", "Cdkn2C", "Cdkn2D", "Chek1", "Chek2", "Crebbp", "Cul1", "Dbf4", "E2F1", "E2F2", "E2F3", "E2F4", "E2F5", "Ep300", "Espl1", "Fzr1", "Gadd45A", "Gadd45B", "Gadd45G", "Gsk3B", "Hdac1", "Hdac2", "Mad1L1", "Mad2L1", "Mad2L2", "Mcm2", "Mcm3", "Mcm4", "Mcm5", "Mcm6", "Mcm7", "Mdm2", "Myc", "Orc1", "Orc2", "Orc3", "Orc4", "Orc5", "Orc6", "Pcna", "Pkmyt1", "Plk1", "Prkdc", "Pttg1", "Pttg2", "Rad21", "Rb1", "Rbl1", "Rbl2", "Rbx1", "Sfn", "Skp1", "Skp1P2", "Skp2", "Smad2", "Smad3", "Smad4", "Smc1A", "Smc1B", "Smc3", "Stag1", "Stag2", "Tfdp1", "Tfdp2", "Tgfb1", "Tgfb2", "Tgfb3", "Tp53", "Ttk", "Wee1", "Wee2", "Ywhab", "Ywhae", "Ywhag", "Ywhah", "Ywhaq", "Ywhaz", "Zbtb17"]

right = ['HMGCS1', 'HMGCR', 'MVK', 'PMVK', 'MVD', 'FDPS', 'FDFT1', 'SQLE', 'LSS', 'CYP51A1', 'MSMO1', 'NSDHL', 'SC5D', 'DHCR7', 'LDLR', 'PCSK9', 'VLDLR', 'SCARB1', 'LRP1', 'LRP2', 'NPC1L1', 'ABCA1', 'ABCG1', 'ABCG5', 'ABCG8', 'CYP7A1', 'CYP27A1', 'ABCB11', 'LIPA', 'NPC1', 'NPC2', 'STARD3', 'VAPA', 'VAPB', 'OSBPL5', 'SOAT1', 'LCAT', 'LIPG', 'PLTP', 'CETP', 'LPA', 'LIPC', 'APOA1', 'APOC1', 'APOB', 'APOC2', 'APOH', 'SORT1', 'LPL', 'ANGPTL3', 'ANGPTL4', 'SLC27A1', 'SLC27A2', 'SLC27A3', 'SLC27A4', 'SLC27A5', 'SLC27A6', 'CD36', 'FABP1', 'FABP2', 'FABP3', 'FABP4', 'FABP5', 'FABP6', 'FABP7', 'FABP12', 'ACSL1', 'ACSL3', 'ACSL4', 'ACSL5', 'ACSM1', 'ACSM2A', 'ACSM2B', 'ACSM3', 'ACSM4', 'ACSM5', 'ACSM6', 'ACSS1', 'ACSS2', 'ACSS3', 'ACSBG1', 'ACSBG2', 'CPT1A', 'CPT1B', 'CPT1C', 'CPT2', 'SLC25A20', 'ACADVL', 'ACADL', 'ACADM', 'ACADS', 'ACADSB', 'ECHS1', 'EHHADH', 'ECH1', 'ECHDC2', 'ECHDC3', 'HADH', 'HADHA', 'HADHB', 'ACAA1', 'ACAA2', 'ECI1', 'ECI2', 'ACOX2', 'CRAT', 'CROT', 'MLYCD', 'PHYH', 'HSD17B4', 'SCP2', 'FASN', 'ACACA', 'ACACB', 'ELOVL1', 'ELOVL2', 'ELOVL3', 'ELOVL4', 'ELOVL5', 'ELOVL6', 'ELOVL7', 'TECR', 'ACOT1', 'ACOT7', 'THEM4', 'THEM5', 'PPT1', 'PPT2', 'HSD17B12', 'BAAT', 'SCD', 'FADS1', 'FADS2']

right = [x.capitalize() for x in right]

left_set = set(left)
right_set = set(right)

dat = pd.read_csv('./neonatal.csv', index_col=0)

left_dat = dat
# left_dat = dat.drop(index=[x for x in dat.index if x not in left_set])
right_dat = dat.drop(index=[x for x in dat.index if x not in right_set])

ans = pd.DataFrame(0, index=left_dat.index, columns=right_dat.index, dtype=float)
for r in left_dat.index:
    for c in right_dat.index:
        row_series = left_dat.loc[r]
        col_series = right_dat.loc[c]
        ans.at[r, c] = row_series.corr(col_series)

ans.to_csv('./neonatal.all.csv')

