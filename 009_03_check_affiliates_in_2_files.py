# -*- coding: utf-8 -*-
"""009-03 Check Affiliates in 2 files.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1X71o6Sc431Z9ZOjHvEQ-_g-pO64waE6x
"""

import pandas as pd

ar = pd.read_excel('AR.xlsx', sheet_name='Tracking Codes')
a = pd.read_excel('A.xlsx', sheet_name='sheet')

in_ar = ar[~ar['AffiliateID'].isin(a['Affiliate ID'])]
in_a = a[~a['Affiliate ID'].isin(ar['AffiliateID'])]

print(in_ar)
print(in_a)

with pd.ExcelWriter('comparasion.xlsx') as writer:
    in_ar.to_excel(writer, sheet_name='Only in AR', index=False)
    in_a.to_excel(writer, sheet_name='Only in A', index=False)