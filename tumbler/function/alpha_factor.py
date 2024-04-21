import numpy as np


def factor_zscore(df):
    dates = list(set([x[0] for x in list(df.index)]))
    dates.sort()

    for d in dates:
        val = np.array(df.loc[d])
        df.loc[d] = (val - np.mean(val)) / np.std(val)
    return df
