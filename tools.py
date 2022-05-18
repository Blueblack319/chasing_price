import numpy as np
import pandas as pd


def get_comp(df, interval):
    df_comp = np.array([])

    for idx, row in df.iterrows():
        if idx + interval >= df.index[-1]:
            break
        val = (
            (df["Adj Close"][idx] - df["Adj Close"][idx + interval])
            / df["Adj Close"][idx + interval]
        ) * 100
        df_comp = np.append(df_comp, round(val, 2))

    return pd.Series(df_comp)
