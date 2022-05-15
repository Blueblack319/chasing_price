import yfinance as yf
import datetime as dt
from db import connect, create_tables, insert_data
import pandas as pd
import numpy as np
from etfdb import extract_etfs_by_vol


def get_comp(df, interval):
    df_comp = np.array([])

    for idx, row in df.iterrows():
        if idx + interval >= df.index[-1]:
            break
        df_comp = np.append(
            df_comp, round(df["Adj Close"][idx] - df["Adj Close"][idx + interval], 2)
        )

    return pd.Series(df_comp)


def main():
    # set configuration for getting data
    # tickers = ["^KS11"]
    tickers = extract_etfs_by_vol()
    start = "2021-01-01"
    end = str(dt.datetime.now()).split()[0]
    short_window = 20
    middle_window = 60
    long_window = 120

    # get data
    # 20일, 60일, 90일, 120일 동안 하락한 or 상승한 자산
    data = yf.download(tickers=tickers, start=start, end=end)
    df = data.drop(columns=["Open", "High", "Low", "Close", "Volume"])
    df = df.droplevel(level=0, axis=1)
    df = df.round(2)

    conn = connect()
    create_tables(tickers, conn)

    for ticker in tickers:
        ticker_df = pd.DataFrame(data=df[ticker])
        ticker_df.set_axis(["Adj Close"], axis="columns", inplace=True)
        ticker_df["SMA20"] = ticker_df["Adj Close"].rolling(window=short_window).mean()
        ticker_df["SMA60"] = ticker_df["Adj Close"].rolling(window=middle_window).mean()
        ticker_df["SMA120"] = ticker_df["Adj Close"].rolling(window=long_window).mean()
        ticker_df.dropna(inplace=True)

        insert_data(ticker, ticker_df, conn)

    # 나중에 차이 구할때 쓰임
    # df["comp_20"] = get_comp(df, interval=short_window)
    # df["comp_60"] = get_comp(df, interval=middle_window)
    # df["comp_120"] = get_comp(df, interval=long_window)
    # df.reset_index(inplace=True)

    conn.close()


if __name__ == "__main__":
    main()
