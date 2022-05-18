import datetime as dt
from db import connect, create_tables, insert_tickers_data, query_data
import pandas as pd
from etfdb import extract_etfs_by_vol
from get_yfin import get_close_val
from tools import get_comp


def main():
    """set configuration for getting data"""
    tickers = ["^KS11", "AGG"]
    # tickers = extract_etfs_by_vol()
    start = "2004-01-01"
    end = str(dt.datetime.now()).split()[0]
    short_window = 20
    middle_window = 60
    long_window = 120

    """get data"""
    # 20일, 60일, 90일, 120일 동안 하락한 or 상승한 자산
    # df = get_close_val(tickers, start, end)
    # print(df)

    conn = connect()
    # create_tables(tickers, conn)
    # insert_tickers_data(tickers, conn)

    data = query_data("agg", conn, start, end)
    df_query = pd.DataFrame(data).set_axis(["Date", "Adj Close"], axis=1)

    """나중에 차이 구할때 쓰임"""
    df_query["comp_to_20"] = get_comp(df_query, interval=short_window)
    df_query["comp_to_60"] = get_comp(df_query, interval=middle_window)
    df_query["comp_to_120"] = get_comp(df_query, interval=long_window)
    # df.reset_index(inplace=True)

    print(df_query[:30])
    conn.close()


if __name__ == "__main__":
    main()
