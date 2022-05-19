import datetime as dt
from db import (
    connect,
    create_tables,
    insert_tickers_data,
    query_data,
    add_column,
    show_tables_name,
    insert_comp_data,
)
import pandas as pd

# from etfdb import extract_etfs_by_vol
from get_yfin import get_close_val
from tools import get_comp


def main():
    """set configuration for getting data"""
    # tickers = ["^KS11", "AGG"]
    # tickers = extract_etfs_by_vol()
    start = "2004-01-01"
    end = str(dt.datetime.now()).split()[0]
    short_window = 20
    middle_window = 60
    long_window = 120
    column_info = {}

    """get data"""
    # 20일, 60일, 90일, 120일 동안 하락한 or 상승한 자산
    # df = get_close_val(tickers, start, end)
    # print(df)

    conn = connect()

    """Get tables' name"""
    table_list = show_tables_name(conn)
    tickers = [name[0] for name in table_list]

    """Create tables"""
    # create_tables(tickers, conn)

    """Insert data into tables"""
    # insert_tickers_data(tickers, conn)

    """Query data"""
    data = query_data("agg", conn, start, end)

    """나중에 차이 구할때 쓰임"""
    df_query = pd.DataFrame(data).set_axis(["Date", "Adj Close"], axis=1)
    df_query["comp_to_20"] = get_comp(df_query, interval=short_window)
    df_query["comp_to_60"] = get_comp(df_query, interval=middle_window)
    df_query["comp_to_120"] = get_comp(df_query, interval=long_window)
    df_query.drop(["Date", "Adj Close"], axis=1, inplace=True)

    """Add Columns"""
    # for ticker in tickers:
    #     add_column(
    #         conn, ticker, comp_to_20="float4", comp_to_60="float4", comp_to_120="float4"
    #     )
    """Insert compared data"""
    insert_comp_data("agg", df_query, conn)

    conn.close()


if __name__ == "__main__":
    main()
