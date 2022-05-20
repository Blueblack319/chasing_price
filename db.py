from config import config
import psycopg2
import yfinance as yf
import datetime as dt
import pandas as pd


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        params = config()
        print("Connecting to the PostgreSQL database...")

        conn = psycopg2.connect(**params)

        cur = conn.cursor()

        print("PostgreSQL database version:")
        cur.execute("SELECT version()")

        db_version = cur.fetchone()
        print(db_version)

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            return conn


def create_tables(tickers, conn):
    try:
        cur = conn.cursor()
        print("Creating tables...")
        for ticker in tickers:
            ticker = ticker.replace("^", "_")
            cur.execute(
                """
            CREATE TABLE IF NOT EXISTS {0} (
                close_date DATE NOT NULL,
                close float4 NOT NULL,
                sma_20 float4 NOT NULL,
                sma_60 float4 NOT NULL,
                sma_120 float4 NOT NULL
            )
            """.format(
                    ticker
                )
            )
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def insert_ticker_data(ticker, df, conn):
    try:
        cur = conn.cursor()
        ticker = ticker.replace("^", "_")
        arg_string = ",".join(
            "('%s', '%s', '%s', '%s', '%s')" % (str(a).split()[0], b, c, d, e)
            for a, (b, c, d, e) in df.iterrows()
        )

        cur.execute(
            """
            INSERT INTO {0}(close_date, close, sma_20, sma_60, sma_120)
            VALUES
            """.format(
                ticker
            )
            + arg_string
        )
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    # finally:
    #     if conn is not None:
    #         conn.close()


def update_comp_data(ticker, df, conn):
    try:
        cur = conn.cursor()
        ticker = ticker.replace("^", "_")
        cols = df.columns[1:]

        for date, comp_20, comp_60, comp_120 in df.to_numpy():
            # date = date.strftime("%Y-%m-%d")
            print(type(date))
            cur.execute(
                f""" UPDATE {ticker}
                SET comp_to_20 = {comp_20},
                    comp_to_60 = {comp_60},
                    comp_to_120 = {comp_120}
                WHERE close_date::date = '{date}'"""
            )
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    # finally:
    #     if conn is not None:
    #         conn.close()


def insert_tickers_data(tickers, conn):
    for ticker in tickers:
        ticker_df = pd.DataFrame(data=df[ticker])
        ticker_df.set_axis(["Adj Close"], axis="columns", inplace=True)
        ticker_df["SMA20"] = ticker_df["Adj Close"].rolling(window=short_window).mean()
        ticker_df["SMA60"] = ticker_df["Adj Close"].rolling(window=middle_window).mean()
        ticker_df["SMA120"] = ticker_df["Adj Close"].rolling(window=long_window).mean()
        ticker_df.dropna(inplace=True)

        insert_ticker_data(ticker, ticker_df, conn)


def query_data(ticker, conn, start_date, end_date):
    query_sql = f"SELECT close_date, close FROM {ticker} WHERE close_date BETWEEN '{start_date}' AND '{end_date}' ORDER BY close_date DESC"

    try:
        cur = conn.cursor()
        print("Query data...")
        cur.execute(query_sql)
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    # finally:
    #     if conn is not None:
    #         conn.close()


def add_column(conn, table, **column_info):
    name = table.replace("^", "_")

    try:
        cur = conn.cursor()
        print(f"Add columns into {name} table...")

        for col_name, col_type in column_info.items():
            cur.execute(
                f"""
                ALTER TABLE {name} 
                ADD COLUMN IF NOT EXISTS {col_name} {col_type};
                """
            )

        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def show_tables_name(conn):
    try:
        cur = conn.cursor()
        print(f"Get tables' name...")

        cur.execute(
            f"""
            SELECT table_name
            FROM information_schema.tables 
            WHERE table_schema= 'public' 
            ORDER BY table_name;
            """
        )
        tables = cur.fetchall()
        cur.close()
        return tables
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
