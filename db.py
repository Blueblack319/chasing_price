from config import config
import psycopg2
import yfinance as yf
import datetime as dt


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
                date DATE NOT NULL,
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


def insert_data(ticker, df, conn):
    try:
        cur = conn.cursor()
        ticker = ticker.replace("^", "_")
        cols = df.columns
        arg_string = ",".join(
            "('%s', '%s', '%s', '%s', '%s')" % (str(a).split()[0], b, c, d, e)
            for a, (b, c, d, e) in df.iterrows()
        )

        cur.execute(
            """
            INSERT INTO {0}(date, close, sma_20, sma_60, sma_120)
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

