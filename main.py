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
        for ticker in tickers:
            ticker = ticker.replace('^', '_')
            cur.execute(
            """
            CREATE TABLE IF NOT EXISTS {0} (
                date DATE NOT NULL,
                close float4 NOT NULL,
                sma_20 float4 NOT NULL,
                sma_60 float4 NOT NULL,
                sma_120 float4 NOT NULL
            )
            """.format(ticker))
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
def insert_one(ticker, df, conn):
    cur = conn.cursor()
    ticker = ticker.replace('^', '_')
    cols = df.columns
    
    for col in df.columns:
        cur.execute(
            """
            INSERT INTO {ticker}(date, close, sma_20, sma_60, sma_120)
            VALUES({df})
            """.format(ticker, )
        )


if __name__ == "__main__":
    tickers = ["^KS11"]
    start = "2017-01-01"
    end = str(dt.datetime.now()).split()[0]
    short_window = 20
    middle_window = 60
    long_window = 120

    data = yf.download(tickers=tickers, start=start, end=end)
    data_df = data.drop(columns=['Open', 'High', 'Low', 'Close', 'Volume'])
    data_df['SMA20'] = data_df['Adj Close'].rolling(window=short_window).mean()
    data_df['SMA60'] = data_df['Adj Close'].rolling(window=middle_window).mean()
    data_df['SMA120'] = data_df['Adj Close'].rolling(window=long_window).mean()
    data_df = data_df.dropna()
    data_df = data_df.round(decimals=2)
    
    
    conn = connect()
    create_tables(tickers, conn)
    insert_one(tickers[0], data_df, conn)