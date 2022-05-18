import yfinance as yf


def get_close_val(tickers, start, end):
    data = yf.download(tickers=tickers, start=start, end=end)
    df = data.drop(columns=["Open", "High", "Low", "Close", "Volume"])
    df = df.droplevel(level=0, axis=1)
    return df.round(2)

