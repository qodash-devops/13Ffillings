import data.yfinance as yf
import pytest

tickers=["PIPR","EDI"]
@pytest.mark.parametrize("ticker",tickers)
def test_ticker(ticker):
    t=yf.Ticker(ticker)
    i=t.info
    c=t.history(period="3y")

    assert 'volume' in i.keys()


