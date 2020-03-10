import yfinance as yf
import os,pymongo
import logging
mongo_uri=os.environ.get('MONGO_URI','mongodb://localhost:27020')
client = pymongo.MongoClient(mongo_uri)
db = client['edgar']
stock_info=db['stock_info']
stock_prices=db['stock_prices']


def get_tickers():
    res=stock_info.find({"info.error":{"$exists":False},"info.securityType":"Common Stock"},{"info":1})
    t=[r["info"]["ticker"] for r in res]
    return t


def upload_stock_prices():
    tickers=get_tickers()
    logging.info(f'Getting history for {len(tickers)} tickers ...')
    t=yf.Tickers(tickers)
    res=t.history(period="3y")
    res = res["Close"]
    res.index = res.index.astype(str)
    for t in res.columns:
        close=res[t].dropna().to_dict()
        stock_prices.update({"_id":t},{"$set":{"Close":close}},upsert=True)
    return res

if __name__ == '__main__':
    upload_stock_prices()