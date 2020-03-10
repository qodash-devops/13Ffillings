import yfinance as yf
import os,pymongo
import logging
from tqdm import tqdm
import warnings
warnings.simplefilter("ignore")

mongo_uri=os.environ.get('MONGO_URI','mongodb://localhost:27020')
client = pymongo.MongoClient(mongo_uri)
db = client['edgar']
stock_info=db['stock_info']
stock_prices=db['stock_prices']

chunk_split=lambda lst,n:[lst[i:i + n] for i in range(0, len(lst), n)]

def get_tickers():
    res=stock_info.find({"info.error":{"$exists":False},"info.securityType":"Common Stock"},{"info":1})
    t=[r["info"]["ticker"] for r in res]
    return t

def upload_stock_prices():
    tickers=get_tickers()
    logging.info(f'Getting history for {len(tickers)} tickers ...')

    for ticker in tqdm(tickers):
        try:
            t = yf.Ticker(ticker)
            res=t.history(period="3y")
            res = res["Close"]
            res.index = res.index.astype(str)
            close=res.dropna().to_dict()
            stock_prices.update({"_id":ticker},{"$set":{"Close":close}},upsert=True)
        except:
            logging.error(f'loading chunk {ticker}')


if __name__ == '__main__':
    upload_stock_prices()