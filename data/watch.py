import yfinance as yf
import os,pymongo
import logging
from tqdm import tqdm
from datetime import timedelta
import warnings
warnings.simplefilter("ignore")
from fire import Fire
import pandas as pd

mongo_uri=os.environ.get('MONGO_URI','mongodb://localhost:27020')
client = pymongo.MongoClient(mongo_uri)
db = client['edgar']
stock_info=db['stock_info']
positions_recap=db['stocks_positions']
positions=db['positions_view']


def update_positions_view():

    res=positions.aggregate([
        {
            "$lookup":{
                    "from": "positions_prices",
                    "localField": "_id",
                    "foreignField": "_id",
                    "as": "matched_docs"
                    }
            },
            {"$match": {"matched_docs": { "$eq": []}}}
    ])
    for r in tqdm(list(res)):
        r=update_position(r)




    res=list(res)
def update_position(p):
        try:
            ticker=stock_info.find_one({"cusip":p["cusip"]})

            if ticker['close']!=[] and 'info' in ticker.keys():
                spots=pd.DataFrame(ticker['close']).set_index('Date')
                spots.index = pd.to_datetime(spots.index)
            else:
                t=yf.Ticker(ticker['ticker'])
                spots=t.history(period='3y')
                ticker['info']=t.info
            quarter_date = nearest(spots.index, pd.to_datetime(p['quarter_date']))
            next_quarter = nearest(spots.index, pd.to_datetime(p['quarter_date']) + timedelta(days=30.5 * 3))
            past_quarter = nearest(spots.index, pd.to_datetime(p['quarter_date']) - timedelta(days=30.5 * 3))
            p['init_spot'] = spots['Close'].loc[quarter_date]
            p['quantity'] = float(p['quantity'])
            p['quarter_perf'] = (spots['Close'].loc[next_quarter] / spots['Close'].loc[quarter_date] - 1) * 100
            p['prev_quarter_perf'] = (spots['Close'].loc[quarter_date] / spots['Close'].loc[past_quarter] - 1) * 100
            try:
                p['marketcap_perc'] = float(p['quantity']) / float(ticker['info']['marketCap']) * 100
                p['sector'] = ticker['info']['sector']
                p['industry'] = ticker['info']['industry']
            except:
                pass
            p['volume_perc'] = float(p['quantity']) / float(ticker['info']['regularMarketVolume']) * 100

            positions_recap.update({'_id': p['_id']}, p, upsert=True)
        except:
            try:
                logging.error(f'Updating position : (ticker={ticker["ticker"]})')
            except:
                logging.error(f'Updating position : (cusip={p["cusip"]})')
        return p


def nearest(items, pivot):
    return min(items, key=lambda x: abs(x - pivot))


if __name__ == '__main__':
    update_positions_view()