import yfinance as yf
import os,pymongo
import logging
from tqdm import tqdm
import warnings
warnings.simplefilter("ignore")
from fire import Fire
import pandas as pd

mongo_uri=os.environ.get('MONGO_URI','mongodb://localhost:27020')
client = pymongo.MongoClient(mongo_uri)
db = client['edgar']
stock_info=db['stock_info']
positions_prices=db['positions_prices']
positions=db['positions_view']
stock_prices=db['stock_prices']

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
    for r in res:
        ticker=stock_info.find_one({"cusip":r["cusip"]})
        spots=stock_prices.find_one({'_id':ticker['info']['ticker']})
        if spots is None:
            continue
        spots=pd.DataFrame(spots['Close']).set_index('Date')
        r['spots']=spots
    res=list(res)
    pass




if __name__ == '__main__':
    update_positions_view()