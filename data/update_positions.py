import os,pymongo
import logging
from tqdm import tqdm
from datetime import timedelta,datetime
import pandas as pd
import numpy as np
import warnings
import colorlog
warnings.simplefilter("ignore")
from fire import Fire
from bson.objectid import ObjectId

handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s[%(asctime)s - %(levelname)s]:%(message)s'))
logger = colorlog.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

mongo_uri=os.environ.get('MONGO_URI','mongodb://localhost:27020')
client = pymongo.MongoClient(mongo_uri)
db = client['edgar']
index=db['page_index']

def update_positions_collection(filter_dict={},output_col='positions_stockinfo'):
    filings=db['filings_13f']
    info=db['stock_info']
    past_info={}
    db.drop_collection(output_col)
    positions=db[output_col]
    res=filings.find(filter_dict,batch_size=100)
    bar = tqdm(res, desc='filings', total=res.count())
    def get_info(cusip):
        try:
            return past_info[cusip]
        except:
            past_info[cusip]=info.find_one({"cusip":cusip})
            return past_info[cusip]

    for f in bar:
        for p in f['positions']:
            i=get_info(p['cusip'])
            if len(i['close'])>0:
                close=pd.DataFrame(i['close'])
                quarter_idx=np.argmin(abs(close['Date']-f['quarter_date']))
                init_s=close.iloc


            else:
                logger.warning(f'No spot for cusip={p["cusip"]}')
        print(f)
        pass


if __name__ == '__main__':
    update_positions_collection()

