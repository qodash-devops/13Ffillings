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
    # res=filings.aggregate([ {"$unwind":"$positions"},
    #                         {"$lookup": {
    #                                "from": "stock_info",
    #                                "localField": "positions.cusip",
    #                                "foreignField": "cusip",
    #                                "as": "stock_info"}},{"$count":'_id'}
    #                         ])
    bar = tqdm(res, desc='filings', total=res.count())
    def get_info(p):
        cusip=p['cusip']
        try:
            i=past_info[cusip]
            pass
        except:
            past_info[cusip]=info.find_one({"cusip":cusip})
            i=past_info[cusip]
        try:
            close = pd.DataFrame(i['close'])
            quarter_idx=np.argmin(abs(close['Date']-f['quarter_date']))
            init_s=close.iloc[quarter_idx]['Close']
            prev_s=close.iloc[max(quarter_idx-64,0)]['Close']
            next_s=close.iloc[min(quarter_idx+64,len(close)-1)]['Close']
            p['prev_q_return']=init_s/prev_s-1
            p['next_q_return']=next_s/init_s-1
        except:
            p['prev_q_return']=np.nan
            p['next_q_return'] = np.nan
        try:
            p['q_rel_capi']=p['quantity']/i['market_cap']*100
        except:
            p['q_rel_capi']=np.nan
        try:
            p['q_rel_volume']=p['quantity']/i['volume']*100
        except:
            p['q_rel_volume']=np.nan
        return p
    i=0
    for f in bar:
        if i>100:
            break
        p=pd.DataFrame(f['positions'])
        p=p.apply(get_info,axis=1)
        i+=1
        # close=pd.DataFrame(i['close'])
        # quarter_idx=np.argmin(abs(close['Date']-f['quarter_date']))
        # init_s=close.iloc[quarter_idx]['Close']
        # prev_s=close.iloc[max(quarter_idx-64,0)]['Close']
        # next_s=close.iloc[min(quarter_idx+64,len(close)-1)]['Close']
        # p['prev_q_return']=init_s/prev_s-1
        # p['next_q_return']=next_s/init_s-1
        # p['q_rel_capi']=p['quantity']/i['market_cap']*100
        # p['q_rel_volume']=p['quantity']/i['volume']*100


if __name__ == '__main__':
    update_positions_collection()

