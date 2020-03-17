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

def update_positions_collection(output_col='positions_stockinfo'):
    filings=db['filings_13f']
    info=db['stock_info']
    past_info={}
    db.drop_collection(output_col)
    positions=db[output_col]
    res=filings.find({},batch_size=100)
    n_filigs=res.count()
    res=filings.aggregate([{"$lookup": {
                                   "from": "stock_info",
                                   "localField": "positions.cusip",
                                   "foreignField": "cusip",
                                   "as": "stock_info"}}
                            ])
    bar = tqdm(res, desc='filings', total=n_filigs)
    def get_info(p):
        try:
            close = pd.DataFrame(p['close'])
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
            p['spot']=init_s
            p['spot_date']=close.index[quarter_idx]
        except:
            p['spot']=np.nan
            p['spot_date']=np.nan
        try:
            p['q_rel_capi']=p['quantity']/p['market_cap']*100
        except:
            p['q_rel_capi']=np.nan
        try:
            p['q_rel_volume']=p['quantity']/p['volume']*100
        except:
            p['q_rel_volume']=np.nan
        try:
            p['sector']=p['info']['sector']
        except:
            p['sector']=np.nan
        return p
    for f in bar:
        p=pd.DataFrame(f['positions'])
        i=pd.DataFrame(f['stock_info'])
        p=pd.merge(p,i,on='cusip')
        p=p.apply(get_info,axis=1)
        pos_list=p[['cusip','sector','ticker','quantity','spot','spot_date','prev_q_return','next_q_return','q_rel_capi','q_rel_volume']]
        pos_list['filer_name']=f['filer_name']
        pos_list['quarter_date']=f['quarter_date']
        pos_list['_id']=p['_id_x']
        pos_list=pos_list.to_dict(orient='records')
        updates=[pymongo.ReplaceOne({'_id':pos['_id']},pos,upsert=True) for pos in pos_list]
        positions.bulk_write(updates)


if __name__ == '__main__':
    update_positions_collection()

