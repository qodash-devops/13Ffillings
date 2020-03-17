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

# os.environ["MODIN_ENGINE"] = "ray"

handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s[%(asctime)s - %(levelname)s]:%(message)s'))
logger = colorlog.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

mongo_uri=os.environ.get('MONGO_URI','mongodb://localhost:27020')
client = pymongo.MongoClient(mongo_uri)
db = client['edgar']
index=db['page_index']
@profile
def update_positions_collection(output_col='positions_stockinfo'):
    filings=db['filings_13f']
    info=db['stock_info']
    past_info={}
    db.drop_collection(output_col)
    positions=db[output_col]
    res=filings.find({},batch_size=1000)
    n_filigs=res.count()
    res=filings.aggregate([{"$lookup": {
                                   "from": "stock_info",
                                   "localField": "positions.cusip",
                                   "foreignField": "cusip",
                                   "as": "stock_info"}}
                            ])
    bar = tqdm(res, desc='filings', total=n_filigs)
    quarter_indices={}
    @profile
    def get_info(p):
        try:
            res={'quantity':p['quantity'],'cusip':p['cusip'],'ticker':p['ticker'],
                 'filer_name':f['filer_name'],'quarter_date':f['quarter_date'],'_id':p['_id_x']}
            try:
                quarter_idx=quarter_indices[res['cusip']]
            except:
                quarter_idx=np.argmin(abs(np.array([pp['Date'] for pp in p['close']])-f['quarter_date']))
                quarter_indices[res['cusip']]=quarter_idx
            init_s=p['close'][quarter_idx]['Close']
            prev_s=p['close'][max(quarter_idx-64,0)]['Close']
            next_s=p['close'][min(quarter_idx+64,len(p['close'])-1)]['Close']
            res['prev_q_return']=init_s/prev_s-1
            res['next_q_return']=next_s/init_s-1
        except:
            res['prev_q_return']=np.nan
            res['next_q_return'] = np.nan
        try:
            res['spot']=init_s
            res['spot_date']=p['close'][quarter_idx]['Date']
        except:
            res['spot']=np.nan
            res['spot_date']=np.nan
        try:
            res['q_rel_capi']=p['quantity']/p['market_cap']*100
        except:
            res['q_rel_capi']=np.nan
        try:
            res['q_rel_volume']=p['quantity']/p['volume']*100
        except:
            res['q_rel_volume']=np.nan
        try:
            res['sector']=p['info']['sector']
        except:
            res['sector']=np.nan
        return res
    iter=0
    for f in bar:
        if iter>5000:
            break
        pos=pd.DataFrame(f['positions'])
        i=pd.DataFrame(f['stock_info'])
        try:
            pos=pd.merge(pos,i,on='cusip')
            pos_list=pos.apply(get_info,axis=1)
            updates=[pymongo.ReplaceOne({'_id':pos['_id']},pos,upsert=True) for pos in pos_list]
            positions.bulk_write(updates)
        except:
            pass
        iter+=1


if __name__ == '__main__':
    update_positions_collection()

