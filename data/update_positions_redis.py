import os,pymongo
import logging
from tqdm import tqdm
from datetime import timedelta,datetime
import pandas as pd
import numpy as np
import warnings
import colorlog
warnings.simplefilter("ignore")
import rq
from redis import Redis
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor,as_completed



handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s[%(asctime)s - %(levelname)s]:%(message)s'))
logger = colorlog.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.INFO)



redis_host='localhost'
import tqdm


def process_filing(f,db):
    def get_info(p):
        try:
            res={'quantity':p['quantity'],'cusip':p['cusip'],'ticker':p['ticker'],
                 'filer_name':f['filer_name'],'quarter_date':f['quarter_date'],'_id':p['_id_x']}

            quarter_idx=np.argmin(abs(np.array([pp['Date'] for pp in p['close']])-f['quarter_date']))
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

    pos=pd.DataFrame(f['positions'])
    i=pd.DataFrame(f['stock_info'])
    try:
        pos=pd.merge(pos,i,on='cusip')
        pos_list=pos.apply(get_info,axis=1)
        updates=[pymongo.ReplaceOne({'_id':pos['_id']},pos,upsert=True) for pos in pos_list]
        db['positions_stockinfo'].bulk_write(updates)
    except:
        pass

def process_cursor(pipeline,skip_n, limit_n):
    mongo_uri = os.environ.get('MONGO_URI', 'mongodb://localhost:27020')
    client = pymongo.MongoClient(mongo_uri)
    db = client['edgar']
    # npipeline = pipeline + [{'$skip': skip_n}, {'$limit': limit_n}]
    # cur = db['filings_13f'].aggregate(npipeline)
    cur=db['filings_13f'].find({}).skip(skip_n).limit(limit_n)
    bar=tqdm.tqdm(cur,desc=f'positions queue ({skip_n}-{skip_n+limit_n})',total=limit_n,position=int(skip_n/limit_n))
    for doc in bar:
        doc['stock_info']=[]
        for p in doc['positions']:
            i=db['stock_info'].find_one({'cusip':p['cusip']})
            if not i is None:
                doc['stock_info'].append(i)
        process_filing(doc,db)

def enque_positions(pipeline,n_cores=3,doc_count=3000):
    batchsize = doc_count // n_cores
    skips = range(0, n_cores * batchsize, batchsize)

    pool = ProcessPoolExecutor(n_cores)
    futures = []
    # pool=mp.Pool(n_cores)
    # res=pool.map(process_cursor,[(pipeline,s,batchsize) for s in skips])
    for s in skips:
        futures.append(pool.submit(process_cursor,pipeline,s, batchsize))
    for f in as_completed(futures):
        logger.info('future finished')



def update_positions_collection(output_col='positions_stockinfo',n_cores=3):
    mongo_uri = os.environ.get('MONGO_URI', 'mongodb://localhost:27020')
    client = pymongo.MongoClient(mongo_uri)
    db = client['edgar']
    db['stock_info'].ensure_index('cusip',pymongo.ASCENDING)

    filings=db['filings_13f']
    info=db['stock_info']
    past_info={}
    db.drop_collection(output_col)
    positions=db[output_col]
    res=filings.find({},batch_size=1000)
    n_filigs=res.count()
    pipeline=[{"$lookup": {"from": "stock_info","localField": "positions.cusip",
                            "foreignField": "cusip","as": "stock_info"}}]
    enque_positions(pipeline,doc_count=n_filigs,n_cores=n_cores)



if __name__ == '__main__':
    update_positions_collection(n_cores=5)

