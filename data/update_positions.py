import os,pymongo
import logging
from tqdm import tqdm
import pandas as pd
import numpy as np
import warnings
import colorlog
from fire import Fire
warnings.simplefilter("ignore")
from concurrent.futures import ProcessPoolExecutor,as_completed

from multiprocessing import Process


handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s[%(asctime)s - %(levelname)s]:%(message)s'))
logger = colorlog.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

output_col='positions_stockinfo'

redis_host='localhost'
import tqdm


def process_filing(f,db):
    def get_info(p):
        try:
            res={'quantity':p['quantity'],'cusip':p['cusip'],'ticker':p['ticker'],
                 'filer_name':f['filer_name'],'quarter_date':f['quarter_date'],'_id':p['_id_x'],'quarter':str(f['year'])+' - '+f['quarter']}

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
        db[output_col].bulk_write(updates)
    except:
        pass

def positions_worker(limit_n,skip_n,existing_positions):
    mongo_uri = os.environ.get('MONGO_URI', 'mongodb://localhost:27020')
    client = pymongo.MongoClient(mongo_uri)
    db = client['edgar']
    cur = db['filings_13f'].find({}).skip(skip_n).limit(limit_n)
    bar = tqdm.tqdm(cur, desc=f'positions queue ({skip_n}-{skip_n + limit_n})', total=limit_n,
                    position=int(skip_n / limit_n))
    for doc in bar:
        doc['stock_info'] = []
        for p in doc['positions']:
            if p['_id'] in existing_positions:
                continue
            i = db['stock_info'].find_one({'cusip': p['cusip']})
            if not i is None:
                doc['stock_info'].append(i)
        process_filing(doc, db)

class PositionsRunner:
    def __init__(self,n_cores=3):
        self.n_cores=n_cores
        self.processes=[]
    def run(self,reset_all=False):
        logger.info(f"Running with {self.n_cores} processes")
        mongo_uri = os.environ.get('MONGO_URI', 'mongodb://localhost:27020')
        client = pymongo.MongoClient(mongo_uri)
        db = client['edgar']
        db['stock_info'].ensure_index('cusip', pymongo.ASCENDING)
        if reset_all:
            logger.warning(f'Removing existing collection {output_col}')
            db.drop_collection(output_col)
        filings = db['filings_13f']
        res = filings.find({}, batch_size=1000)
        n_filigs = res.count()
        existing_positions=[r['_id'] for r in db[output_col].find({},{'_id':1})]
        logger.info(f'Number of existing positions:{len(existing_positions)}')
        batchsize = n_filigs // self.n_cores
        skips = range(0, self.n_cores * batchsize, batchsize)
        pool=ProcessPoolExecutor(self.n_cores)
        fututes=[]
        for s in skips:
            fututes.append(pool.submit(positions_worker,batchsize,s,existing_positions))
        for f in as_completed(fututes):
            logger.info(f'process completed ')




if __name__ == '__main__':
    Fire(PositionsRunner)


