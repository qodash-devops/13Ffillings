import yfinance as yf
import os,pymongo
import logging
from tqdm import tqdm,trange
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor
import warnings
from fire import Fire
import pandas as pd
import colorlog
warnings.simplefilter("ignore")

handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s[%(asctime)s - %(levelname)s]:%(message)s'))
logger = colorlog.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


mongo_uri=os.environ.get('MONGO_URI','mongodb://edgar-miner:27020')
client = pymongo.MongoClient(mongo_uri)
db = client['edgar']
stock_info=db['stock_info']
positions_recap=db['stocks_positions']
positions=db['positions_view']


def update_positions_view_batch(batch_size=1000,fetch=False):
    if type(batch_size)==tuple:
        batch_id=batch_size[1]
        batch_size=batch_size[0]
    else:
        batch_id=0

    res=positions.aggregate([
        {"$sample":{"size":batch_size}},
        {
            "$lookup":{
                    "from": "positions_prices",
                    "localField": "_id",
                    "foreignField": "_id",
                    "as": "matched_docs"
                    }
            },
            {"$match": {"matched_docs": { "$eq": []}}},
        # {"$limit": batch_size},
    ])
    res=list(res)
    failures=[]
    pbar=tqdm(res, position=batch_id+1,desc=f'Batch ({batch_id})', leave=True)

    for r in pbar:
        try:
            update_position(r,fetch_yahoo=fetch)
        except KeyboardInterrupt:
            logger.warning('Earling stopped KeyboardInterrupt')
            raise KeyboardInterrupt
        except:
            failures.append(r['cusip'])
        pbar.set_postfix({"N_ERRORS":len(failures)})
        # pbar.update()
        # pbar.postfix[""]


def update_position(p,fetch_yahoo=True):

        ticker=stock_info.find_one({"cusip":p["cusip"]})

        if ticker['close']!=[] and 'info' in ticker.keys():
            spots=pd.DataFrame(ticker['close']).set_index('Date')
            spots.index = pd.to_datetime(spots.index)
        elif fetch_yahoo:
            ticker['ticker']=ticker['ticker'].replace('*','')
            t=yf.Ticker(ticker['ticker'])
            spots=t.history(period='3y')
            ticker['info']=t.info
            ticker['close'] = spots["Close"].copy()
            ticker['close'].index = ticker['close'].index.astype(str)
            ticker['close'] = ticker['close'].dropna().to_frame().reset_index().to_dict(orient='records')
            stock_info.update({"cusip":p["cusip"]},ticker)
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
        return p

def nearest(items, pivot):
    return min(items, key=lambda x: abs(x - pivot))

def update_all(batch_size=1000,fetch=False,nthreads=5):
    logger.info('Getting positions count...')
    n_positions=positions.count_documents({})
    n_existing=positions_recap.count_documents({})
    n_missing=n_positions-n_existing
    # n_missing=5000000
    logger.warning(f'{n_missing} missing positions')

    if nthreads>0:
        batches=[(batch_size,i) for i in range(int(n_missing/batch_size))]
        run_threaded(update_positions_view_batch,batches)
    else:
        for i in tqdm(range(int(n_missing/batch_size)),position=0,desc='Total',leave=False):
            update_positions_view_batch(batch_size=batch_size,fetch=fetch)

def run_threaded(f, my_iter,pool_size=4):
    with ThreadPoolExecutor(pool_size) as executor:
        results = list(tqdm(executor.map(f, my_iter),desc='Total', total=len(my_iter)))
    return results

if __name__ == '__main__':
    Fire({'all':update_all,'batch':update_positions_view_batch})