from datetime import datetime
import colorlog
import json
import logging
from fire import Fire
import sys
import time
import os
import numpy as np
from scrapy_redis import get_redis
from edgar.edgar.items import PositionItem
from edgar.edgar.settings import color_formatter
from concurrent.futures import ProcessPoolExecutor,as_completed
import data.yfinance as yf
from .es import ESDB

redisuri=os.environ.get('REDIS_URI','redis://localhost:6379')
logger = logging.getLogger('item_worker')
handler = colorlog.StreamHandler()
handler.setFormatter(color_formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

positions_index='13f_positions'

class Stats:
    _data={}
    _last_log_time=0
    name=''
    def inc_value(self,key,val=1):
        try:
            self._data[key]+=val
        except:
            self._data[key]=val
    def log(self,interval):
        if time.time()-self._last_log_time>interval:
            logger.info(f"{self.name}>>Statistics:{self._data}")
            self._last_log_time=time.time()

def pool_worker(class_name,i):
    w=eval(class_name+'()')
    w.stats.name=class_name+'_'+str(i)
    w.run()

class RedisWorker:
    def __init__(self,keys,name='redis_worker'):
        self.redis = get_redis(url=redisuri)
        self.keys=keys
        self.timeout=10
        self.wait=1
        self.name=name
        self.stats=Stats()
        self.stats.name=name
    def run(self):
        logger.info(f'Starting worker:{self.keys}')
        processed = 0
        while True:
            self.stats.log(30)
            ret = self.redis.blpop(self.keys, self.timeout)
            if ret is None:
                self.stats.inc_value('idle_time(s)',self.wait)
                time.sleep(self.wait)
                continue
            source, data = ret
            try:
                item = json.loads(data)
                self.process_item(item)
                self.stats.inc_value('processed')
            except KeyboardInterrupt:
                break
            except Exception:
                logger.exception(f"Failed to load item {data}")
                continue


            processed += 1
    def process_item(self,item):
        raise NotImplemented

class FilingsWorker(RedisWorker):
    def __init__(self, keys=None):
        if keys is None:
            keys = ['positions:items']
        self.es=ESDB()
        super().__init__(keys=keys)
        self.log_interval=30
    def process_item(self,item):
        i = dict(item)
        positions_updates=[]
        for p in item['positions']:
            info = self.es.get_info(p['cusip'])
            if not info is None:
                update=self.updatePosition(p, i, info)
                if not update is None:
                    positions_updates.append(update)
        if len(positions_updates)>0:
            self.es.es.bulk(positions_updates,positions_index)
        self.stats.log(self.log_interval)

    def updatePosition(self, position,filing, stockinfo):
        assert position['cusip']==stockinfo['cusip']
        pos = PositionItem()
        try:
            pos['info']=stockinfo['info']
            pos['quarter_date']=filing['quarter_date']
            pos['quarter']=filing['quarter']
            pos['year']=filing['year']
            pos['filer_name']=filing['filer_name']
            pos['filer_cik']=filing['filer_cik']

            pos['quantity']=position['quantity']
            pos['instrumentclass']=position['class']
            pos['cusip']=position['cusip']
            pos['ticker']=stockinfo['ticker']
            quarter_idx=np.argmin(abs(np.array([pp['index'] for pp in stockinfo['close']])-filing['quarter_date']))
            pos['spot_date']=stockinfo['close'][quarter_idx]['index']
            pos['spot']=stockinfo['close'][quarter_idx]['Close']

            next_quarter_idx=min(quarter_idx + 64, len(stockinfo['close']) - 1)
            prev_quarter_idx=max(quarter_idx-64,0)
            pos['next_quarter_spot_date'] = stockinfo['close'][next_quarter_idx]['index']
            pos['next_quarter_spot'] = stockinfo['close'][next_quarter_idx]['Close']
            pos['past_quarter_spot_date'] = stockinfo['close'][prev_quarter_idx]['index']
            pos['past_quarter_spot'] = stockinfo['close'][prev_quarter_idx]['Close']

            keys = {'filing_id': pos['filing_id'], 'cusip': pos['cusip']}

            # update=pymongo.ReplaceOne(keys, pos, upsert=True)
            update={} #TODO implement update for elastic search
            self.stats.inc_value('positions')
            return update
        except KeyboardInterrupt:
            exit(1)
        except:
            logger.error(f"getting position reason={sys.exc_info()}")


class StockInfoWorker(FilingsWorker):
    def __init__(self):
        self.es=ESDB()
        super().__init__(keys=['stockinfo:items'])
        self.log_interval = 30
    def process_item(self,item):
        i = dict(item)
        filings = self.es.get_filings_position(i['cusip'])
        positions_updates=[]
        for f in filings:
            for p in f['positions']:
                if p['cusip'] == i['cusip']:
                    update=self.updatePosition(p, f, i)
                    if not update is None:
                        positions_updates.append(update)
        if len(positions_updates) > 0:
            self.es.es.bulk(positions_updates,)
        self.stats.log(self.log_interval)

class WorkerStarter:
    def run(self,positions=2,stockinfo=2):
        n_procs=positions+stockinfo
        pool=ProcessPoolExecutor(n_procs)
        futures=[]
        for i in range(positions):
            f=pool.submit(pool_worker,'PositionWorker',i)
            futures.append(f)
        for i in range(stockinfo):
            f=pool.submit(pool_worker,'StockInfoWorker',i)
            futures.append(f)
        for _ in as_completed(futures):
            pass
    def positions(self):
        W=FilingsWorker()
        W.run()
    def stockinfo(self):
        W=StockInfoWorker()
        W.run()


if __name__ == '__main__':
    Fire(WorkerStarter)