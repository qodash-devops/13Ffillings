from edgar.edgar.es import ESDB,helpers
import pandas as pd
from fire import Fire
import time
from data.batches.batch import BatchProcess,logger
from concurrent.futures import ProcessPoolExecutor,as_completed
from tqdm import tqdm
import logging
from datetime import datetime
from six import string_types
import hashlib


import sys
es=ESDB()
import collections

MAX_ES_TASKS_MIN=2000
TARGET_INDEX='13f_info_positions'

es.es.cluster.put_settings({
    "transient": {
        "script.max_compilations_rate" : f"{MAX_ES_TASKS_MIN}/1m"
    }
})
try:
    logger.warning('Deleting index '+TARGET_INDEX)
    es.es.indices.delete(index=TARGET_INDEX)
except:
    logger.warning(TARGET_INDEX+' not found')
time.sleep(2)
es.create_index(TARGET_INDEX,settings={
                                    "settings": {"index.mapping.ignore_malformed": True , "index.mapping.total_fields.limit": 4000 },
                                    "mappings":{
                                        "properties":{"positions":{"type":"nested"}}
                                    }
})

logger.setLevel(logging.DEBUG)



class StockInfoJoin(BatchProcess):
    def __init__(self):
        q={
            "query": {
                "exists": {
                    "field": "close"
                }
            }
        }
        qq = {"size": 0, "aggs": {"qd": {"terms": {"field": "quarter_date"}}}}
        self.quarters = es.es.search(qq, index='13f_positions')
        self.quarters = [q['key_as_string'] for q in self.quarters['aggregations']['qd']['buckets']]
        self.batch_size=1000
        self.buffer=[]
        self.indexed_docs=0
        super().__init__(q, '13f_stockinfo', TARGET_INDEX)

    def get_id(self, id):

        return item_id

    def _process(self,r):
        def dt(x):
            return datetime.strptime(x[:10], '%Y-%m-%d')
        cusip=r['_source']['cusip']
        stockinfo=es.get_info(cusip)
        if stockinfo is None:
            pos={'status':'not_identified'}
            return []
        res=[]
        for qd in self.quarters:
            # tmp=abs(np.array([dt(pp['index']) for pp in r['_source']['close']])-dt(qd))
            tmp=pd.DataFrame(r['_source']['close'])
            tmp['index'] = pd.to_datetime(tmp['index'])
            quarter_idx=abs(tmp['index']-dt(qd)).idxmin()
            quarter_info={}
            stockinfo=r['_source']
            quarter_info['spot_date'] = stockinfo['close'][quarter_idx]['index']
            quarter_info['spot'] = stockinfo['close'][quarter_idx]['Close']
            next_quarter_idx = min(quarter_idx + 64, len(stockinfo['close']) - 1)
            prev_quarter_idx = max(quarter_idx - 64, 0)
            quarter_info['next_quarter_spot_date'] = stockinfo['close'][next_quarter_idx]['index']
            quarter_info['next_quarter_spot'] = stockinfo['close'][next_quarter_idx]['Close']
            quarter_info['past_quarter_spot_date'] = stockinfo['close'][prev_quarter_idx]['index']
            quarter_info['past_quarter_spot'] = stockinfo['close'][prev_quarter_idx]['Close']
            quarter_info['status']='identified'
            quarter_info['ticker']=r['_source']['ticker']
            info=r['_source']['info']
            # info={k:v for k,v in info.items() if not isinstance(v,list)}
            quarter_info={**quarter_info,**info}

            id=(qd,cusip)
            res.append((id,quarter_info))
        return res

    def _update(self,id,r):
        quarter_date=id[0]
        cusip=id[1]
        q={
                "query":{
                     "bool": {
                        "filter": [
                            {
                                "bool": {
                                    "filter": [
                                        {
                                            "bool": {
                                                "should": [
                                                    {
                                                        "match_phrase": {
                                                            "cusip.keyword": cusip
                                                        }
                                                    }
                                                ],
                                                "minimum_should_match": 1
                                            }
                                        },
                                        {
                                            "bool": {
                                                "should": [
                                                    {
                                                        "range": {
                                                            "quarter_date": {
                                                                "gte": quarter_date,
                                                                "lte": quarter_date,
                                                                "time_zone": "America/New_York"
                                                            }
                                                        }
                                                    }
                                                ],
                                                "minimum_should_match": 1
                                            }
                                        }
                                    ]
                                }
                            }
                        ]
                    }

                }
            }

        positions=helpers.scan(es.es,q,index='13f_positions')
        for pos in positions:
            try:
                pos=pos['_source']
                pos['value_q_end']=pos['quantity']*r['spot']/1e6
                pos['value_past_q'] = pos['quantity'] * r['past_quarter_spot'] / 1e6
                pos['value_next_q'] = pos['quantity'] * r['past_quarter_spot'] / 1e6
                res_pos={**pos,**r}
                index_action = {
                    '_index': TARGET_INDEX,
                    '_source': res_pos
                }
                self.index_item(index_action)
            except KeyboardInterrupt:
                raise KeyboardInterrupt
            except:
                pass



    def index_item(self,action):
        item_unique_key = action['_source']['cusip']+'-'+action['_source']['quarter_date']+'-'+action['_source']['filer_cik']+'-'+str(action['_source']['quantity'])
        item_unique_key = item_unique_key.encode('utf-8')
        item_id = hashlib.sha1(item_unique_key).hexdigest()
        action['_id']=item_id
        self.buffer.append(action)
        if len(self.buffer)>self.batch_size:
            gen=helpers.parallel_bulk(es.es,self.buffer,thread_count=5,raise_on_error=False)
            for success,info in gen:
                if not success:
                    logger.error(info)
            self.indexed_docs+=len(self.buffer)
            self.buffer=[]
            # logger.info(f'{self.indexed_docs} indexed documents')



    def run(self,n_proc=1):
        inputs = self._get_input()
        if n_proc==1:
            return self.run_inputs(inputs,0)

        n=int(len(inputs)/n_proc)
        chunks=[inputs[i * n:(i + 1) * n] for i in range((len(inputs) + n - 1) // n)]
        pool=ProcessPoolExecutor(len(chunks))
        futures=[]
        for i,c in enumerate(chunks):
            f=pool.submit(self.run_inputs,c,i)
            futures.append(f)
        for f in as_completed(futures):
            pass

    def run_inputs(self,inputs,proc_i):
        for r in tqdm(inputs,leave=False,position=proc_i,desc='Process '+str(proc_i)):
            results=self._process(r)
            for id,ri in results:
                self._update(id,ri)

if __name__ == '__main__':
    Fire(StockInfoJoin)