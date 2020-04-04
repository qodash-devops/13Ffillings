from edgar.edgar.es import ESDB
import pandas as pd
from datetime import datetime
import time
from data.batches.batch import BatchProcess,logger
from concurrent.futures import ProcessPoolExecutor,as_completed
from tqdm import tqdm
import logging
es=ESDB()

import collections

MAX_ES_TASKS_MIN=500

es.es.cluster.put_settings({
    "transient": {
        "script.max_compilations_rate" : f"{MAX_ES_TASKS_MIN}/1m"
    }
})

logger.setLevel(logging.DEBUG)


def flatten(d, parent_key='', sep='.'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        elif isinstance(v,list):
            for v_i in v:
                if isinstance(v, collections.MutableMapping):
                    items.extend(flatten(v_i, new_key, sep=sep).items())
                else:
                    items.append((new_key,v))
        else:
            items.append((new_key, v))
    return dict(items)


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
        super().__init__(q, '13f_stockinfo', '13f_positions')

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
            spot={}
            stockinfo=r['_source']
            spot['spot_date'] = stockinfo['close'][quarter_idx]['index']
            spot['spot'] = stockinfo['close'][quarter_idx]['Close']
            next_quarter_idx = min(quarter_idx + 64, len(stockinfo['close']) - 1)
            prev_quarter_idx = max(quarter_idx - 64, 0)
            spot['next_quarter_spot_date'] = stockinfo['close'][next_quarter_idx]['index']
            spot['next_quarter_spot'] = stockinfo['close'][next_quarter_idx]['Close']
            spot['past_quarter_spot_date'] = stockinfo['close'][prev_quarter_idx]['index']
            spot['past_quarter_spot'] = stockinfo['close'][prev_quarter_idx]['Close']
            spot['status']='identified'
            spot['ticker']=r['_source']['ticker']
            info=flatten(r['_source']['info'],parent_key='info',sep='_')
            info={k:v for k,v in info.items() if not isinstance(v,list)}
            spot={**spot,**info}


            id=(qd,cusip)
            res.append((id,spot))
        return res

    def _update(self,id,r):
        params = r
        script=""
        for k,v in params.items():
            if not v is None:
                k=k.replace('-','_')
                if type(v) == float:
                    script+=f"ctx._source.{k}={v};\n"
                elif type(v)== int:
                    script += f"ctx._source.{k}={v}L;\n"
                elif type(v)==str:
                    v=v.replace('"',"")
                    script += f"ctx._source.{k}=\"{v}\";\n"
        body = {
            "query": {
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
                                                        "cusip.keyword": id[1]
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
                                                            "gte": id[0],
                                                            "lte": id[0],
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
            },
            "script": {
                "inline": script,
                "lang": "painless"
            }
        }
        max_retries=3
        for nretry in range(max_retries):
            try:
                res={'task':'Not sent'}
                res = es.es.update_by_query(self.target, body=body, wait_for_completion=False,params={"conflicts": "proceed"})
                task=es.es.tasks.get(res['task'])
                assert not "error" in task.keys()
                break
            except:
                logger.debug(f'({nretry})Retrying id:{id} task:'+res['task'])
                time.sleep(2)
        if nretry==max_retries-1:
            logger.warning(f'{id} failed')

    def run(self,n_proc=1):
        inputs = self._get_input()
        if n_proc==1:
            return self.run_inputs(inputs)

        n=int(len(inputs)/n_proc)
        chunks=[inputs[i * n:(i + 1) * n] for i in range((len(inputs) + n - 1) // n)]
        pool=ProcessPoolExecutor(len(chunks))
        futures=[]
        for c in chunks:
            f=pool.submit(self.run_inputs,c)
            futures.append(f)
        for f in as_completed(futures):
            pass

    def run_inputs(self,inputs):

        i=0
        for r in tqdm(inputs,leave=False,position=1):
            results=self._process(r)
            for id,ri in results:
                self._update(id,ri)
            i+=1

if __name__ == '__main__':
    B=StockInfoJoin()
    B.run()