from edgar.edgar.es import ESDB
import elasticsearch.helpers as es_helpers
from datetime import datetime
import numpy as np
import logging
import colorlog
from edgar.edgar.settings import color_formatter

from tqdm import tqdm
es=ESDB()

logger = logging.getLogger('item_worker')
handler = colorlog.StreamHandler()
handler.setFormatter(color_formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)



class BatchProcess:
    def __init__(self,q,index,target_index=None):
        self.totcount=es.es.count(q,index=index)['count']
        self.q=q
        self.index=index
        self.updates=[]
        self.chunk_size=100
        if target_index is None:
            self.target=index
        else:
            self.target=target_index
        print(f'Starting to process {self.totcount} documents')
    def _get_input(self):
        resp=es_helpers.scan(es.es,self.q,index=self.index)
        # for r in resp:
        #     yield r
        return list(resp)
    def _process(self,r):
        raise NotImplemented
    def _update(self,id,r):
        params = r
        script=""
        for k in params.keys():
            script+=f"ctx._source.{k}=params.{k};\n"
        body = {
            "script": {
                "source": script,
                "lang": "painless",
                "params": params
            }
        }
        update={
            '_op_type': 'update',
            '_index': self.target,
            '_id': id,
            'doc': body
        }
        self.updates.append(update)
        if len(self.updates)>self.chunk_size:
            res=es_helpers.bulk(es.es,self.updates)
            self.updates=[]
            # es.es.update(self.target, id, body=body)



    def run(self):
        allinputs=self._get_input()
        i=0
        for r in tqdm(allinputs,total=self.totcount):
            results=self._process(r)
            for id,ri in tqdm(results,position=1,leave=False,desc=f'processing item:{i}'):
                self._update(id,ri)
            i+=1



