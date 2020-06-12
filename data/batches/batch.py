from edgar.edgar.es import ESDB
import elasticsearch.helpers as es_helpers
from concurrent.futures import ProcessPoolExecutor,as_completed
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
        self.n_proc=1
        if target_index is None:
            self.target=index
        else:
            self.target=target_index
        print(f'Starting to process {self.totcount} documents')
    def _get_input(self):
        resp=es_helpers.scan(es.es,self.q,index=self.index)
        # for r in resp:
        #     yield r
        if self.totcount<100000:
            return list(resp)
        else:
            return resp
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

    def run(self,n_proc=1):
        inputs = self._get_input()
        if n_proc==1:
            return self.run_inputs(inputs,0)
        self.n_proc=n_proc
        n=int(self.totcount/n_proc)
        logger.info('Reading inputs')
        read_input=[]
        for i in tqdm(inputs,total=self.totcount):
            read_input.append(i)
        inputs=read_input
        self.chunks=[inputs[i * n:(i + 1) * n] for i in range((len(inputs) + n - 1) // n)]
        pool=ProcessPoolExecutor(len(self.chunks))
        futures=[]
        for i,c in enumerate(self.chunks):
            f=pool.submit(self.run_inputs,c,i)
            futures.append(f)
        for f in as_completed(futures):
            pass

    def run_inputs(self,inputs,proc_i):
        if isinstance(inputs,list):
            prog=tqdm(inputs,leave=False,position=proc_i,desc='Process '+str(proc_i))
        else:
            n_chunks=self.n_proc
            prog = tqdm(inputs, leave=False, position=proc_i,total=int(self.totcount/n_chunks), desc='Process ' + str(proc_i))
        for r in prog:
            results=self._process(r)
            for id,ri in results:
                self._update(id,ri)
