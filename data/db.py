import pymongo,os
from concurrent.futures import ProcessPoolExecutor,as_completed,ThreadPoolExecutor
class DB(object):
    def __init__(self):
        self.mongo_uri = os.environ.get('MONGO_URI', 'mongodb://localhost:27020')
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client['edgar']

    def aggregate(self,pipeline,collection,batchsize=1000,cursor=True):
        res=self.db[collection].aggregate(pipeline,batchsize)
        if not cursor:
            res=list(res)
        return res

    def multiproc_aggregate(self,pipeline,collection,doc_count=130000,n_cores = 10 ):
        # number of splits (logical cores of the CPU-1)
        batchsize=doc_count//n_cores
        skips = range(0, n_cores * batchsize, batchsize)
        def process_cursor(skip_n, limit_n):
            npipeline=pipeline+[{'$skip':skip_n},{'$limit':limit_n}]
            cur=self.db[collection].aggregate(npipeline)
            return cur
            # for doc in cur:
            #     yield doc
        pool=ThreadPoolExecutor(n_cores)
        futures=[]
        cursors=[]
        for s in skips:
            futures.append(pool.submit(process_cursor,s,batchsize))
        for f in as_completed(futures):
           cursors.append(f.result())
        cursors=zip(*tuple(cursors))
        for docs in cursors:
            for doc in docs:
                yield doc

def check_run_time(cursor):
    t1=time.time()
    n=0
    for r in res:
        n+=1
    print(f'Run time for {n} docs:{round(time.time()-t1)}s')
if __name__ == '__main__':
    import time
    p=[{"$lookup": {"from": "stock_info",
                   "localField": "positions.cusip",
                   "foreignField": "cusip",
                   "as": "stock_info"}}]
    db=DB()
    res=db.multiproc_aggregate(p,'filings_13f',doc_count=173039,n_cores=3)
    check_run_time(res)
