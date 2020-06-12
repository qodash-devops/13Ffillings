from data.batches.batch import BatchProcess
import hashlib
import logging,colorlog
from edgar.edgar.settings import color_formatter
from edgar.edgar.es import ESDB,helpers
import time

es=ESDB()
logger = logging.getLogger('item_worker')
handler = colorlog.StreamHandler()
handler.setFormatter(color_formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

TARGET_INDEX='stock_prices'

try:
    logger.warning('Deleting index '+TARGET_INDEX)
    es.es.indices.delete(index=TARGET_INDEX)
except:
    logger.warning(TARGET_INDEX+' not found')
time.sleep(2)
es.create_index(TARGET_INDEX,settings={"settings": {"index.mapping.ignore_malformed": True }})



class StockPrices(BatchProcess):
    def __init__(self):
        q={
          "query":{"match_all": {}}
        }
        self.indexed_docs=0
        self.buffer=[]
        self.batch_size=1000
        super().__init__(q,index='13f_stockinfo',target_index=TARGET_INDEX)

    def _process(self,r):
        cusip=r['_source']['cusip']
        ticker=r['_source']['ticker']
        try:
            for close in r['_source']['close']:
                close['ticker']=ticker
                close['cusip']=cusip
                item_unique_key = close['cusip']+'-'+close['index']
                item_unique_key = item_unique_key.encode('utf-8')
                item_id = hashlib.sha1(item_unique_key).hexdigest()
                yield item_id,close
        except:
            pass
    def _update(self,id,r):
        index_action = {
            '_index': self.target,
            '_source': r,
            '_id':id
        }
        self.index_item(index_action)
    def index_item(self,action):
        self.buffer.append(action)
        if len(self.buffer)>self.batch_size:
            gen=helpers.parallel_bulk(es.es,self.buffer,thread_count=5,raise_on_error=False)
            for success,info in gen:
                if not success:
                    logger.error(info)
            self.indexed_docs+=len(self.buffer)
            self.buffer=[]


if __name__ == '__main__':
    B=StockPrices()
    B.run()



