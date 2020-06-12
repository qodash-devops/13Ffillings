from data.batches.batch import BatchProcess
import hashlib
import logging,colorlog
from edgar.edgar.settings import color_formatter
from edgar.edgar.es import ESDB,helpers
from datetime import datetime

es=ESDB()
logger = logging.getLogger('item_worker')
handler = colorlog.StreamHandler()
handler.setFormatter(color_formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class PositionsChange(BatchProcess):
    def __init__(self):
        q={"query": {"match_all": {}}}
        self.indexed_docs=0
        self.buffer=[]
        self.batch_size=1000
        super().__init__(q,index='13f_info_positions',target_index='13f_info_positions')
    def _process(self,r):
        try:
            if 'quantity_chg' in r['_source'].keys():
                return
            q={ "sort":[{"publishdate":{"order":"asc"}}],
                "query": {
                    "bool": {
                      "must": [
                        { "match": { "cusip": r['_source']['cusip']}},
                        { "match": { "filer_cik":  r['_source']['filer_cik']  }}
                      ]
                    }
                  }
            }
            relevant_positions=es.es.search(body=q,index=self.index)
            relevant_positions=relevant_positions['hits']['hits']
            for i,pos in enumerate(relevant_positions):
                if 'quantity_chg' in pos['_source'].keys():
                    break
                elif i>0:
                    item_id=pos['_id']
                    last_date=relevant_positions[i-1]['_source']['publishdate']
                    last_qtity=relevant_positions[i-1]['_source']['quantity']
                    last_value=relevant_positions[i-1]['_source']['value_q_end']
                    res={}
                    res['quantity_chg']=pos['_source']['quantity']-last_qtity
                    res['value_chg']=pos['_source']['value_q_end']-last_value
                    res['days_since_last_publish'] = (datetime.strptime(pos['_source']['publishdate'][:10],
                                                                       '%Y-%m-%d') - datetime.strptime(last_date[:10],
                                                                                                       '%Y-%m-%d')).days
                    yield item_id,res
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except:
            pass
    def _update(self,id,r):
        # script=''
        # params={}
        # for k,v in r.items():
        #     script+=f'ctx._source.{k}=params.{k};\n'
        #     params[k]=v
        # q={"script":
        #        {
        #            "source":script,
        #            "lang":"painless",
        #            "params":params
        #        }}
        # res=es.es.update(index=self.target,id=id,body=q)
        action={
            '_op_type':'update',
            '_index': self.target,
            '_id':id,
            'doc':r
        }
        self.buffer.append(action)
        if len(self.buffer)>=self.batch_size:
            success,info=helpers.bulk(es.es,self.buffer)
            if not success:
                logger.error(info)
            self.buffer=[]


if __name__ == '__main__':
    B=PositionsChange()
    B.run(n_proc=1)



