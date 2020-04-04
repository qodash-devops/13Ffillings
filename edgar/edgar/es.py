import os
from elasticsearch import Elasticsearch,helpers
import logging
uri=os.environ.get('ES_SERVER','http://localhost:9201')

logger = logging.getLogger("elasticsearch")
logger.setLevel(logging.WARNING)
MAX_AGG_SIZE=3000000
class ESDB:
    settings = {}  # default settings for index mappings

    def __init__(self):
        self.es=Elasticsearch(hosts=[uri],retry_on_timeout=True)
        self.update_cluster_settings()
    def create_index(self,index,existok=True,settings=None):

        if settings is None:
            settings=self.settings
        if existok:
            if not self.es.indices.exists(index):
                self.es.indices.create(index,body=settings)
                self.es.indices.refresh(index)
            else:
                if settings!={}:
                    logger.warning(f'Updating setting for index:{index} :  {settings}')
                    self.es.indices.close(index=index)
                    self.es.indices.put_settings(settings, index=index)
                    self.es.indices.open(index)
                    self.es.indices.refresh()
        else:
            self.es.indices.create(index,body=settings)



    def get_index_urls(self):
        urls = []
        resp = helpers.scan(self.es, index="13f_index", query={"_source": "index"}, size=1000)
        for r in resp:
            urls.append(r["_source"]["index"])
        return list(set(urls))

    def get_filing_urls(self):
        urls=[]
        resp=helpers.scan(self.es,index="13f_index",query={"_source":"filingurl"},size=1000)
        for r in resp:
            urls.append(r["_source"]["filingurl"])
        return urls

    def get_filings_cusips(self):
        q={
              "size":0,
              "aggs" : {
                "cusip" : {"terms" : { "field" : "cusip.keyword","size" : MAX_AGG_SIZE }}
              }
            }
        resp=self.es.search(body=q,index='13f_positions')
        cusips=[r['key'] for r in resp['aggregations']['cusip']['buckets']]
        cusips=list(set(cusips))
        return cusips
    def get_info_cusips(self,index='13f_stockinfo'):
        cusips = []
        q = {
            "size": "0",
            "aggs": {
                "cusip": {
                    "terms": {
                        "field": "cusip.keyword",
                        "size": MAX_AGG_SIZE
                    }
                }
            }
        }
        resp = self.es.search(body=q,index=index)
        cusips=[r['key'] for r in resp['aggregations']['cusip']['buckets']]
        cusips = list(set(cusips))
        return cusips

    def get_url(self,url,index='13f_index',field_name='filingurl'):
        q={"query":{
                    "bool":{
                        "should":[
                           {"match_phrase": {
                              field_name: url
                            }}
                        ]
                    }
                }
            }
        try:
            resp=self.es.search(index=index,body=q,size=10)
            res=resp['hits']['hits'][0]['_source']
            return res
        except:
            return None


    def get_info(self,cusip):
        q = {"query": {
            "bool": {
                "should": [
                    {"match_phrase": {
                        "cusip": cusip
                    }}
                ]
            }
        }
        }
        try:
            resp = self.es.search(index="13f_stockinfo", body=q, size=10)
            res = resp['hits']['hits'][0]['_source']
            return res
        except:
            return None

    def get_positions(self,cusip):
        # TODO debug
        q={
                "query": {
                    "match" : {
                        "cusip" : {
                            "query" : cusip
                        }
                    }
                }
            }

        resp = helpers.scan(self.es, index='13f_positions', query=q, size=1000)
        positions = list(resp)
        return positions

    def remove_url(self,url,index="13f_index"):
        q = {"query": {
                "bool": {
                    "should": [
                        {"match_phrase": {
                            "filingurl": url
                        }}
                    ]
                }
            }
        }
        resp = self.es.search(index=index, body=q)
        for hit in resp['hits']['hits']:
            id=hit['_id']
            res=self.es.delete(index=index,id=id)
            assert res['result']=='deleted'


        pass
    def update_cluster_settings(self):
        self.es.cluster.put_settings({
                "persistent" : {
                    "search.max_buckets" : MAX_AGG_SIZE+1000,
                }
        })

if __name__ == '__main__':
    DB=ESDB()
    res=DB.get_info("464286871")
    # res=(DB.get_filing_urls())
    # res=DB.get_url(res[0])
    # DB.remove_url(res['url'])
