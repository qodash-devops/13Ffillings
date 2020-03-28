import os
from elasticsearch import Elasticsearch,helpers
import logging
uri=os.environ.get('ES_SERVER','http://localhost:9200')

logger = logging.getLogger("elasticsearch")
logger.setLevel(logging.WARNING)

class ESDB:
    def __init__(self):
        self.es=Elasticsearch(hosts=[uri])


    def create_index(self,index,existok=True):
        if existok:
            if not self.es.indices.exists(index):
                self.es.indices.create(index)
        else:
            self.es.indices.create(index)

    def get_filing_urls(self):
        urls=[]
        resp=helpers.scan(self.es,index="13f_index",query={"_source":"url"},size=1000)
        for r in resp:
            urls.append(r["_source"]["url"])
        return urls
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
    def remove_url(self,url,index="13f_index"):
        q = {"query": {
                "bool": {
                    "should": [
                        {"match_phrase": {
                            "url": url
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


if __name__ == '__main__':
    DB=ESDB()
    DB.create_index('test_index')
    # res=(DB.get_filing_urls())
    # res=DB.get_url(res[0])
    # DB.remove_url(res['url'])
