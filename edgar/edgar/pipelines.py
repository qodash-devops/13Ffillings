import logging
import pymongo
from utils import cache
import requests
# openfigi_apikey='a1d6899c-553b-4ee4-bb21-364417efeac'
openfigi_apikey=''
class EdgarPipeline(object):
    collection_name = '13F'
    collection_stock_info='stock_info'
    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
    @classmethod
    def from_crawler(cls, crawler):
        ## pull in information from settings.py
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE')
        )
    def open_spider(self, spider):
        ## initializing spider
        ## opening db connection
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        ## clean up when spider is closed
        self.client.close()

    def process_item(self, item, spider):
        ## how to handle each filing
        i=dict(item)
        info=self.update_stock_info(item['positions'])
        key = {'docurl': i['docurl']}
        self.db[self.collection_name].update(key, i, upsert=True)
        logging.debug("Filing added to MongoDB")
        return item

    def update_stock_info(self,positions):
        cusips=[p['cusip'] for p in positions]
        res=self.db[self.collection_stock_info].find({'cusip':cusips})
        res=list(res)
        missing_cusips=list(set([c for c in cusips if c not in res]))
        api_cusips = self.openfigi(missing_cusips)
        for p in positions:
            self.db[self.collection_name].update({'cusip':cusips}, res, upsert=True)
        return res


    @staticmethod
    @cache.region('long_term', 'get_stock_info')
    def openfigi(cusips):
        if len(cusips)>10:
            chunks = [cusips[x:x + 10] for x in range(0, len(cusips), 10)]
            res=[]
            for c in chunks:
                r=EdgarPipeline.openfigi(c)
                res.append(r)


        job = [{'idType': 'ID_CUSIP', 'idValue': cusip} for cusip in cusips]
        openfigi_url = 'https://api.openfigi.com/v1/mapping'
        openfigi_headers = {'Content-Type': 'text/json'}
        if openfigi_apikey:
            openfigi_headers['X-OPENFIGI-APIKEY'] = openfigi_apikey
        response = requests.post(url=openfigi_url, headers=openfigi_headers,
                                 json=job)
        if response.status_code != 200:
            raise Exception('Bad response code {}'.format(str(response.status_code)))
        data = response.json()
        res = {}
        for i in range(len(cusips)):
            if not 'error' in data[i].keys():
                res[cusips[i]] = data[i]['data'][0]
            else:
                res[cusips[i]] = data[i]
        return res


if __name__ == '__main__':
    res=EdgarPipeline.openfigi(['464288240','171085','464287879'])