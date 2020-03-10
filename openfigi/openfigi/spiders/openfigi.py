import scrapy
import os
from ..items import OpenfigiItem
import pymongo
import json

mongo_uri=os.environ.get('MONGO_URI','mongodb://localhost:27020')
client = pymongo.MongoClient(mongo_uri)
db = client['edgar']
stock_info=db['stock_info']
filings=db['13F']

openfigi_apikey=''

chunk_split=lambda lst,n:[lst[i:i + n] for i in range(0, len(lst), n)]

class StockInfoSpider(scrapy.Spider):
    name = "openfigi"
    chunk_size=10
    def _get_missing_cusips(self):
        self.logger.info('Loading missing cusips from mongo...')
        filings.ensure_index([("positions.cusip",pymongo.DESCENDING)])
        all_cusips=filings.distinct("positions.cusip")
        all_cusips=list(all_cusips)
        present=[c['cusip'] for c in list(stock_info.find({},{"cusip":1}))]
        missing=[c for c in all_cusips if not c in present]
        return sorted(missing)

    def start_requests(self):
        missing_cusips=self._get_missing_cusips()
        chunks=chunk_split(missing_cusips,self.chunk_size)
        if len(missing_cusips)>100:
            self.logger.warning(f'loading {len(missing_cusips)} cusips in {len(chunks)} chunks')
        for c in chunks:
            job = [{'idType': 'ID_CUSIP', 'idValue': cusip} for cusip in c]
            openfigi_url = 'https://api.openfigi.com/v2/mapping'
            openfigi_headers = {'Content-Type': 'application/json'}
            if openfigi_apikey:
                openfigi_headers['X-OPENFIGI-APIKEY'] = openfigi_apikey
            request=scrapy.http.JsonRequest(url=openfigi_url, callback=self.parse_info,method='POST',
                                     headers=openfigi_headers,data=job)
            request.cb_kwargs['cusips']=c
            yield request

    def parse_info(self, response,cusips):
        jsonresponse = json.loads(response.body_as_unicode())
        for i in range(len(cusips)):
            I=OpenfigiItem()
            I['cusip']=cusips[i]
            if 'error' in jsonresponse[i].keys():
                I['info']=jsonresponse[i]
            else:
                info=[d for d in jsonresponse[i]['data'] if '/' not in d['ticker']]
                if info==[]:
                    info=jsonresponse[i]['data'][0]
                else:
                    info = info[0]
                I['info']=info
            yield I
