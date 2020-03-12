import scrapy
import re,os,pymongo
from ..items import EdgarItem
from .edgar import FilingSpider
from datetime import datetime
import sys
quarters={3:'Q1',6:'Q2',9:'Q3',12:'Q4'}
quarters_to_parse=[1,2,3,4]

mongo_uri=os.environ.get('MONGO_URI','mongodb://localhost:27020')
client = pymongo.MongoClient(mongo_uri)
db = client['edgar']
page_index=db['page_index']
filings=db['filings_13f']


def find_element(txt,tag='reportCalendarOrQuarter'):
    res=re.findall(f'<{tag}>[\s\S]*?<\/{tag}>', txt)
    res=[r.replace(f'<{tag}>','').replace(f'</{tag}>','') for r in res]
    return res

class MissingFilingSpider(FilingSpider):
    name = "edgarmissing"
    custom_settings={'DELTAFETCH_ENABLED':False}
    def start_requests(self):
        index=page_index.aggregate([{"$unwind":"$filings"},{'$project':{'url':"$filings"}}])
        present=filings.find({},{'docurl':1})
        index=[r['url'] for r in index]
        present=[r['docurl'] for r in present]
        missing=set(index).difference(set(present))
        self.logger.warning(f'Found {len(missing)} urls missing')
        for url in missing:
            res=filings.find_one({'docurl':url})
            if res is None:
                yield scrapy.Request(url=url, callback=self.parse_filing13F)
            else:
                self.logger.info(f'SKIPPING EXISTING URL:{url}')







