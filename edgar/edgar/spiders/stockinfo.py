import scrapy
import os
from ..items import StockInfoItem
import pymongo
from random import sample
mongo_uri=os.environ.get('MONGO_URI','mongodb://localhost:27020')
client = pymongo.MongoClient(mongo_uri)
db = client['edgar']
stock_info=db['stock_info']
filings=db['filings_13f']



class QuantumonlineSpider(scrapy.Spider):
    name = 'stockinfo'
    allowed_domains = ['quantumonline.com']
    batch_size=5000
    # start_urls = ['https://www.quantumonline.com/search.cfm']
    custom_settings = {'DELTAFETCH_ENABLED': False,'JOBDIR':'',
                       'ITEM_PIPELINES':{'edgar.pipelines.PositionsPipeline': 300}
                       }
    def _get_missing_cusips(self):
        self.logger.info('Loading missing cusips from mongo...')
        filings.ensure_index([("positions.cusip", pymongo.DESCENDING)])
        all_cusips = filings.distinct("positions.cusip")
        all_cusips = list(all_cusips)
        present = [c['cusip'] for c in list(stock_info.find({}, {"cusip": 1}))]
        missing = [c for c in all_cusips if (not c in present) and (not c is None) ]
        return sorted(missing)

    def start_requests(self):
        missing_cusips=self._get_missing_cusips()
        n_missing=len(missing_cusips)
        if n_missing>self.batch_size:
            missing_cusips=sample(missing_cusips,self.batch_size)
        if len(missing_cusips)>100:
            self.logger.warning(f'loading {len(missing_cusips)} cusips , ({n_missing}) total missing')
        for c in missing_cusips:
            h={"Content-Type":"application/x-www-form-urlencoded"}
            request=scrapy.FormRequest(url='https://www.quantumonline.com/search.cfm',formdata={"sopt":"cusip","tickersymbol":c},headers=h,callback=self.parse_cusip)
            request.cb_kwargs["cusip"]=c
            yield request


    def parse_cusip(self, response,cusip):
        try:
            notFound=response.xpath("//*[contains(text(), 'Not Found!')]").get()
            if notFound is None:
                tmp=response.xpath("//*[contains(text(), 'Ticker Symbol:')]").get()
                ticker=tmp.split('\xa0')[0].split(':')[1].strip()
                exchange=tmp.split('\xa0')[-1].strip('</b>').split(':')[1].strip()
                i=StockInfoItem()
                i['cusip']=cusip
                i['ticker']=ticker.replace('*','')
                i['exchange']=exchange
                i['status']='OK'
                yield i
            else:
                i = StockInfoItem()
                i['cusip'] = cusip
                i['ticker'] = ''
                i['exchange'] = ''
                i['status'] = 'NOTFOUND'
                yield i

        except:
            pass
