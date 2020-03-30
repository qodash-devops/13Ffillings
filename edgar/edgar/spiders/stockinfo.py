import scrapy
import os
from ..items import StockInfoItem
from ..es import ESDB
from scrapy_redis.spiders import RedisSpider
es=ESDB()


class QuantumonlineSpider(scrapy.Spider):
    name = 'stockinfo'
    es_index= '13f_stockinfo'
    allowed_domains = ['quantumonline.com']
    custom_settings = {
        'ELASTICSEARCH_INDEX': es_index,
        'ELASTICSEARCH_TYPE': 'stockinfo',
        'ELASTICSEARCH_BUFFER_LENGTH': 10,
        'ELASTICSEARCH_UNIQ_KEY': 'cusip',
        'ITEM_PIPELINES' : {
                'edgar.pipelines.InfoPipeline':100,
                'edgar.pipelines.ElasticSearchPipeline': 200,
                'edgar.pipelines.PositionsInfoPipeline':300
        }

    }

    def _get_missing_cusips(self):

        all_cusips = es.get_filings_cusips()
        existing=es.get_info_cusips()
        missing = list(set(all_cusips)-set(existing))
        return missing

    def start_requests(self):
        es.create_index(self.es_index)
        missing_cusips=self._get_missing_cusips()
        n_missing=len(missing_cusips)
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
