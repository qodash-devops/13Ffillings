import scrapy
import pandas as pd
import json
import re
from ..items import StockInfoItem
from ..es import ESDB
import urllib
import random
from datetime import datetime
es=ESDB()

spot_period="3y"

class StockInfo(scrapy.Spider):
    name = 'stockinfo'
    es_index= '13f_stockinfo'
    batch_size=5000
    custom_settings = {
        'CONCURRENT_REQUESTS_PER_IP':100,
        'ELASTICSEARCH_INDEX': es_index,
        'ELASTICSEARCH_TYPE': 'stockinfo',
        'ELASTICSEARCH_BUFFER_LENGTH': 10,
        'ELASTICSEARCH_UNIQ_KEY': 'cusip',
        'ITEM_PIPELINES' : {
                'edgar.pipelines.ElasticSearchPipeline': 200
        }

    }
    def _get_missing_cusips(self):

        all_cusips = es.get_filings_cusips()
        existing=es.get_info_cusips()
        missing = list(set(all_cusips)-set(existing))
        random.shuffle(missing)
        missing=missing[:min(self.batch_size,len(missing))]
        return missing

    def start_requests(self):
        es.create_index(self.es_index,settings={"settings": {"index.mapping.ignore_malformed": True , "index.mapping.total_fields.limit": 4000 }})
        missing_cusips=self._get_missing_cusips()
        n_missing=len(missing_cusips)
        self.logger.info(f'{n_missing} cusips to find ...')
        for c in missing_cusips:
            h={"Content-Type":"application/x-www-form-urlencoded"}
            request=scrapy.FormRequest(url='https://www.quantumonline.com/search.cfm',formdata={"sopt":"cusip","tickersymbol":c},headers=h,callback=self.parse_qo_cusip)
            request.cb_kwargs["cusip"]=c
            yield request

    def parse_qo_cusip(self, response,cusip):
        try:
            notFound=response.xpath("//*[contains(text(), 'Not Found!')]").get()
            if notFound is None:
                tmp=response.xpath("//*[contains(text(), 'Ticker Symbol:')]").get()
                ticker=tmp.split('\xa0')[0].split(':')[1].strip()
                exchange=tmp.split('\xa0')[-1].strip('</b>').split(':')[1].strip()
                ticker=ticker.replace('*','')
                req=scrapy.Request(url='https://finance.yahoo.com/quote/'+ticker,callback=self.parse_yahoo_info)
                req.cb_kwargs['cusip']=cusip
                req.cb_kwargs['ticker']=ticker
                yield req
            else:
                i = StockInfoItem()
                i['cusip'] = cusip
                i['ticker'] = ''
                i['exchange'] = ''
                i['status'] = 'NOTFOUND'
                yield
                self.crawler.stats.inc_value('ninfo')

        except:
            pass

    def accepted(self,response):

        print(response.text)

    def parse_yahoo_info(self,response,cusip,ticker):
        if 'consent.yahoo.com' in response.url:
            # by pass redirection to consent pop up GPDR if proxy in europe
            req=scrapy.FormRequest.from_response(response,
                                            formdata={"agree":"agree"},
                                            clickdata={'name': 'agree'},
                                            callback=self.parse_yahoo_info)
            req.cb_kwargs['cusip']=cusip
            req.cb_kwargs['ticker']=ticker
            yield req
            return
        html=response.text
        try:
            json_str = html.split('root.App.main =')[1].split(
                '(this)')[0].split(';\n}')[0].strip()
            data = json.loads(json_str)['context']['dispatcher']['stores']['QuoteSummaryStore']

            # info data
            new_data = json.dumps(data).replace('{}', 'null')
            new_data = re.sub(r'\{[\'|\"]raw[\'|\"]:(.*?),(.*?)\}', r'\1', new_data)
            info=json.loads(new_data)
            info=flatten(info,parent_key='info',sep='_') #flatten dict to one level
            info = {k:v for k,v in info.items() if not v is None and not isinstance(v,list)} # removing empty idems

            item=StockInfoItem()
            item['ticker']=ticker
            item['cusip']=cusip
            item['status']='FOUND'
            item['info']=info

            # price data
            _base_url = 'https://query1.finance.yahoo.com'
            period = spot_period.lower()
            params = {"range": period}
            params["interval"] = '1d'
            params["includePrePost"] = False
            # params["events"] = ""

            # Getting data from json
            url = "{}/v8/finance/chart/{}".format(_base_url, ticker)
            url = url+f'?{urllib.parse.urlencode(params)}'
            # data = _requests.get(url=url, params=params, proxies=proxy)
            req=scrapy.http.Request(url=url+ticker,callback=self.parse_yahoo_spot,)
            req.meta['item']=item
            self.crawler.stats.inc_value('ninfo')
            yield req
        except:
            self.logger.error(f'Getting info for {ticker}')
    def parse_yahoo_spot(self,response):
        item=response.meta['item']
        try:
            data=json.loads(response.text)
            data=data['chart']['result'][0]
            if 'timestamp' not in data.keys():
                self.logger.warning('No spot dates for: '+item['ticker'])
                return
            time_stamps=pd.to_datetime(data['timestamp'],unit='s')
            res_close=[]
            for i in range(len(time_stamps)):
                dt=time_stamps[i]
                res_close.append({'index':dt,'Close':data['indicators']['quote'][0]['close'][i],'Volume':data['indicators']['quote'][0]['volume'][i]})
            item['close']=res_close
            self.crawler.stats.inc_value('info_ticker_found')
        except:
            self.logger.error(f'Getting spot for {item["ticker"]}')
        yield item

def flatten(d, parent_key='', sep='.'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for v_i in v:
                if isinstance(v, dict):
                    items.extend(flatten(v_i, new_key, sep=sep).items())
                else:
                    items.append((new_key, v))
        else:
            items.append((new_key, v))
    return dict(items)