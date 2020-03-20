import scrapy
import re,os,pymongo
from ..items import F13FilingItem,StockInfoItem
from .filings import MissingFilingSpider
from datetime import datetime
import time
from bson.objectid import ObjectId
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

class MergeSpider(MissingFilingSpider):
    name = "mergespider"
    custom_settings={'DELTAFETCH_ENABLED':False,'JOBDIR':'',
                     'ITEM_PIPELINES':{'edgar.pipelines.MergePipeline': 300},
                     'DEPTH_PRIORITY':1,
                     'SCHEDULER_DISK_QUEUE' :'scrapy.squeues.PickleFifoDiskQueue',
                     'SCHEDULER_MEMORY_QUEUE' : 'scrapy.squeues.FifoMemoryQueue'}

    stock_info={}
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

    def parse_filing13F(self, response):
        try:
            txt = response.body.decode()
            #Removing namepsaces from xml
            txt=re.sub('<n\S{1,2}:','<',txt)
            txt = re.sub('<\/n\S{1,3}:', '</', txt)
            txt = re.sub('<N\S{1,2}:', '<', txt)
            txt = re.sub('<\/N\S{1,3}:', '</', txt)
            txt = txt.replace('<eis:','<').replace('</eis:','</')
            report_type = find_element(txt, 'reportType')[0]
            assert '13F' in report_type
            filing = F13FilingItem()
            filing['quarter_date'] = find_element(txt, 'reportCalendarOrQuarter')[0]
            try:
                dt = datetime.strptime(filing['quarter_date'], '%m-%d-%Y')
                filing['year'] = dt.year
                filing['quarter'] = quarters[dt.month]
            except:
                self.logger.error(f"Parsing date:{filing['quarter_date']}")
            filing['filer_cik'] = find_element(txt, 'cik')[0]
            filer_name = find_element(txt, 'filingManager')[0]
            filing['filer_name'] = find_element(filer_name, 'name')[0]
            filing['docurl'] = response.url
            filing['filing_type'] = '13F'
            res_positions = []
            positions = find_element(txt, 'infoTable')
            if report_type=='13F NOTICE' and len(positions)==0:
                #removing notice from index
                page_index.update({}, {"$pull":{ "filings": response.url }},multi=True)
                self.crawler.stats.inc_value('Removed_page_indices')
                return

            if len(positions)==0:
                self.crawler.stats.inc_value('Number_of_filigs_without_position')
            if len(positions)>10000:
                self.logger.warning(f"Filing with {len(positions)} positions URL={response.url}")
            cusips=[]
            for p in positions:
                stock_name = find_element(p,  'nameOfIssuer')[0]
                stock_cusip = find_element(p,  'cusip')[0]
                cusips.append(stock_cusip)
                shares = find_element(p,  'shrsOrPrnAmt')[0]
                n_shares = find_element(shares,  'sshPrnamt')[0]
                try:
                    n_shares=float(n_shares.replace(' ',''))
                except:
                    pass
                put_call = find_element(p, 'putCall')
                res_positions.append({'name': stock_name, 'cusip': stock_cusip, 'symbol': '',
                                      'quantity': n_shares, 'callput': put_call})

            filing['positions'] = res_positions
            cusips=list(set(cusips))
            for cusip in cusips:
                info = db['stock_info'].find_one({"cusip": cusip})
                if info is None:
                    h = {"Content-Type": "application/x-www-form-urlencoded"}
                    request = scrapy.FormRequest(url='https://www.quantumonline.com/search.cfm',
                                                 formdata={"sopt": "cusip", "tickersymbol": cusip}, headers=h,
                                                 callback=self.parse_cusip)
                    request.cb_kwargs["cusip"] = cusip
                    yield request

            if len(res_positions)==0:
                self.logger.info(f'Filing processing ReportType="{report_type}" Npositions={len(res_positions)}  URL={response.url}')
            if len(positions) > 0:
                self.crawler.stats.inc_value("Number_positions",len(positions))
                yield filing

        except:
            self.logger.error(f'Processing url: {response.url}')
            self.logger.error(f"Reason {sys.exc_info()}")

