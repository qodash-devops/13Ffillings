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

class MissingFilingSpider(scrapy.Spider):
    name = "edgarfilings"
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

    def parse_filing13F(self, response):
        txt = response.body.decode()
        report_type = find_element(txt, 'reportType')[0]
        assert '13F' in report_type
        filing = EdgarItem()
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
        positions = find_element(txt, 'ns1:infoTable')
        ns = 'ns1:'
        if len(positions) == 0:
            positions = find_element(txt, 'infoTable')
            ns = ''
        for p in positions:
            stock_name = find_element(p, ns + 'nameOfIssuer')[0]
            stock_cusip = find_element(p, ns + 'cusip')[0]
            shares = find_element(p, ns + 'shrsOrPrnAmt')[0]
            n_shares = find_element(shares, ns + 'sshPrnamt')[0]
            put_call = find_element(txt, 'putCall')
            res_positions.append({'name': stock_name, 'cusip': stock_cusip, 'symbol': '',
                                  'quantity': n_shares, 'callput': put_call})

        filing['positions'] = res_positions
        if len(positions) > 0:
            yield filing



