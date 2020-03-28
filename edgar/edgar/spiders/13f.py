import scrapy
import re
from ..items import F13FilingItem
from datetime import datetime
import sys
from ..es import ESDB


es=ESDB()

def find_element(txt,tag='reportCalendarOrQuarter'):
    res=re.findall(f'<{tag}>[\s\S]*?<\/{tag}>', txt)
    res=[r.replace(f'<{tag}>','').replace(f'</{tag}>','') for r in res]
    return res

class FilingSpider(scrapy.Spider):
    name = "filings"
    es_index='13f_filings'
    pipeline = []
    custom_settings = {
        'ELASTICSEARCH_INDEX': es_index,
        'ELASTICSEARCH_TYPE': 'filing',
        'ELASTICSEARCH_BUFFER_LENGTH':10,
        'ELASTICSEARCH_UNIQ_KEY': 'filingurl'}
    stock_info={}

    def start_requests(self):
        es.create_index(self.es_index,existok=True)
        urls=es.get_filing_urls()
        for url in urls:
            url_data=es.get_url(url,index=self.es_index)
            if url_data is None:
                yield scrapy.Request(url=url, callback=self.parse_filing13F)
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
                filing['quarter_date']=dt
                filing['filingyear'] = dt.year
            except:
                self.logger.error(f"Parsing date:{filing['quarter_date']}")
            filing['filer_cik'] = find_element(txt, 'cik')[0]
            filer_name = find_element(txt, 'filingManager')[0]
            filing['filer_name'] = find_element(filer_name, 'name')[0]
            filing['filingurl'] = response.url
            filing['filing_type'] = '13F'
            res_positions = []
            positions = find_element(txt, 'infoTable')
            if report_type=='13F NOTICE' and len(positions)==0:
                #removing notice from index
                es.remove_url(response.url,index="13f_index")
                self.crawler.stats.inc_value('Removed_page_indices')
                return
            if len(positions)==0:
                self.crawler.stats.inc_value('Number_of_filigs_without_position')
            if len(positions)>10000:
                self.logger.warning(f"Filing with {len(positions)} positions URL={response.url}")
            cusips=[]
            for p in positions:
                titleclass = find_element(p, 'titleOfClass')[0]

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
                                      'quantity': n_shares, 'callput': put_call,'class':titleclass})

            filing['positions'] = res_positions

            if len(res_positions)==0:
                self.logger.info(f'Filing processing ReportType="{report_type}" Npositions={len(res_positions)}  URL={response.url}')
            if len(positions) > 0:
                self.crawler.stats.inc_value("Number_positions",len(positions))
                yield filing

        except:
            self.logger.error(f'Processing url: {response.url}')
            self.logger.error(f"Reason {sys.exc_info()}")

