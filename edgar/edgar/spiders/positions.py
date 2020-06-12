import scrapy
import re
from ..items import PositionItem,ItemList
from datetime import datetime
import sys
from ..es import ESDB
import random
es=ESDB()

def find_element(txt,tag='reportCalendarOrQuarter'):
    res=re.findall(f'<{tag}>[\s\S]*?<\/{tag}>', txt)
    res=[r.replace(f'<{tag}>','').replace(f'</{tag}>','') for r in res]
    return res

class FilingSpider(scrapy.Spider):
    name = "positions"
    es_index='13f_positions'
    pipeline = []
    custom_settings = {
        'ELASTICSEARCH_UNIQ_KEY':['filingurl','cusip','quantity'],
        'ELASTICSEARCH_INDEX': es_index,
        'ELASTICSEARCH_TYPE': 'position',
        'ELASTICSEARCH_BUFFER_LENGTH': 500,
        'ELASTICSEARCH_UNIQ_KEY': ['cusip','filingurl'],
        'ITEM_PIPELINES': {
            'edgar.pipelines.ElasticSearchPipeline': 300
        }

    }
    stock_info={}

    def start_requests(self):
        es.create_index(self.es_index,existok=True)
        urls=es.get_filing_urls()
        random.shuffle(urls)
        for url in urls:
            url_data=es.get_url(url,index=self.es_index)
            if url_data is None:
                req=scrapy.Request(url=url, callback=self.parse_filing13F)
                yield req
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
            quarter_date = find_element(txt, 'reportCalendarOrQuarter')[0]
            try:
                quarter_date = datetime.strptime(quarter_date, '%m-%d-%Y')
            except:
                self.logger.error(f"Parsing date:{quarter_date}")

            filer_cik = find_element(txt, 'cik')[0]
            filer_name = find_element(txt, 'filingManager')[0]
            filer_name = find_element(filer_name, 'name')[0]
            url= response.url
            res_positions = []
            positions = find_element(txt, 'infoTable')
            if report_type=='13F NOTICE' and len(positions)==0:
                #removing notice from index
                try:
                    es.remove_url(response.url,index="13f_index")
                except:
                    pass
                self.crawler.stats.inc_value('Removed_page_indices')
                return
            if len(positions)==0:
                self.crawler.stats.inc_value('Number_of_filigs_without_position')
            if len(positions)>10000:
                self.logger.warning(f"Filing with {len(positions)} positions URL={response.url}")

            publish_date=es.get_url(response.url,index="13f_index")['publishdate']

            positions_res=[]
            for p in positions:
                titleclass = find_element(p, 'titleOfClass')[0]
                stock_name = find_element(p,  'nameOfIssuer')[0]
                stock_cusip = find_element(p,  'cusip')[0]
                shares = find_element(p,  'shrsOrPrnAmt')[0]
                n_shares = find_element(shares,  'sshPrnamt')[0]
                value = find_element(p,'value')[0]
                try:
                    n_shares=float(n_shares.replace(' ',''))
                except:
                    pass
                put_call = find_element(p, 'putCall')
                pos_item=PositionItem()
                pos_item['filingurl']=response.url
                pos_item['filer_name']=filer_name
                pos_item['filer_cik']=filer_cik
                pos_item['quarter_date']=quarter_date
                pos_item['quantity']=n_shares
                pos_item['reported_value']=value
                pos_item['cusip']=stock_cusip
                pos_item['stockname']=stock_name
                pos_item['instrumentclass']=titleclass
                pos_item['put_call']=put_call
                pos_item['status']='scraped'
                pos_item['publishdate']=publish_date
                self.crawler.stats.inc_value('positions')
                positions_res.append(pos_item)
            f=ItemList()
            f['list']=positions_res
            yield f
        except:
            self.logger.error(f'Processing url: {response.url}')
            self.logger.error(f"Reason {sys.exc_info()}")

