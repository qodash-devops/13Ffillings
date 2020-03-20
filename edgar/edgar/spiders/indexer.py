import scrapy
import re,os,pymongo
from ..items import PageIndexItem
from datetime import datetime
import sys
quarters={3:'Q1',6:'Q2',9:'Q3',12:'Q4'}
quarters_to_parse=[1,2,3,4]

mongo_uri=os.environ.get('MONGO_URI','mongodb://localhost:27020')
client = pymongo.MongoClient(mongo_uri)
db = client['edgar']
page_index=db['page_index']


def find_element(txt,tag='reportCalendarOrQuarter'):
    res=re.findall(f'<{tag}>[\s\S]*?<\/{tag}>', txt)
    res=[r.replace(f'<{tag}>','').replace(f'</{tag}>','') for r in res]
    return res

class EdgarIndexSpider(scrapy.Spider):
    name = "edgarindex"
    custom_settings={'DELTAFETCH_ENABLED':False}
    pipeline=[]
    def start_requests(self):
        years = self.settings.get('YEARS')
        urls = []
        for y in years:
            self.logger.info(f"Scraping YEAR={y}")
            for q in ['QTR' + str(i) for i in [1, 2, 3, 4]]:
                self.logger.info(f"Scraping QUARTER={y}/{q}")
                url = f'https://www.sec.gov/Archives/edgar/daily-index/{y}/{q}/'
                urls.append(url)
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_quarter,dont_filter=True)


    def parse_quarter(self, response):
        links = response.css('a')
        for l in links:
            link_url = l.attrib['href']
            if ('form.' in link_url and '.idx' in link_url):
                next_page = response.urljoin(link_url)
                yield response.follow(next_page, callback=self.parse_daily,dont_filter=True)


    def parse_daily(self, response):
        lines = response.text.split('\n')
        q = re.findall('QTR\d', response.url)[0]
        y = re.findall('/\d\d\d\d/', response.url)[0].strip('/')
        d = datetime.strptime(response.url.split('.')[-2], '%Y%m%d')
        item=PageIndexItem()
        item['index']=response.url
        item['quarter']=q
        item['day']=d
        item['year']=y
        f_list=[]

        for i in range(len(lines)):
            if '13F' in lines[i]:
                url = re.findall('edgar/data/[\s\S]*?.txt', lines[i])
                if len(url) == 0:
                    self.logger.warning(f'could not fetch url from :{lines[i]}')
                    continue
                url = 'https://www.sec.gov/Archives/' + url[0]
                f_list.append(url)
        item['n_filings'] = len(f_list)
        item['filings']=f_list
        yield item


