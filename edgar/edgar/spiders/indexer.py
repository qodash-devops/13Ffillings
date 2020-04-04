import scrapy
import re
from ..items import PageIndexItem
from ..es import ESDB
from datetime import datetime
quarters={3:'Q1',6:'Q2',9:'Q3',12:'Q4'}
quarters_to_parse=[1,2,3,4]

es=ESDB()

def find_element(txt,tag='reportCalendarOrQuarter'):
    res=re.findall(f'<{tag}>[\s\S]*?<\/{tag}>', txt)
    res=[r.replace(f'<{tag}>','').replace(f'</{tag}>','') for r in res]
    return res

class EdgarIndexSpider(scrapy.Spider):
    name = "indexer"
    es_index='13f_index'
    existing_indices=[]
    custom_settings = {
        'ELASTICSEARCH_INDEX' : es_index,
        'ELASTICSEARCH_TYPE' : 'page_index',
        'ELASTICSEARCH_UNIQ_KEY' : 'filingurl',
        'ITEM_PIPELINES': {
            'edgar.pipelines.ElasticSearchPipeline': 300
        }
    }

    def start_requests(self):
        es.create_index(self.es_index,existok=True)
        self.existing_indices = es.get_index_urls()
        years = self.settings.get('YEARS')
        urls = []
        for y in years:
            self.logger.info(f"Scraping YEAR={y}")
            for q in ['QTR' + str(i) for i in [1, 2, 3, 4]]:
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
                if not next_page in self.existing_indices:
                    yield response.follow(next_page, callback=self.parse_daily,dont_filter=True)


    def parse_daily(self, response):
        lines = response.text.split('\n')
        q = re.findall('QTR\d', response.url)[0]
        y = re.findall('/\d\d\d\d/', response.url)[0].strip('/')
        d = datetime.strptime(response.url.split('.')[-2], '%Y%m%d')


        for i in range(len(lines)):
            if '13F' in lines[i]:
                url = re.findall('edgar/data/[\s\S]*?.txt', lines[i])
                if len(url) == 0:
                    self.logger.warning(f'could not fetch url from :{lines[i]}')
                    continue
                url = 'https://www.sec.gov/Archives/' + url[0]

                item = PageIndexItem()
                item['index'] = response.url
                item['quarter'] = q
                item['publishdate'] = d
                item['year'] = y
                item['filingurl'] = url
                item['doc_type'] = lines[i].split('  ')[0]
                yield item



