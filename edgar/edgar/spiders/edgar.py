import scrapy
import re
from ..items import EdgarItem
from datetime import datetime

quarters={3:'Q1',6:'Q2',9:'Q3',12:'Q4'}

def find_element(txt,tag='reportCalendarOrQuarter'):
    res=re.findall(f'<{tag}>[\s\S]*?<\/{tag}>', txt)
    res=[r.replace(f'<{tag}>','').replace(f'</{tag}>','') for r in res]
    return res

class FilingSpider(scrapy.Spider):
    name = "edgar"

    def start_requests(self):
        years=self.settings.get('YEARS')
        urls=[]
        for y in years:
            self.logger.info(f"Scraping YEAR={y}")
            for q in ['QTR'+str(i) for i in [1,2,3,4]]:
                self.logger.info(f"Scraping QUARTER={y}/{q}")
                url=f'https://www.sec.gov/Archives/edgar/daily-index/{y}/{q}/'
                urls.append(url)
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_quarter)

    def parse_quarter(self, response):
        links = response.css('a')
        for l in links:
            link_url = l.attrib['href']
            if ('form.' in link_url and '.idx' in link_url):
                next_page = response.urljoin(link_url)
                yield scrapy.Request(next_page, callback=self.parse_daily)
    def parse_daily(self,response):
        lines=response.text.split('\n')
        urls=[]
        for i in range(len(lines)):
            if '13F' in lines[i]:
                url=re.findall('edgar/data/[\s\S]*?.txt',lines[i])
                if len(url)==0:
                    self.logger.warning(f'could not fetch url from :{lines[i]}')
                    continue
                urls.append(url[0])
        for url in urls:
            yield scrapy.Request('https://www.sec.gov/Archives/'+url, callback=self.parse_filing)

    def parse_filing(self, response):
        txt=response.body.decode()
        report_type=find_element(txt,'reportType')[0]
        assert '13F' in report_type
        filing=EdgarItem()
        filing['quarter_date']=find_element(txt,'reportCalendarOrQuarter')[0]
        try:
            dt=datetime.strptime(filing['quarter_date'],'%m-%d-%Y')
            filing['year']=dt.year
            filing['quarter']=quarters[dt.month]
        except:
            self.logger.error(f"Parsing date:{filing['quarter_date']}")
        filing['filer_cik']=find_element(txt,'cik')[0]
        filer_name=find_element(txt,'filingManager')[0]
        filing['filer_name']=find_element(filer_name,'name')[0]
        filing['docurl']=response.url
        res_positions=[]
        positions=find_element(txt,'ns1:infoTable')
        for p in positions:
            stock_name=find_element(p,'ns1:nameOfIssuer')[0]
            stock_cusip=find_element(p,'ns1:cusip')[0]
            shares=find_element(p,'ns1:shrsOrPrnAmt')[0]
            n_shares=find_element(shares,'ns1:sshPrnamt')[0]
            put_call=find_element(txt,'putCall')
            res_positions.append({'name':stock_name,'cusip':stock_cusip,'symbol':'',
                                  'quantity':n_shares,'callput':put_call})
        filing['positions']=res_positions
        if len(positions)>0:
            yield filing




