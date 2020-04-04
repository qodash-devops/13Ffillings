import scrapy
from scrapy.downloadermiddlewares.httpproxy import HttpProxyMiddleware
from scrapy_proxies.randomproxy import RandomProxy
class MyipSpider(scrapy.Spider):
    name = 'myip'
    start_urls = ['http://www.mon-ip.com']

    def parse(self, response):
        for ip in response.xpath('//*[@id="PageG"]'):
            print(ip.xpath('p[3]/span[2]//text()').extract_first())