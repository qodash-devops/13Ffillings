import scrapy

class MyipSpider(scrapy.Spider):
    name = 'myip'
    start_urls = ['http://www.mon-ip.com']

    def parse(self, response):
        for ip in response.xpath('//*[@id="PageG"]'):
            print(ip.xpath('p[3]/span[2]//text()').extract_first())