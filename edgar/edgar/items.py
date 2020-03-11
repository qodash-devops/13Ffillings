# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class EdgarItem(scrapy.Item):
    # define the fields for your item here like:

    filing_type=scrapy.Field()
    filer_name=scrapy.Field()
    filer_cik = scrapy.Field()
    positions =scrapy.Field()
    quarter_date=scrapy.Field()
    quarter=scrapy.Field()
    year=scrapy.Field()
    symbol=scrapy.Field()
    docurl=scrapy.Field()


class StockInfoItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    cusip=scrapy.Field()
    ticker=scrapy.Field()
    exchange=scrapy.Field()