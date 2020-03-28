# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class F13FilingItem(scrapy.Item):
    # define the fields for your item here like:

    filing_type=scrapy.Field()
    filer_name=scrapy.Field()
    filer_cik = scrapy.Field()
    positions =scrapy.Field()
    quarter_date=scrapy.Field()
    quarter=scrapy.Field()
    filingyear=scrapy.Field()
    symbol=scrapy.Field()
    filingurl=scrapy.Field()

class PositionItem(scrapy.Item):
    filing_id=scrapy.Field()
    stockinfo_id =scrapy.Field()
    quarter_date=scrapy.Field()
    quarter=scrapy.Field()
    year=scrapy.Field()
    filer_name=scrapy.Field()
    filer_cik=scrapy.Field()
    quantity=scrapy.Field()
    ticker=scrapy.Field()
    cusip=scrapy.Field()
    info=scrapy.Field()
    instrumentclass=scrapy.Field()
    spot=scrapy.Field()
    spot_date=scrapy.Field()
    next_quarter_spot=scrapy.Field()
    next_quarter_spot_date=scrapy.Field()
    past_quarter_spot=scrapy.Field()
    past_quarter_spot_date=scrapy.Field()

class StockInfoItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    cusip=scrapy.Field()
    ticker=scrapy.Field()
    exchange=scrapy.Field()
    status=scrapy.Field()


class PageIndexItem(scrapy.Item):
    index=scrapy.Field()
    quarter=scrapy.Field()
    publishdate=scrapy.Field()
    year=scrapy.Field()
    url=scrapy.Field()
    doc_type=scrapy.Field()

