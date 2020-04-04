# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class ItemList(scrapy.Item):
    list=scrapy.Field()

class PositionItem(scrapy.Item):
    filingurl = scrapy.Field()
    filer_name=scrapy.Field()
    filer_cik = scrapy.Field()
    quarter_date=scrapy.Field()
    publishdate=scrapy.Field()
    quantity=scrapy.Field()
    cusip=scrapy.Field()
    stockname=scrapy.Field()
    instrumentclass=scrapy.Field()
    put_call=scrapy.Field()
    info=scrapy.Field()
    spot=scrapy.Field()
    spot_date=scrapy.Field()
    next_quarter_spot=scrapy.Field()
    next_quarter_spot_date=scrapy.Field()
    past_quarter_spot=scrapy.Field()
    past_quarter_spot_date=scrapy.Field()

    ticker=scrapy.Field()
    close=scrapy.Field()
    info=scrapy.Field()

    status=scrapy.Field()

class StockInfoItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    cusip=scrapy.Field()
    ticker=scrapy.Field()
    status=scrapy.Field()
    close=scrapy.Field()
    info=scrapy.Field()


class PageIndexItem(scrapy.Item):
    index=scrapy.Field()
    quarter=scrapy.Field()
    publishdate=scrapy.Field()
    year=scrapy.Field()
    filingurl=scrapy.Field()
    doc_type=scrapy.Field()


