# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class OpenfigiItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    cusip=scrapy.Field()
    info=scrapy.Field()
