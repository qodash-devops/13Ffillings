import logging
import pymongo

class EdgarPipeline(object):
    collection_name = 'filings_13f'
    collection_stock_info='stock_info'
    collection_empty = 'empty_filings'
    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
    @classmethod
    def from_crawler(cls, crawler):
        ## pull in information from settings.py
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE')
        )
    def open_spider(self, spider):
        ## initializing spider
        ## opening db connection
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        ## clean up when spider is closed
        self.client.close()

    def process_item(self, item, spider):
        ## how to handle each filing
        i=dict(item)
        key = {'docurl': i['docurl']}
        if len(item['positions'])==0:
            self.db[self.collection_empty].update(key, i, upsert=True)
        else:
            self.db[self.collection_name].update(key, i, upsert=True)
        return item


