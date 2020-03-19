import yfinance as yf
import pymongo
import sys

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


class EdgarPipeline(object):
    collection_name = 'filings_13f'
    collection_stock_info='stock_info'
    collection_empty = 'empty_filings'
    index_collection = 'page_index'
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
        i = dict(item)
        if spider.name=='stockinfo':
            key = {'cusip': i['cusip']}
            try:
                i['close'],i['info']=self.get_spots(i['ticker'])
            except:
                i['close']=[]
            self.db[self.collection_stock_info].update(key, i, upsert=True)
        elif spider.name=='edgarindex':
            index_def = dict(item)
            self.db[self.index_collection].update({'index': index_def['index']},
                                                  index_def, upsert=True)
        else:
            try:
                key = {'docurl': i['docurl']}
                if len(item['positions'])==0:
                    self.db[self.collection_empty].update(key, i, upsert=True)
                elif len(item['positions'])<1000:
                    self.db[self.collection_name].update(key, i, upsert=True)
                else:
                    positions_split=chunks(item['positions'],1000)
                    url=item['docurl']
                    idx=1
                    del i['positions']
                    self.db[self.collection_name].update({'docurl': i['docurl']}, i, upsert=True)
                    for p in positions_split:
                        i['positions']=p
                        i['docurl']=url+f'_part_{idx}'
                        self.db[self.collection_name].update({'docurl': i['docurl']}, i, upsert=True)
                        idx+=1


            except:
                print('Error inserting item==>'+str(sys.exc_info()))
        i={}
        # return item


    def get_spots(self,ticker):
        t = yf.Ticker(ticker)
        info=t.info
        res = t.history(period="3y")
        res = res["Close"]
        res.index = res.index.astype(str)
        close = res.dropna().to_frame().reset_index().to_dict(orient='records')
        return close,info


