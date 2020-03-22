import data.yfinance as yf
import pymongo
import sys
from bson.objectid import ObjectId
from .items import PositionItem
import calendar
import os
import numpy as np

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
        res.index = res.index.to_pydatetime()
        close = res.dropna().to_frame().reset_index().to_dict(orient='records')
        return close,info


class PositionsPipeline(EdgarPipeline):
    # @profile
    def process_item(self, item, spider):
        self.spider=spider
        # ## how to handle each filing
        # item_type=item._class.__name__
        # if item_type=='x_F13FilingItem':
        #     for i in range(len(item['positions'])):
        #         item['positions'][i]['_id']=ObjectId(('CUS'+item['positions'][i]['cusip']).encode())
        #     i=dict(item)
        #     key = {'docurl': i['docurl']}
        #
        #     if len(item['positions']) == 0:
        #         update=self.db[self.collection_empty].update(key, i, upsert=True)
        #     else:
        #         update=self.db[self.collection_name].update(key, i, upsert=True)
        #         try:
        #             i['_id']=update['upserted']
        #         except:
        #             pass
        #         spider.crawler.stats.inc_value('filings')
        #
        #     #yielding the positions items
        #     for p in item['positions']:
        #         info=self.db[self.collection_stock_info].find_one({'_id':p['_id']})
        #         if not info is None:
        #             self.updatePosition(p,i, info)
        #
        #
        # elif item_type=='x_StockInfoItem':
        #     i=dict(item)
        #     key = {'cusip': i['cusip']}
        #     try:
        #         i['close'], i['info'] = self.get_spots(i['ticker'])
        #     except:
        #         i['close'] = []
        #     i['_id']=ObjectId(('CUS' + i['cusip']).encode())
        #     self.db[self.collection_stock_info].update(key, i, upsert=True)
        #     filings=self.db[self.collection_name].find({"positions.cusip":i['cusip']})
        #     spider.crawler.stats.inc_value('stock_info')
        #     for f in filings:
        #         for p in f['positions']:
        #             if p['cusip']==i['cusip']:
        #                 self.updatePosition(p,f, i)

    def updatePosition(self, position,filing, stockinfo):
        assert position['cusip']==stockinfo['cusip']
        if len(stockinfo['close'])==0:
            try:
                ticker=stockinfo['ticker']
            except:
                ticker='Notfound'
            # self.spider.logger.warning(f"No spots for cusip={position['cusip']} ticker={ticker} name=\"{position['name']}\"")
            return
        pos = PositionItem()
        try:
            pos['filing_id'] = filing['_id']
            pos['stockinfo_id']=stockinfo['_id']
            pos['info']=stockinfo['info']
            pos['quarter_date']=filing['quarter_date']
            pos['quarter']=filing['quarter']
            pos['year']=filing['year']
            pos['filer_name']=filing['filer_name']
            pos['filer_cik']=filing['filer_cik']

            pos['quantity']=position['quantity']
            pos['cusip']=position['cusip']
            pos['ticker']=stockinfo['ticker']
            quarter_idx=np.argmin(abs(np.array([pp['index'] for pp in stockinfo['close']])-filing['quarter_date']))
            pos['spot_date']=stockinfo['close'][quarter_idx]['index']
            pos['spot']=stockinfo['close'][quarter_idx]['Close']

            next_quarter_idx=min(quarter_idx + 64, len(stockinfo['close']) - 1)
            prev_quarter_idx=max(quarter_idx-64,0)
            pos['next_quarter_spot_date'] = stockinfo['close'][next_quarter_idx]['index']
            pos['next_quarter_spot'] = stockinfo['close'][next_quarter_idx]['Close']
            pos['past_quarter_spot_date'] = stockinfo['close'][prev_quarter_idx]['index']
            pos['past_quarter_spot'] = stockinfo['close'][prev_quarter_idx]['Close']

            keys={'filing_id':pos['filing_id'],'cusip':pos['cusip']}
            self.db['positions'].update(keys,pos,upsert=True)
            self.spider.crawler.stats.inc_value('positions')

        except:
            self.spider.logger.error(f"getting position reason={sys.exc_info()}")


