from fire import Fire
from edgar.edgar.settings import MONGO_DATABASE,MONGO_URI
import pymongo
client = pymongo.MongoClient(MONGO_URI)
db = client[MONGO_DATABASE]
stock_info=db['stock_info']
filings=db['13F']

class FillingsDB(object):
    stock_info = db['stock_info']
    filings = db['13F']

    def getFilers(self,filter_string=''):
        self.filings.find()
