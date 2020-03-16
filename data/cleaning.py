import os,pymongo
import logging
from tqdm import tqdm
from datetime import timedelta,datetime
import warnings
import colorlog
warnings.simplefilter("ignore")
from fire import Fire
from bson.objectid import ObjectId

handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s[%(asctime)s - %(levelname)s]:%(message)s'))
logger = colorlog.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.INFO)



mongo_uri=os.environ.get('MONGO_URI','mongodb://localhost:27020')
client = pymongo.MongoClient(mongo_uri)
db = client['edgar']
index=db['page_index']


def remove_duplicate_index():
    res=index.find({})
    res=list(res)
    all_urls=[]
    for r in tqdm(res):
        all_urls+=r['filings']
        if len(r['filings'])!=len(list(set(r['filings']))):
            #duplicate urls
            r['filings']=list(set(r['filings']))
            index.update({'_id':r['_id']},r)
    print(f'n_urls={len(all_urls)} , n unique={len(set(all_urls))}')

def clean_positions():
    filings=db['filings_13f']
    res=filings.find({},batch_size=1000)
    bar=tqdm(res,desc='filings',total=res.count())
    for f in bar:
        if type(f['quarter_date'])==str:
            f['quarter_date']=datetime.strptime(f['quarter_date'],'%m-%d-%Y')
        elif not isinstance(f['quarter_date'],datetime):
            logger.error(f'filing {f["_id"]} quarter_date error!')
        for i in range(len(f['positions'])):
            if type(f['positions'][i]['quantity'])==str:
                f['positions'][i]['quantity']=float(f['positions'][i]['quantity'].strip())
            elif type(f['positions'][i]['quantity'])!=float:
                logger.error(f'Position {i} for filing {f["_id"]}:type of quantity!')
            f['positions'][i]['_id']=ObjectId()
        filings.update({"_id":f['_id']},f)


def clean_stock_info():
    stock_info=db['stock_info']
    res=stock_info.find({},batch_size=1000)
    bar=tqdm(res,desc='stock_info',total=res.count())
    for info in bar:
        if "info" in info.keys():
            info['market_cap']=info['info']['marketCap']
            info['sector']=info['info']['sector']
            info['volume']=info['info']['averageDailyVolume10Day']
            for i in range(len(info['close'])):
                if type(info['close'][i]['Date'])==str:
                    info['close'][i]['Date']=datetime.strptime(info['close'][i]['Date'],'%Y-%m-%d')
            stock_info.update({"_id":info["_id"]},info)
        else:
            logger.warning(f'No info for id_{input(["_id"])}: {info}')

if __name__ == '__main__':
    Fire({'index':remove_duplicate_index,
          'positions':clean_positions,
          'info':clean_stock_info
          })
