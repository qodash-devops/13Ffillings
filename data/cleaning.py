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
        r['n_filings']=len(r['filings'])
        index.update({'_id':r['_id']},r)
    print(f'n_urls={len(all_urls)} , n unique={len(set(all_urls))}')

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
def clean_positions(max_positions_split=10000000):
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
        if len(f['positions'])>max_positions_split:
            filings.delete_one({'_id':f['_id']})
            for positions in chunks(f['positions'],max_positions_split):
                nf={k:v for k,v in f.items() if k!='positions'}
                nf['positions']=positions
                nf['_id']=ObjectId()
                filings.insert(nf)
        else:
            filings.update({"_id":f['_id']},f)


def clean_stock_info():
    stock_info=db['stock_info']
    res=stock_info.find({},batch_size=100)
    bar=tqdm(res,desc='stock_info',total=res.count())
    for info in bar:
        if "info" in info.keys():
            info['market_cap']=info['info']['marketCap']
            info['sector']=info['info'].get('sector','')
            info['volume']=info['info'].get('averageDailyVolume10Day',0)
            for i in range(len(info['close'])):
                if type(info['close'][i]['Date'])==str:
                    info['close'][i]['Date']=datetime.strptime(info['close'][i]['Date'],'%Y-%m-%d')
            stock_info.update({"_id":info["_id"]},info)
        else:
            pass




if __name__ == '__main__':
    Fire({'index':remove_duplicate_index,
          'positions':clean_positions,
          'info':clean_stock_info
          })
