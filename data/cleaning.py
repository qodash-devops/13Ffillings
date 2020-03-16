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




if __name__ == '__main__':
    Fire({'index':remove_duplicate_index,
          'positions':clean_positions
          })
