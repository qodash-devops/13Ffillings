import yfinance as yf
import os,pymongo
import logging
from tqdm import tqdm
from datetime import timedelta
import warnings
warnings.simplefilter("ignore")
from fire import Fire
from concurrent.futures import ProcessPoolExecutor
import pandas as pd


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


if __name__ == '__main__':
    remove_duplicate_index()