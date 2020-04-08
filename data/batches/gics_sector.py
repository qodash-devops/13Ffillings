from edgar.edgar.es import ESDB
import pandas as pd
from datetime import datetime
import time
from data.batches.batch import BatchProcess,logger
from threading import Thread
from concurrent.futures import ProcessPoolExecutor,as_completed
from tqdm import tqdm
import logging
import sys
es=ESDB()

import collections

MAX_ES_TASKS_MIN=2000
TARGET_INDEX='13f_positions'

def update_asset_type():
    script="""
        String res="";
        if (ctx._source.info_price_quoteType!=null){
            res=ctx._source.info_price_quoteType;
        }
        if (ctx._source.info_fundProfile_categoryName !=null){
            res=res+" - "+ctx._source.info_fundProfile_categoryName;
        }
        if (ctx._source.info_summaryProfile_sector!=null){
            res=res+" - "+ctx._source.info_summaryProfile_sector;
        }
        ctx._source.instrument_category=res;
    """
    q={
        "script":{
            "source":script,
            "lang":"painless"
        },
        "query":{
            "match_all":{}
        }
    }
    res=es.es.update_by_query(index='13f_info_positions',body=q,wait_for_completion=False)
    print(res)



if __name__ == '__main__':
    update_asset_type()