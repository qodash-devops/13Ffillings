from fire import Fire
from edgar.edgar.settings import MONGO_DATABASE, MONGO_URI
import logging
import pymongo
import colorlog
import sys
client = pymongo.MongoClient(MONGO_URI)
db = client[MONGO_DATABASE]
stock_info = db['stock_info']
filings = db['filings_13f']


logger = logging.getLogger('item_worker')
handler = colorlog.StreamHandler()
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def create_view(n, p, collection_on="filings_13f"):
    try:
        res=db.get_collection(n).drop()
        res=db.command({
            "create": n,
            "viewOn": collection_on,
            "pipeline": p
        })
    except:
        logging.error(f'View: {n} {sys.exc_info()}')


views = {
    "all_quarters_view": [{"$project": {"quarter": {"$concat":  [{"$toString": "$year"}," - ","$quarter"]}}},
                          {"$group": {"_id": "quarter", "quarters": {"$addToSet": "$quarter"}}}],
    "filers_quarter_view": [{"$project": {"filer_name": "$filer_name","quarter": {"$concat": [{"$toString": "$year"}," - ","$quarter" ]}}},
                            {"$group": {"_id": "$filer_name", "filed_quarters": {"$addToSet": "$quarter"}}},
                            {"$lookup":{"from":"all_quarters_view","foreignField":"all_quarters_view","localField":"all_quarters","as":"all_quarters"}},
                            {"$project":{"filer_name":"$filer_name","filed_quarters":"$filed_quarters","missing_quarters":{"$setDifference":[{"$arrayElemAt":["$all_quarters.quarters",0]},"$filed_quarters"]}}}],
}

materialized_views=[("positions",[{"$addFields": {"sector":{"$concat": [{"$ifNull": [ "$info.sector", "" ]} , {"$ifNull": [ "$info.category", "" ]}]}}},
                                {"$group": { "_id": {"sector":"$sector","quarter_date":"$quarter_date","filer_name":"$filer_name"},"size":{"$sum":{"$multiply": ["$quantity","$spot"]}},"ticker":{"$sum": 1}}},
                                {"$project": {"_id":0,"sector":"$_id.sector","quarter_date":"$_id.quarter_date","filer_name":"$_id.filer_name","size":1,"ticker":1}},
                                { "$out": "positions_by_sector" }])
                    ]

def refresh_materialized_views(collection,pipeline):
    output_col=pipeline[-1]["$out"]
    logger.info(f'dropping collection {output_col}')
    db[output_col].drop()
    logger.info(f'Updating materialized view on: {collection} => {output_col}')
    db[collection].aggregate(pipeline, allowDiskUse=True)
    return


def create_indices():
    logger.info('creating indices...')
    filings.create_index([("filer_name",pymongo.ASCENDING),("positions.cusip", pymongo.ASCENDING)])
    db["positions"].create_index([("filer_name",pymongo.ASCENDING),("info.sector",pymongo.ASCENDING),
                                  ("info.category",pymongo.ASCENDING),("quarter_date",pymongo.ASCENDING)])



if __name__ == '__main__':
    create_indices()
    for c,p in materialized_views:
        refresh_materialized_views(c,p)
    for k, v in views.items():
        create_view(k, v)

