from fire import Fire
from edgar.edgar.settings import MONGO_DATABASE, MONGO_URI
import logging
import pymongo
import sys
client = pymongo.MongoClient(MONGO_URI)
db = client[MONGO_DATABASE]
stock_info = db['stock_info']
filings = db['filings_13f']


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

    "positions_view":[{"$unwind": "$positions"},{"$project": {"filer_name":1,"quarter_date":1,"cusip":"$positions.cusip","quantity":"$positions.quantity"}}]
}






if __name__ == '__main__':
    for k, v in views.items():
        create_view(k, v)
