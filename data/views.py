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

    "positions_view":[{"$unwind": "$positions"},{"$project": {"filer_name":1,"quarter_date":1,"cusip":"$positions.cusip","quantity":"$positions.quantity"}}],
    "positions_stock":[ {"$unwind": "$positions"},{"$project": {"filer_name":1,"quarter_date":1,"cusip":"$positions.cusip","quantity":"$positions.quantity"}},
                        {"$lookup": {"from": "stock_info","localField": "cusip","foreignField": "cusip","as": "stock_info"}},
                        {"$project": {"quarter_date":{"$dateFromString": {"dateString": "$quarter_date","format":"%m-%d-%Y"}},
                                    "filer_name":1,"quantity":{"$toDouble": "$quantity"},
                                    "dates":{"$map": {"input": {"$arrayElemAt": ["$stock_info.close.Date",0]},"as": "itemdate","in": {"$dateFromString": {"dateString": "$$itemdate","format":"%Y-%m-%d"}}}},
                                    "prices":{"$arrayElemAt": ["$stock_info.close",0]},
                                    "ticker":{"$arrayElemAt": ["$stock_info.ticker",0]},
                                    "market_cap":{"$arrayElemAt": ["$stock_info.info.marketCap",0]},
                                    "sector":{"$arrayElemAt": ["$stock_info.info.sector",0]},
                                    "volume":{"$arrayElemAt": ["$stock_info.info.regularMarketVolume",0]}}},
                        {"$project": {"quarter_date":1,"quantity":1,"filer_name":1,"init_date":{"$min":{"$filter": {"input": "$dates","as": "item","cond": {"$gte": ["$$item","$quarter_date"]}}}},
                                    "past_q_date":{"$add":["$quarter_date",- 1000 * 3600 * 24 * 30.5 * 3]},"next_q_date":{"$add":["$quarter_date", 1000 * 3600 * 24 * 30.5 * 3]},
                                    "dates":1,"prices":1,"ticker":1,"market_cap":1,"sector":1,"volume":1}},
                        {"$project": {"init_date":1,"quarter_date":1,"quantity":1,"filer_name":1,"prices":1,"ticker":1,"market_cap":1,"sector":1,"volume":1,
                                    "past_q_date":{"$min":{"$filter": {"input": "$dates","as": "item","cond": {"$gte": ["$$item","$past_q_date"]}}}},
                                    "next_q_date":{"$min":{"$filter": {"input": "$dates","as": "item","cond": {"$gte": ["$$item","$next_q_date"]}}}}
                        }},
                        {"$project": {"init_date":1,"quarter_date":1,"quantity":1,"filer_name":1,"prices":1,"ticker":1,"market_cap":1,"sector":1,"volume":1,"past_q_date":1,"next_q_date":1,
                                   "init_spot":{"$arrayElemAt": [{"$filter": {"input": "$prices","as": "p","cond": { "$eq":[{"$dateFromString": {"dateString": "$$p.Date","format":"%Y-%m-%d"}},"$init_date"] }}},0]},
                                   "past_q_spot":{"$arrayElemAt": [{"$filter": {"input": "$prices","as": "p","cond": { "$eq":[{"$dateFromString": {"dateString": "$$p.Date","format":"%Y-%m-%d"}},"$past_q_date"] }}},0]},
                                   "next_q_spot":{"$arrayElemAt": [{"$filter": {"input": "$prices","as": "p","cond": { "$eq":[{"$dateFromString": {"dateString": "$$p.Date","format":"%Y-%m-%d"}},"$next_q_date"] }}},0]}
                        }},
                        {"$project": {"quarter_date":1,"quantity":1,"filer_name":1,"ticker":1,"market_cap":1,"sector":1,"volume":1,
                                    "q_rel_cap":{"$divide": ["$quantity","$market_cap"]},
                                    "q_rel_vol":{"$divide": ["$quantity","$volume"]},
                                    "init_spot":1,"past_q_spot":1,"next_q_spot":1,
                                    "return_before":{"$add":[{"$divide": ["$init_spot.Close","$past_q_spot.Close" ]},-1]},
                                    "return_after":{"$add":[{"$divide": ["$next_q_spot.Close","$init_spot.Close" ]},-1]}
                        }}
                        ]
}

def create_indices():
    filings.create_index([("filer_name",pymongo.ASCENDING),("positions.cusip", pymongo.ASCENDING)])



if __name__ == '__main__':
    create_indices()
    for k, v in views.items():
        create_view(k, v)
