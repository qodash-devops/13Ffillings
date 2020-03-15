conn = new Mongo();
db = conn.getDB("edgar");

var updateStockPositions=function(){
    db.filings_13f.aggregate([
            {"$unwind": "$positions"},{"$project": {"filer_name":1,"quarter_date":1,"cusip":"$positions.cusip","quantity":"$positions.quantity"}},
            {"$lookup": {"from": "stock_info","localField": "cusip","foreignField": "cusip","as": "stock_info"}},
            {"$project": {"quarter_date":{"$dateFromString": {"dateString": "$quarter_date",onError:null,"format":"%m-%d-%Y"}},
                        "filer_name":1,"quantity":{"$toDouble": "$quantity"},
                        "dates":{"$map": {"input": {"$arrayElemAt": ["$stock_info.close.Date",0]},"as": "itemdate","in": {"$dateFromString": {"dateString": "$$itemdate",onError:null,"format":"%Y-%m-%d"}}}},
                        "prices":{"$arrayElemAt": ["$stock_info.close",0]},
                        "ticker":{"$arrayElemAt": ["$stock_info.ticker",0]},
                        "market_cap":{"$arrayElemAt": ["$stock_info.info.marketCap",0]},
                        "sector":{"$arrayElemAt": ["$stock_info.info.sector",0]},
                        "volume":{"$arrayElemAt": ["$stock_info.info.regularMarketVolume",0]}}},
            {$match: {"dates":{$gt:[]}}},
            {"$project": {"quarter_date":1,"quantity":1,"filer_name":1,"init_date":{"$min":{"$filter": {"input": "$dates","as": "item","cond": {"$gte": ["$$item","$quarter_date"]}}}},
                        "past_q_date":{"$add":["$quarter_date",- 1000 * 3600 * 24 * 30.5 * 3]},"next_q_date":{"$add":["$quarter_date", 1000 * 3600 * 24 * 30.5 * 3]},
                        "dates":1,"prices":1,"ticker":1,"market_cap":1,"sector":1,"volume":1}},
            {"$project": {"init_date":1,"quarter_date":1,"quantity":1,"filer_name":1,"prices":1,"ticker":1,"market_cap":1,"sector":1,"volume":1,
                        "past_q_date":{"$min":{"$filter": {"input": "$dates","as": "item","cond": {"$gte": ["$$item","$past_q_date"]}}}},
                        "next_q_date":{"$min":{"$filter": {"input": "$dates","as": "item","cond": {"$gte": ["$$item","$next_q_date"]}}}}
            }},
            {"$project": {"init_date":1,"quarter_date":1,"quantity":1,"filer_name":1,"prices":1,"ticker":1,"market_cap":1,"sector":1,"volume":1,"past_q_date":1,"next_q_date":1,
                       "init_spot":{"$arrayElemAt": [{"$filter": {"input": "$prices","as": "p","cond": { "$eq":[{"$dateFromString": {"dateString": "$$p.Date",onError:null,"format":"%Y-%m-%d"}},"$init_date"] }}},0]},
                       "past_q_spot":{"$arrayElemAt": [{"$filter": {"input": "$prices","as": "p","cond": { "$eq":[{"$dateFromString": {"dateString": "$$p.Date",onError:null,"format":"%Y-%m-%d"}},"$past_q_date"] }}},0]},
                       "next_q_spot":{"$arrayElemAt": [{"$filter": {"input": "$prices","as": "p","cond": { "$eq":[{"$dateFromString": {"dateString": "$$p.Date",onError:null,"format":"%Y-%m-%d"}},"$next_q_date"] }}},0]}
            }},
            {"$project": {"quarter_date":1,"quantity":1,"filer_name":1,"ticker":1,"market_cap":1,"sector":1,"volume":1,
                            "q_rel_cap":{$cond: { if: { $eq: ["$market_cap",0] }, then:0 , else: {$divide: ["$quantity","$market_cap"]} }},
                            "q_rel_vol":{$cond: { if: { $eq: ["$volume",0] }, then:0 , else: {$divide: ["$quantity","$volume"]} }} ,
                        "init_spot":1,"past_q_spot":1,"next_q_spot":1,
                        "return_before":{$cond: { if: { $eq:["$past_q_spot.Close",0] }, then: 0, else: {"$add":[{"$divide": ["$init_spot.Close","$past_q_spot.Close" ]},-1]}}},
                        "return_after":{$cond: { if: { $eq:["$init_spot.Close",0] }, then: 0, else: {"$add":[{"$divide": ["$next_q_spot.Close","$init_spot.Close" ]},-1]}}}}},
            { $merge: { into: "stocks_positions", whenMatched: "replace" } }

    ]);

};
db.system.js.save({_id:"updateStockPositions",value:updateStockPositions})
updateStockPositions();