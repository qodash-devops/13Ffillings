var positionMap=function(){
    for (let p in this.positions) {
        var value={
            quarter_date:this.quarter_date,
            filer_name:this.filer_name,
            quantity:this.quantity,
        };
        emit(p.cusip,value);
    }
}
var positionReduce=function(key,values){
    var res={positions: values};
    return res;
}
db.filings_13f.mapReduce(positionMap,positionReduce,{out:"test_map_reduce"})