#!/usr/bin/env bash
log_file=/crawler/logs.log
touch $log_file
print_title () {
	echo -e """
		\e[1m  \e[92m $1 \e[0m
	"""
}
run_crawler(){
    print_title "Running crawler $1..."
    cd $1
    export PYTHONPATH=$PWD
    python /crawler/edgar/crawler-go.py $1   >> $log_file 2>&1 &
    cd ..
    if [ $? -eq 0 ]; then
        echo OK
    else
        echo FAIL
    fi
}



#echo "MONGO_URI=$MONGO_URI"
#run_crawler edgar
#run_crawler quantumonline
tail -f $log_file