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
    python /crawler/$1/crawler-go.py   >> $log_file 2>&1 &
    cd ..
    if [ $? -eq 0 ]; then
        echo OK
    else
        echo FAIL
    fi
}
run_crawler edgar
run_crawler openfigi
tail -f $log_file