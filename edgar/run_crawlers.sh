#!/usr/bin/env bash

log_file="/crawler/edgar/logs.txt"

print_title () {
	echo -e """
		\e[1m  \e[92m $1 \e[0m
	"""
}
run_crawler(){
    print_title "Running crawler : $1"
    python /crawler/edgar/crawler-go.py $1 > $log_file 2>&1 &
}

run_crawler "filings"
run_crawler "indexer"
run_crawler "stockinfo"
tail -f $log_file
