#!/usr/bin/env bash

print_title () {
	echo -e """\e[1m  \e[92m $1 \e[0m"""
}
print_title "Stopping existing deployement..."
docker-compose down

print_title "Deploying elasticstack..."
docker-compose up -d elasticsearch kibana

print_title "Running page indexer ..."
docker-compose build
docker-compose run -d --name edgar_page_indexer crawler indexer

print_title "Starting in crawlers background  ..."
docker-compose up -d  stockinfo crawler

docker-compose --compatibility scale stockinfo=4 crawler=4y