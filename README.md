# 13Ffillings
Crawl 13F filings from edgar database and stores them to mongo db.
<br>
Based on scrapy (https://scrapy.org/). 

### quick start:
#### run on docker:
###### requirements: Docker, docker-compose.
       $ docker-compose up -d mongo
       $ docker-compose up -d crawler
#### run on host:
make sure to have libdb-dev installed.<br>

       $ git clone https://github.com/qodash-devops/13Ffillings.git
       $ cd cd 13Ffillings
       $ pip install -r requirements
#### custom database:        