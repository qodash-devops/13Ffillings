import os
BOT_NAME = 'edgar'
SPIDER_MODULES = ['edgar.spiders']
# NEWSPIDER_MODULE = 'edgar.spiders'
ROBOTSTXT_OBEY = True

DOWNLOAD_DELAY = .25
RANDOMIZE_DOWNLOAD_DELAY = True
CONCURRENT_REQUESTS_PER_IP=15
# ...
# Configure item pipelines
# See http://scrapy.readthedocs.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'edgar.pipelines.EdgarPipeline': 300,
}

MONGO_URI=os.environ.get('MONGO_HOST','mongodb://localhost:27020')
MONGO_DATABASE = 'edgar'

YEARS=os.environ.get('CRAWL_YEARS','2019').split(',')

SPIDER_MIDDLEWARES = {
    'scrapy_deltafetch.DeltaFetch': 100,
}
DELTAFETCH_ENABLED = True