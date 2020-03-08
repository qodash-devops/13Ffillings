import os

BOT_NAME = 'openfigi'

SPIDER_MODULES = ['openfigi.spiders']
NEWSPIDER_MODULE = 'openfigi.spiders'

DOWNLOAD_DELAY = 13
RANDOMIZE_DOWNLOAD_DELAY = True
CONCURRENT_REQUESTS_PER_IP=1
MONGO_URI=os.environ.get('MONGO_URI','mongodb://localhost:27020')
MONGO_DATABASE = 'edgar'
ITEM_PIPELINES = {
    'openfigi.pipelines.OpenfigiPipeline': 300,
}

LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(levelname)s: %(message)s'


ROBOTSTXT_OBEY = True

