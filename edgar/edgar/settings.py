import os,copy
from colorlog import ColoredFormatter
import scrapy.utils.log

BOT_NAME = 'edgar'
SPIDER_MODULES = ['edgar.spiders']
# NEWSPIDER_MODULE = 'edgar.spiders'
ROBOTSTXT_OBEY = False

DOWNLOAD_DELAY = .10
RANDOMIZE_DOWNLOAD_DELAY = True
CONCURRENT_REQUESTS_PER_IP=50
MEMUSAGE_LIMIT_MB=4000
# ...
# Configure item pipelines
# See http://scrapy.readthedocs.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'edgar.pipelines.EdgarPipeline': 300
}

MONGO_URI=os.environ.get('MONGO_URI','mongodb://localhost:27020')
MONGO_DATABASE = 'edgar'


LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(levelname)s: %(message)s'

YEARS=os.environ.get('CRAWL_YEARS','2019').split(',')

SPIDER_MIDDLEWARES = {
    'scrapy_deltafetch.DeltaFetch': 100,
}
DELTAFETCH_ENABLED = True


color_formatter = ColoredFormatter(
    (
        '%(name)s>>> %(log_color)s%(levelname)-5s%(reset)s '
        '%(yellow)s[%(asctime)s]%(reset)s'
        '%(white)s %(name)s %(funcName)s %(bold_purple)s:%(lineno)d%(reset)s '
        '%(log_color)s%(message)s%(reset)s'
    ),
    datefmt='%y-%m-%d %H:%M:%S',
    log_colors={
        'DEBUG': 'blue',
        'INFO': 'bold_cyan',
        'WARNING': 'red',
        'ERROR': 'bg_bold_red',
        'CRITICAL': 'red,bg_white',
    }
)

_get_handler = copy.copy(scrapy.utils.log._get_handler)

def _get_handler_custom(*args, **kwargs):
    handler = _get_handler(*args, **kwargs)
    handler.setFormatter(color_formatter)
    return handler

scrapy.utils.log._get_handler = _get_handler_custom