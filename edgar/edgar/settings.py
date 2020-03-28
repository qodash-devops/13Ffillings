import os,copy
from colorlog import ColoredFormatter
import scrapy.utils.log

BOT_NAME = 'edgar'
SPIDER_MODULES = ['edgar.spiders']
# NEWSPIDER_MODULE = 'edgar.spiders'
ROBOTSTXT_OBEY = False
DOWNLOAD_DELAY = .10
CONCURRENT_REQUESTS_PER_IP=100


ELASTICSEARCH_SERVERS = [os.environ.get('ES_SERVER','http://localhost:9200')]
ITEM_PIPELINES = {
    'scrapyelasticsearch.scrapyelasticsearch.ElasticSearchPipeline': 200
}

EXTENSIONS = {
    'edgar.extensions.logstats.LogStats': 100,
    'scrapy.extensions.corestats.LogStats':None
}

LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(levelname)s: %(message)s'

YEARS=os.environ.get('CRAWL_YEARS','2018,2019,2020').split(',')









#########################################
# LOGS
color_formatter = ColoredFormatter(
    (
        '%(log_color)s%(levelname)-5s%(reset)s '
        '%(yellow)s[%(asctime)s]%(reset)s'
        '%(white)s %(name)s %(funcName)s %(bold_purple)s:%(lineno)d%(reset)s '
        '%(log_color)s%(message)s%(reset)s'
    ),
    datefmt='%y-%m-%d %H:%M:%S',
    log_colors={
        'DEBUG': 'blue',
        'INFO': 'bold_cyan',
        'WARNING': 'bold_yellow',
        'ERROR': 'bold_red',
        'CRITICAL': 'red,bg_white',
    }
)

_get_handler = copy.copy(scrapy.utils.log._get_handler)

def _get_handler_custom(*args, **kwargs):
    handler = _get_handler(*args, **kwargs)
    handler.setFormatter(color_formatter)
    return handler

scrapy.utils.log._get_handler = _get_handler_custom