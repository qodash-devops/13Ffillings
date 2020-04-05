import os,copy
from colorlog import ColoredFormatter
import scrapy.utils.log

settings_dir=os.path.dirname(os.path.realpath(__file__))

BOT_NAME = 'edgar'
SPIDER_MODULES = ['edgar.spiders']
# NEWSPIDER_MODULE = 'edgar.spiders'
ROBOTSTXT_OBEY = False
DOWNLOAD_DELAY = .30
CONCURRENT_REQUESTS_PER_IP=100


ELASTICSEARCH_SERVERS = [os.environ.get('ES_SERVER','http://localhost:9201')]
laz = {

}

LOGSTATS_INTERVAL=60
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(levelname)s: %(message)s'

YEARS=os.environ.get('CRAWL_YEARS','2018,2019,2020').split(',')

EXTENSIONS = {
    'edgar.extensions.logstats.ESLogStats': 200,
     'scrapy.extensions.telnet.TelnetConsole': None,
}

# ######################### PROXY
if os.path.isfile(settings_dir+'/proxy_list.txt'):
    PROXY_LIST=settings_dir+'/proxy_list.txt'
    # Retry many times since proxies often fail
    RETRY_TIMES = 10
    # Retry on most error codes since proxies fail for different reasons
    RETRY_HTTP_CODES = [500, 503, 504, 400, 403, 404, 408]
    PROXY_MODE = 0
    DOWNLOADER_MIDDLEWARES = {
        'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
        'edgar.middlewares.randomproxy.RandomProxy': 100,
        'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
    }


#########################################
# LOGS
color_formatter = ColoredFormatter(
    (
        '%(bold_blue)s%(name)s | %(reset)s %(log_color)s%(levelname)-5s | %(reset)s '
        '%(yellow)s[%(asctime)s]%(reset)s'
        '%(white)s %(funcName)s %(bold_purple)s:%(lineno)d >>> %(reset)s '
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