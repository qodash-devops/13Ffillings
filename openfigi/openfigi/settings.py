import os
import copy
from colorlog import ColoredFormatter
import scrapy.utils.log

BOT_NAME = 'openfigi'

SPIDER_MODULES = ['openfigi.spiders']
NEWSPIDER_MODULE = 'openfigi.spiders'

DOWNLOAD_DELAY = 20
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


color_formatter = ColoredFormatter(
    (
        BOT_NAME+'>>> %(log_color)s%(levelname)-5s%(reset)s '
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