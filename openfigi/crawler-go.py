from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.settings import Settings
from datetime import datetime
from fire import Fire
import os

default_years=f'{datetime.today().year},{datetime.today().year-1}'
default_filers='*'

def run_crawler():
    settings = Settings()
    os.environ['SCRAPY_SETTINGS_MODULE'] = 'openfigi.settings'
    settings_module_path = os.environ['SCRAPY_SETTINGS_MODULE']
    settings.setmodule(settings_module_path, priority='project')
    settings=get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl('openfigi')
    process.start()

if __name__ == '__main__':
    Fire(run_crawler)