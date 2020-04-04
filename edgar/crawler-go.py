from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.settings import Settings
import requests,time
from fire import Fire
import os
import colorlog
import logging

handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s[%(asctime)s - %(levelname)s]:%(message)s'))
logger = colorlog.getLogger('Crawler-Startup')
logger.addHandler(handler)
logger.setLevel(logging.INFO)



def wait_for_db_connection(urls,timeout=60):
    t=time.time()
    logger.info(f'Waiting for elasticsearch connection [{urls}]...')
    while True:
        if time.time()-t>timeout:
            raise ConnectionError("Unable to connect to elastic search url:"+url)
        for url in urls:
            try:
                r=requests.get(url)
                return
            except:
                time.sleep(0.2)


def run_crawler(crawler="positions", loglevel="INFO"):
    settings = Settings()
    os.environ['SCRAPY_SETTINGS_MODULE'] = 'edgar.settings'
    settings_module_path = os.environ['SCRAPY_SETTINGS_MODULE']
    settings.setmodule(settings_module_path, priority='project')
    settings = get_project_settings()
    wait_for_db_connection(settings.get('ELASTICSEARCH_SERVERS'))

    settings.set("LOG_LEVEL", loglevel)
    process = CrawlerProcess(settings)
    process.crawl(crawler)
    process.start()


if __name__ == '__main__':
    Fire(run_crawler)
