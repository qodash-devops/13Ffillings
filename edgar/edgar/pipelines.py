"""Elastic Search Pipeline for scrappy expanded with support for multiple items"""
from .es import ESDB
from datetime import datetime
from elasticsearch import Elasticsearch, helpers
from six import string_types
import hashlib
import types
import data.yfinance as yf
from .items import ItemList

es=ESDB()

class InvalidSettingsException(Exception):
    pass


class ElasticSearchPipeline(object):
    settings = None
    es = None
    items_buffer = []
    logger=None
    @classmethod
    def validate_settings(cls, settings):
        def validate_setting(setting_key):
            if settings[setting_key] is None:
                raise InvalidSettingsException('%s is not defined in settings.py' % setting_key)

        required_settings = {'ELASTICSEARCH_INDEX', 'ELASTICSEARCH_TYPE'}

        for required_setting in required_settings:
            validate_setting(required_setting)

    @classmethod
    def init_es_client(cls, crawler_settings):
        auth_type = crawler_settings.get('ELASTICSEARCH_AUTH')
        es_timeout = crawler_settings.get('ELASTICSEARCH_TIMEOUT',60)

        es_servers = crawler_settings.get('ELASTICSEARCH_SERVERS', 'localhost:9200')
        es_servers = es_servers if isinstance(es_servers, list) else [es_servers]

        if auth_type == 'NTLM':
            from .transportNTLM import TransportNTLM
            es = Elasticsearch(hosts=es_servers,
                               transport_class=TransportNTLM,
                               ntlm_user= crawler_settings['ELASTICSEARCH_USERNAME'],
                               ntlm_pass= crawler_settings['ELASTICSEARCH_PASSWORD'],
                               timeout=es_timeout)

            return es

        es_settings = dict()
        es_settings['hosts'] = es_servers
        es_settings['timeout'] = es_timeout

        if 'ELASTICSEARCH_USERNAME' in crawler_settings and 'ELASTICSEARCH_PASSWORD' in crawler_settings:
            es_settings['http_auth'] = (crawler_settings['ELASTICSEARCH_USERNAME'], crawler_settings['ELASTICSEARCH_PASSWORD'])

        if 'ELASTICSEARCH_CA' in crawler_settings:
            import certifi
            es_settings['port'] = 443
            es_settings['use_ssl'] = True
            es_settings['ca_certs'] = crawler_settings['ELASTICSEARCH_CA']['CA_CERT'] or certifi.where()
            es_settings['client_key'] = crawler_settings['ELASTICSEARCH_CA']['CLIENT_KEY']
            es_settings['client_cert'] = crawler_settings['ELASTICSEARCH_CA']['CLIENT_CERT']

        es = Elasticsearch(**es_settings)
        return es

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls()
        ext.settings = crawler.settings
        ext.logger=crawler.spider.logger
        cls.validate_settings(ext.settings)
        ext.es = cls.init_es_client(crawler.settings)
        return ext

    def process_unique_key(self, unique_key):
        if isinstance(unique_key, (list, tuple)):
            unique_key = unique_key[0].encode('utf-8')
        elif isinstance(unique_key, string_types):
            unique_key = unique_key.encode('utf-8')
        else:
            raise Exception('unique key must be str or unicode')

        return unique_key

    def get_id(self, item):
        key_def=self.settings['ELASTICSEARCH_UNIQ_KEY']

        if isinstance(key_def, list):
            item_unique_key = '-'.join([item[k] for k in key_def])
        else:
            item_unique_key= item[key_def]
        unique_key = self.process_unique_key(item_unique_key)
        item_id = hashlib.sha1(unique_key).hexdigest()
        return item_id

    def index_item(self, item):

        index_name = self.settings['ELASTICSEARCH_INDEX']
        index_suffix_format = self.settings.get('ELASTICSEARCH_INDEX_DATE_FORMAT', None)
        index_suffix_key = self.settings.get('ELASTICSEARCH_INDEX_DATE_KEY', None)
        index_suffix_key_format = self.settings.get('ELASTICSEARCH_INDEX_DATE_KEY_FORMAT', None)

        if index_suffix_format:
            if index_suffix_key and index_suffix_key_format:
                dt = datetime.strptime(item[index_suffix_key], index_suffix_key_format)
            else:
                dt = datetime.now()
            index_name += "-" + datetime.strftime(dt,index_suffix_format)
        elif index_suffix_key:
            index_name += "-" + index_suffix_key

        index_action = {
            '_index': index_name,
            '_type': self.settings['ELASTICSEARCH_TYPE'],
            '_source': dict(item)
        }

        if self.settings['ELASTICSEARCH_UNIQ_KEY'] is not None:
            item_id = self.get_id(item)
            index_action['_id'] = item_id
            self.logger.debug('Generated unique key %s' % item_id)

        self.items_buffer.append(index_action)

        if len(self.items_buffer) >= self.settings.get('ELASTICSEARCH_BUFFER_LENGTH', 500):
            self.send_items()
            self.items_buffer = []


    def send_items(self):
        helpers.bulk(self.es, self.items_buffer)

    def process_item(self, item, spider):
        if isinstance(item, types.GeneratorType) or isinstance(item, list):
            for each in item:
                self.process_item(each, spider)
        elif isinstance(item, ItemList):
            for each in item['list']:
                self.process_item(each, spider)

        else:
            self.index_item(item)
            self.logger.debug('Item sent to Elastic Search %s' % self.settings['ELASTICSEARCH_INDEX'])
            return item

    def close_spider(self, spider):
        if len(self.items_buffer):
            self.send_items()


class InfoPipeline(object):
    logger=None
    @classmethod
    def from_crawler(cls, crawler):
        ext = cls()
        ext.settings = crawler.settings
        ext.logger = crawler.spider.logger
        return ext
    def process_item(self, item, spider):
        assert spider.name=='stockinfo'
        if item['ticker']!='':
            close,info=self.get_spots(item['ticker'])
            item['close']=close
            item['info']=info
            spider.crawler.stats.inc_value('info_ticker_found')
        spider.crawler.stats.inc_value('ninfo')
        return item

    def get_spots(self,ticker):
        try:
            t = yf.Ticker(ticker)
            info=self.clean_info(t.info)
            res = t.history(period="3y")
            res = res["Close"]
            res.index = res.index.to_pydatetime()
            close = res.dropna().to_frame().reset_index().to_dict(orient='records')
            return close,info
        except:
            self.logger.warning('getting spots for ticker:'+ticker)
            return {},{}
    def clean_info(self,info):
        number_fields=["52WeekChange","SandP52WeekChange","ask","askSize",
                       "averageDailyVolume10Day","averageVolume","averageVolume10days",
                        "beta","beta3Year","bid","bidSize","bookValue","dateShortInterest",
                       "dividendRate","dividendYield","earningsQuarterlyGrowth","enterpriseToEbitda",
                       "enterpriseToRevenue","enterpriseValue","fiftyDayAverage","fiftyTwoWeekHigh",
                       "fiftyTwoWeekLow","forwardPE","marketCap","trailingEps","trailingPE"]
        for k in number_fields:
            try:
                info[k]=float(info[k])
            except:
                del info[k]

class PositionsInfoPipeline(InfoPipeline):
    def process_item(self, item, spider):
        if spider.name=='stockinfo':
            try:
                if item['status']!='NOTFOUND':
                    positions=es.get_positions(item['cusip'])
                    for pos in positions:
                        id=pos["_id"]
                        params={'ticker':item['ticker'],'close':item['close'],'info':item['info']}
                        params["status"]="identified"
                        # script="ctx._source.ticker=params.ticker;ctx._source.close=params.close;ctx._source.info=params.info;"
                        script="""
                            ctx._source.ticker=params.ticker;
                            ctx._source.close=params.close;
                            ctx._source.info=params.info;
                            ctx._source.status=params.status;
                        """
                        body={
                                "script" : {
                                    "source": script,
                                    "lang": "painless",
                                    "params" : params
                                }
                            }
                        es.es.update('13f_positions',id,body=body)
                        spider.crawler.stats.inc_value('identified_positions')
            except:
                self.logger.error("processing stock info item cusip:"+item['cusip'])

        elif spider.name=='positions':
            for i in range(len(item['list'])):
                cusip=item['list'][i]['cusip']
                info=es.get_info(cusip)
                if not info is None:
                    if info['status']=='NOTFOUND':
                        continue
                    try:
                        item['list'][i]['ticker']=info['ticker']
                        item['list'][i]['close']=info['close']
                        item['list'][i]['info']=info['info']
                        if info['info']!={}:
                            item['list'][i]['status']="identified"
                            spider.crawler.stats.inc_value('identified_positions')
                    except:
                        self.logger.error(f'geeting info for cusip:{cusip}')



        return item
