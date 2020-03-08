from beaker.cache import CacheManager
from beaker.util import parse_cache_config_options
import logging,os
cache_opts = {
    'cache.data_dir': '/tmp/cache/edgar/data',
    'cache.lock_dir': '/tmp/cache/edgar/lock',
    'cache.regions': "short_term,long_term",
    'cache.short_term.type': 'file',
    'cache.short_term.expire': '3600',
    'cache.long_term.type': 'file',
    'cache.long_term.expire': '86400'
}
cache = CacheManager(**parse_cache_config_options(cache_opts))
curdir=os.path.dirname(os.path.realpath(__file__))
