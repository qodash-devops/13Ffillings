from beaker.cache import CacheManager
from beaker.util import parse_cache_config_options
import logging,os
cache_opts = {
    'cache.data_dir': '/tmp/cache/data',
    'cache.lock_dir': '/tmp/cache/lock',
    'cache.regions': "short_term,long_term",
    'cache.short_term.type': 'file',
    'cache.short_term.expire': '3600',
    'cache.long_term.type': 'file',
    'cache.long_term.expire': '86400'
}
cache = CacheManager(**parse_cache_config_options(cache_opts))

curdir=os.path.dirname(os.path.realpath(__file__))


def clear_cache():
    from beaker.cache import cache_managers
    for _cache in cache_managers.values():
        _cache.clear()