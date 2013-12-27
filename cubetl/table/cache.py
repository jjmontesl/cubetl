import logging
from os import listdir
from os.path import isfile, join
import itertools
import re
from cubetl.core import Node, Component
import chardet    
from BeautifulSoup import UnicodeDammit
from cubetl.fs import FileReader
import csv
from cubetl.table import TableLookup
from cubetl.util.cache import Cache
from cubetl.script import Eval


# Get an instance of a logger
logger = logging.getLogger(__name__)

class CachedTableLookup(TableLookup):
    
    NOT_CACHED = "NOT_CACHED"
    
    def __init__(self):

        super(CachedTableLookup, self).__init__()

        self._cache = None
        self.cache_hits = 0
        self.cache_misses = 0
        
    def initialize(self, ctx):
        
        super(CachedTableLookup, self).initialize(ctx)
        self._cache = Cache().cache() 
        
    def finalize(self, ctx):
        
        logger.info ("%s  hits/misses: %d/%d" % (self, self.cache_hits, self.cache_misses))
        
        super(CachedTableLookup, self).finalize(ctx)
        
    def process(self, ctx, m):

        keys = self._resolve_lookup_keys(ctx, m)
        cache_key = tuple(sorted(keys.items()))
        
        result = self._cache.get(cache_key, CachedTableLookup.NOT_CACHED)
        if (result != CachedTableLookup.NOT_CACHED):
            self.cache_hits = self.cache_hits + 1
            if (ctx.debug2): logger.debug("Query cache hit: %s" % (result))
        else:
            self.cache_misses = self.cache_misses + 1
            result = self._do_lookup(ctx, m, keys)
            self._cache.put(cache_key, result)

        if (result):
            Eval.process_mappings(ctx, m, self.mappings, result)
        else:
            m.update({ k: ctx.interpolate(m, v) for k,v in self.default.items() })

        yield m

