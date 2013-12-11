import logging
from lxml import etree
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import Table, MetaData, Column
from sqlalchemy.types import Integer, String, Float
import sys
from cubetl.core import Node
from sqlalchemy.sql.expression import and_
from cubetl.sql import SQLTable, LookupQuery
from repoze.lru import LRUCache

# Get an instance of a logger
logger = logging.getLogger(__name__)

class Cache():
    
    def __init__(self):
         
        self._cache = None
         
    def initialize(self): 
         
        if (self._cache == None):
         
            self._cache = LRUCache(512) # 100 max length
        
    def cache(self):
        self.initialize()
        return self._cache

class CachedSQLTable(SQLTable):
    
    def __init__(self):

        super(CachedSQLTable, self).__init__()

        self._cache = None
        self.cache_hits = 0
        self.cache_misses = 0
        
        # TODO: Note: this caches only when there are results (for OLAP classes). At least this shall be optional.
        
    def initialize(self, ctx):
        
        super(CachedSQLTable, self).initialize(ctx)
        self._cache = Cache().cache() 
        
    def finalize(self, ctx):
        
        logger.info ("%s  hits/misses: %d/%d" % (self, self.cache_hits, self.cache_misses))
        
        super(CachedSQLTable, self).finalize(ctx)
        
    def _find(self, ctx, attribs):

        rows = None
        cache_key = tuple(sorted(attribs.items()))
        
        # Check if using primary key
        if (len(attribs.keys()) >0):
                rows = self._cache.get(cache_key)
                if (rows != None) and (ctx.debug2):  
                    logger.debug("Returning row from cache for search attibs: %s" % (attribs))

        # TODO: Cache also on natural keys
                
        # Run through parent
        if (rows == None):
            
            self.cache_misses = self.cache_misses + 1
            
            #query = self.sa_table.select(self._attribsToClause(attribs))
            #rows = self.connection.engine().execute(query)
            
            rowsb = super(CachedSQLTable, self)._find(ctx, attribs)

            rows = []             
            for row in rowsb:
                rows.append(row) 
        
            # Cache if appropriate
            if (len(attribs.keys()) > 0):
                if (len(rows) > 0):
                    if (ctx.debug2): logger.debug("Caching row: %s = %s" % (attribs, rows))
                    self._cache.put(cache_key, rows)
        else:
            self.cache_hits = self.cache_hits + 1
        
        return iter(rows)
        
class CachedLookupQuery(LookupQuery):
    
    NOT_CACHED = "NOT_CACHED"
    
    def __init__(self):

        super(CachedLookupQuery, self).__init__()

        self.connection = None
        self.query = None

        self._cache = None
        self.cache_hits = 0
        self.cache_misses = 0
        
    def initialize(self, ctx):
        
        super(CachedLookupQuery, self).initialize(ctx)
        self._cache = Cache().cache() 
        
    def finalize(self, ctx):
        
        logger.info ("%s  hits/misses: %d/%d" % (self, self.cache_hits, self.cache_misses))
        
        super(CachedLookupQuery, self).finalize(ctx)
        
    def process(self, ctx, m):

        query = ctx.interpolate(m, self.query)
        
        result = self._cache.get(query, CachedLookupQuery.NOT_CACHED)
        if (result != CachedLookupQuery.NOT_CACHED):
            self.cache_hits = self.cache_hits + 1
            if (ctx.debug2): logger.debug("Query cache hit: %s" % (result))
        else:
            self.cache_misses = self.cache_misses + 1
            result = self._do_query(query)
            self._cache.put(query, result)

        if (result != None): m.update(result)

        yield m

