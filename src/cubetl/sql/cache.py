import logging
from lxml import etree
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import Table, MetaData, Column
from sqlalchemy.types import Integer, String, Float
import sys
from cubetl.core import Node
from sqlalchemy.sql.expression import and_
from cubetl.sql import SQLTable
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

class CachingSQLTable(SQLTable):
    
    def __init__(self):

        super(CachingSQLTable, self).__init__()

        self._cache = Cache().cache()
        
    def signal(self, ctx, s):
        
        super(CachingSQLTable, self).signal(ctx, s)    
        
    def find(self, ctx, attribs):

        rows = None
        
        # Check if using primary key
        if (len(attribs.keys()) == 1):
            if (attribs.keys()[0] == self.pk(ctx)["name"]):
                rows = self._cache.get(attribs.values()[0])
                if (rows != None) and (ctx.debug2):  
                    logger.debug("Row cache hit: %s" % (attribs.values()[0]))

        # TODO: Cache also on natural keys
                
        # Run through parent
        if (rows == None):
            
            #query = self.sa_table.select(self._attribsToClause(attribs))
            #rows = self.connection.engine().execute(query)
            
            rowsb = super(CachingSQLTable, self).find(ctx, attribs)

            rows = []             
            for row in rowsb:
                rows.append(row) 
        
            # Cache if appropriate
            if (len(attribs.keys()) == 1):
                if (attribs.keys()[0] == self.pk(ctx)["name"]):
                    if (len(rows) > 0):
                        if (ctx.debug2): logger.debug("Caching row: %s = %s" % (attribs.values()[0], rows))
                        self._cache.put(attribs.values()[0], rows)
        
        return iter(rows)
        
