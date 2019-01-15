import logging
from cubetl.sql.sql import SQLTable, QueryLookup
from cubetl.util.cache import Cache
from cubetl.core import Node, Component

# Get an instance of a logger
logger = logging.getLogger(__name__)


class CachedSQLTable(Component):

    # TODO: Note: this caches only when there are results (for OLAP classes). At least this shall be optional.

    # TODO: This class should compose (proxy) a SQLTable, instead of extending it (?)

    def __init__(self, sqltable):

        super().__init__()

        self._sqltable = sqltable

        self._cache = None
        self.cache_hits = 0
        self.cache_misses = 0

    def initialize(self, ctx):

        #super(CachedSQLTable, self).initialize(ctx)
        ctx.comp.initialize(self._sqltable)
        self._cache = Cache().cache()

    def finalize(self, ctx):

        if (self.cache_hits + self.cache_misses > 0):
            logger.info ("%s  hits/misses: %d/%d (%.2f%%)" % (self, self.cache_hits, self.cache_misses, float(self.cache_hits) / (self.cache_hits + self.cache_misses) * 100))

        ctx.comp.finalize(self._sqltable)
        #super(CachedSQLTable, self).finalize(ctx)

    def _find(self, ctx, attribs):

        rows = None
        cache_key = tuple(sorted(attribs.items()))

        # Check if using primary key
        if (len(attribs.keys()) > 0):
                rows = self._cache.get(cache_key)
                if (rows is not None) and (ctx.debug2):
                    logger.debug("Returning row from cache for search attribs: %s" % (attribs))

        # TODO: Cache also on natural keys

        # Run through parent
        if rows is None:

            self.cache_misses = self.cache_misses + 1

            #query = self.sa_table.select(self._attribsToClause(attribs))
            #rows = self.connection.engine().execute(query)

            rowsb = self._sqltable._find(ctx, attribs)

            rows = []
            for row in rowsb:
                rows.append(row)

            # Cache if appropriate
            if (len(attribs.keys()) > 0):
                if (len(rows) > 0):
                    if (ctx.debug2):
                        logger.debug("Caching row: %s = %s" % (attribs, rows))
                    self._cache.put(cache_key, rows)
        else:
            self.cache_hits = self.cache_hits + 1

        return iter(rows)

    def lookup(self, ctx, attribs):
        return self._sqltable.lookup(ctx, attribs, find_function=self._find)

    def insert(self, ctx, data):
        return self._sqltable.insert(ctx, data)


class CachedQueryLookup(QueryLookup):

    NOT_CACHED = "NOT_CACHED"

    connection = None
    query = None

    _cache = None
    cache_hits = 0
    cache_misses = 0

    def initialize(self, ctx):

        super(CachedQueryLookup, self).initialize(ctx)
        self._cache = Cache().cache()

    def finalize(self, ctx):

        if (self.cache_hits + self.cache_misses > 0):
            logger.info ("%s  hits/misses: %d/%d (%.2f%%)" % (self, self.cache_hits, self.cache_misses, float(self.cache_hits) / (self.cache_hits + self.cache_misses) * 100))

        super(CachedQueryLookup, self).finalize(ctx)

    def process(self, ctx, m):

        query = ctx.interpolate(self.query, m)

        result = self._cache.get(query, CachedQueryLookup.NOT_CACHED)
        if (result != CachedQueryLookup.NOT_CACHED):
            self.cache_hits = self.cache_hits + 1
            if (ctx.debug2):
                logger.debug("Query cache hit: %s" % (result))
        else:
            self.cache_misses = self.cache_misses + 1
            result = self._do_query(query)
            self._cache.put(query, result)

        if result is not None:
            m.update(result)

        yield m

