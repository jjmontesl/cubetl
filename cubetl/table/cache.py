# CubETL
# Copyright (c) 2013-2019 Jose Juan Montes

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


from os import listdir
from os.path import isfile, join
import chardet
import csv
import itertools
import logging
import re

from cubetl.core import Node, Component
from cubetl.fs import FileReader
from cubetl.script import Eval
from cubetl.table import TableLookup
from cubetl.util.cache import Cache


# Get an instance of a logger
logger = logging.getLogger(__name__)


class CachedTableLookup(TableLookup):

    NOT_CACHED = "NOT_CACHED"

    def __init__(self, table, lookup, default=None):
        super().__init__(table=table, lookup=lookup, default=default)

        self.cache_hits = 0
        self.cache_misses = 0

        self._cache = None

    def initialize(self, ctx):

        super(CachedTableLookup, self).initialize(ctx)
        self._cache = Cache().cache()

    def finalize(self, ctx):

        logger.info("%s  hits/misses: %d/%d (%.2f%%)" % (self, self.cache_hits, self.cache_misses, float(self.cache_hits) / (self.cache_hits + self.cache_misses) * 100))

        super(CachedTableLookup, self).finalize(ctx)

    def process(self, ctx, m):

        keys = self._resolve_lookup_keys(ctx, m)
        cache_key = tuple(sorted(keys.items()))

        result = self._cache.get(cache_key, CachedTableLookup.NOT_CACHED)
        if result != CachedTableLookup.NOT_CACHED:
            self.cache_hits = self.cache_hits + 1
        else:
            self.cache_misses = self.cache_misses + 1
            result = self._do_lookup(ctx, m, keys)
            self._cache.put(cache_key, result)

        if result:
            Eval.process_evals(ctx, m, self.mappings, result)
        else:
            print(self.table._rows)
            raise Exception("No rows found when looking up in %s: %s" % (self, keys))
            m.update({ k: ctx.interpolate(m, v) for k, v in self.default.items() })

        if (ctx.debug2):
            logger.debug("Cache table lookup (lookup=%s): %s" % (keys, result))

        yield m

