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
from abc import abstractmethod, ABCMeta
from cubetl.script import Eval


# Get an instance of a logger
logger = logging.getLogger(__name__)


class Table(Component):

    __metaclass__ = ABCMeta

    def lookup(self, ctx, attribs):

        if (len(attribs.keys()) == 0):
            raise Exception("Cannot lookup on table with no criteria (empty attribute set)")

        rows = self.find(ctx, attribs)
        rows = list(rows)
        if (len(rows) > 1):
            raise Exception("Found more than one row when searching for just one in %s: %s" % (self, attribs))
        elif (len(rows) == 1):
            row = rows[0]
        else:
            row = None
            #raise Exception("No rows found when looking up in %s: %s" % (self, attribs))

        logger.debug("Lookup result on %s: %s = %s" % (self, attribs, row))

        return row

    @abstractmethod
    def find(self, ctx, attribs):
        pass

    @abstractmethod
    def insert(self, ctx, attribs):
        pass

    @abstractmethod
    def update(self, ctx, attribs, lookup):
        pass

    @abstractmethod
    def upsert(self, ctx, attribs, lookup):
        pass

    @abstractmethod
    def delete(self, ctx, lookup):
        pass


class MemoryTable(Table):

    _rows = []

    def __init__(self):

        super(MemoryTable, self).__init__()

        self._rows = []

    def find(self, ctx, attribs):
        logger.debug("Searching %s in %s" % (attribs, self))

        for row in self._rows:
            match = True
            for key in attribs.keys():
                if (row[key] != attribs[key]):
                    match = False
                    break

            if (match):
                yield row


    def insert(self, ctx, attribs):
        if (ctx.debug2): logger.debug("Inserting %s in %s" % (attribs, self))

        # TODO: Copy?
        self._rows.append(attribs)

    def update(self, ctx, attribs, lookup):
        pass

    def upsert(self, ctx, attribs, lookup):
        pass

    def delete(self, ctx, lookup):
        pass

class TableInsert(Node):

    def __init__(self):

        super(TableInsert, self).__init__()

        self.table = None
        self.mappings = None

    def initialize(self, ctx):

        super(TableInsert, self).initialize(ctx)
        ctx.comp.initialize(self.table)

    def finalize(self, ctx):

        ctx.comp.finalize(self.table)
        super(TableInsert, self).finalize(ctx)

    def process(self, ctx, m):

        # Process mappings
        attribs = {}
        for mapping in self.mappings:
            if "value" in mapping:
                attribs[mapping["name"]] = ctx.interpolate(m, mapping["value"])
            else:
                attribs[mapping["name"]] = m[mapping["name"]]

        # Store
        self.table.insert(ctx, attribs)

        yield m


class TableLookup(Node):

    def __init__(self):

        super(TableLookup, self).__init__()

        self.table = None
        self.lookup = { }
        self.default = { }
        self.mappings = []

    def initialize(self, ctx):

        super(TableLookup, self).initialize(ctx)
        ctx.comp.initialize(self.table)

    def finalize(self, ctx):

        ctx.comp.finalize(self.table)
        super(TableLookup, self).finalize(ctx)

    def _resolve_lookup_keys (self, ctx, m):

        if (not self.lookup):
            raise Exception("No lookup configuration defined for %s" % self)

        keys = {}
        for (key, expr) in self.lookup.items():
            keys[key] = ctx.interpolate(m, expr)

        return keys

    def _do_lookup(self, ctx, m, keys):
        result = self.table.lookup(ctx, keys)
        return result

    def process(self, ctx, m):

        keys = self._resolve_lookup_keys(ctx, m)

        result = self._do_lookup(ctx, m, keys)

        if (result):
            Eval.process_mappings(ctx, m, self.mappings, result)
        else:
            m.update({ k: ctx.interpolate(m, v) for k,v in self.default.items() })

        yield m
