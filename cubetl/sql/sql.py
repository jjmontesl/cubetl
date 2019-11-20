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


import logging
import sys

from cubetl.core import Node, Component
from cubetl.core.exceptions import ETLConfigurationException
from cubetl.text.functions import parsebool
from sqlalchemy.engine import create_engine
from sqlalchemy.exc import ResourceClosedError
from sqlalchemy.schema import Table, MetaData, Column, ForeignKey
from sqlalchemy.sql.expression import and_
from sqlalchemy.types import Integer, String, Float, Boolean, Unicode, Date, Time, DateTime, Binary


# Get an instance of a logger
logger = logging.getLogger(__name__)


class Connection(Component):

    def __init__(self, url, connect_args=None):
        super().__init__()
        self.url = url
        self.connect_args = connect_args or {}  # {'sslmode':'require'}

        self._engine = None

    #def __repr__(self):
    #    return "%s(url='%s')" % (self.__class__.__name__, self._url)

    def lazy_init(self):
        if self._engine is None:
            url = self.url
            logger.info("Connecting to database: %s (%s)", url, self.connect_args)
            self._engine = create_engine(url, connect_args=self.connect_args)
            self._connection = self._engine.connect()

    def connection(self):
        self.lazy_init()
        return self._connection

    def engine(self):
        self.lazy_init()
        return self._engine


class SQLColumn(Component):

    TYPE_INTEGER = "INTEGER"
    TYPE_STRING = "TEXT"

    def __init__(self, name, type, pk=False, nullable=True, label=None):
        super(SQLColumn, self).__init__()
        self.sqltable = None
        self.name = name
        self.type = type
        self.pk = pk
        self.label = label or name
        self.nullable = nullable

    def __str__(self):
        return "%s(name=%s)" % (self.__class__.__name__, self.name)


class SQLColumnFK(SQLColumn):

    def __init__(self, name, type, pk, fk_sqlcolumn, null=True, label=None):
        super(SQLColumnFK, self).__init__(name, type, pk, null, label=label)
        self.fk_sqlcolumn = fk_sqlcolumn


class SQLTable(Component):

    _selects = 0
    _inserts = 0
    _updates = 0
    _finalized = False

    STORE_MODE_LOOKUP = "lookup"
    STORE_MODE_INSERT = "insert"
    STORE_MODE_UPSERT = "upsert"

    _pk = False

    columns = []

    create = True

    _unicode_errors = 0
    _lookup_changed_fields = None

    def __init__(self, name, connection, columns, label=None):

        super(SQLTable, self).__init__()

        self.sa_table = None
        self.sa_metadata = None

        self.name = name
        self.connection = connection

        self.label = label if label else name

        self.columns = columns or []
        for col in columns:
            col.sqltable = self

    def _get_sa_type(self, column):

        if (column.type == "Integer"):
            return Integer
        elif (column.type == "String"):
            #if (column.length is None): column.length = 128
            return Unicode(length = 128)
        elif (column.type == "Float"):
            return Float
        elif (column.type == "Boolean"):
            return Boolean
        elif (column.type == "AutoIncrement"):
            return Integer
        elif (column.type == "Date"):
            return Date
        elif (column.type == "Time"):
            return Time
        elif (column.type == "DateTime"):
            return DateTime
        elif (column.type == "Binary"):
            return Binary
        else:
            raise Exception("Invalid data type (%s): %s" % (column, column.type))

    def finalize(self, ctx):

        if (not SQLTable._finalized):
            SQLTable._finalized = True
            if (SQLTable._inserts + SQLTable._selects > 0):
                logger.info("SQLTable Totals  ins/upd/sel: %d/%d/%d " %
                            (SQLTable._inserts, SQLTable._updates, SQLTable._selects))

        if (self._inserts + self._selects > 0):
            logger.info("SQLTable %-18s ins/upd/sel: %6d/%6d/%-6d " %
                            (self.name, self._inserts, self._updates, self._selects))
        if (self._unicode_errors > 0):
            logger.warning("SQLTable %s found %d warnings assigning non-unicode fields to unicode columns" %
                           (self.name, self._unicode_errors))

        ctx.comp.finalize(self.connection)

        super(SQLTable, self).finalize(ctx)

    def initialize(self, ctx):

        super(SQLTable, self).initialize(ctx)

        if self._lookup_changed_fields == None:
            self._lookup_changed_fields = []

        ctx.comp.initialize(self.connection)

        logger.debug("Loading table %s on %s" % (self.name, self))

        self.sa_metadata = MetaData()
        self.sa_table = Table(self.name, self.sa_metadata)

        self._selects = 0
        self._inserts = 0
        self._updates = 0
        self._unicode_errors = 0

        # Drop?

        columns_ex = []
        for column in self.columns:

            logger.debug("Adding column to %s: %s" % (self, column))

            column.sqltable = self

            # Check for duplicate names
            if (column.name in columns_ex):
                raise ETLConfigurationException("Duplicate column name '%s' in %s" % (column.name, self))

            columns_ex.append(column.name)

            # Configure column
            if isinstance(column, SQLColumnFK):
                if column.fk_sqlcolumn.sqltable.sa_table is None:
                    logger.warning("Column %s foreign key %s table (%s) has not been defined in backend (ignoring).", column, column.fk_sqlcolumn, column.fk_sqlcolumn.sqltable)
                    continue

                self.sa_table.append_column(Column(column.name,
                                                   self._get_sa_type(column),
                                                   ForeignKey(column.fk_sqlcolumn.sqltable.sa_table.columns[column.fk_sqlcolumn.name]),
                                                   primary_key=column.pk,
                                                   nullable=column.nullable,
                                                   autoincrement=(True if column.type == "AutoIncrement" else False)))
            else:
                self.sa_table.append_column(Column(column.name,
                                                   self._get_sa_type(column),
                                                   primary_key=column.pk,
                                                   nullable=column.nullable,
                                                   autoincrement=(True if column.type == "AutoIncrement" else False)))

        # Check schema:

        # Create if doesn't exist
        if (not self.connection.engine().has_table(self.name)):
            logger.info("Creating table %s" % self.name)
            self.sa_table.create(self.connection.connection())

        # TODO:? Extend?  (unsafe, allow read-only connections and make them default?)
        # TODO:? Delete columns (unsafe, allow read-only connections and make them default?)

    def pk(self, ctx):
        """
        Returns the primary key column definitToClauion, or None if none defined.
        """

        #if (self._pk == False):
        if True:
            pk_cols = []
            for col in self.columns:
                if col.pk:
                    pk_cols.append(col)

            if (len(pk_cols) > 1):
                raise Exception("Table %s has multiple primary keys: %s" % (self.name, pk_cols))
            elif (len(pk_cols) == 1):
                self._pk = pk_cols[0]
            else:
                self._pk = None

        return self._pk

    def _attribsToClause(self, attribs):
        clauses = []
        for k, v in attribs.items():
            if isinstance(v, (list, tuple)):
                clauses.append(self.sa_table.c[k].in_(v))
            else:
                clauses.append(self.sa_table.c[k] == v)

        return and_(*clauses)

    def _rowtodict(self, row):

        d = {}
        for column in self.columns:
            #print column
            d[column.name] = getattr(row, column.name)

        return d

    def _find(self, ctx, attribs):

        self._selects = self._selects + 1
        SQLTable._selects = SQLTable._selects + 1

        query = self.sa_table.select(self._attribsToClause(attribs))
        rows = self.connection.connection().execute(query)

        for r in rows:
            # Ensure we return dicts, not RowProxys from SqlAlchemy
            yield self._rowtodict(r)


    def lookup(self, ctx, attribs, find_function=None):

        logger.debug ("Lookup on '%s' attribs: %s" % (self, attribs))

        if (len(attribs.keys()) == 0):
            raise Exception("Cannot lookup on table '%s' with no criteria (empty attribute set)" % self.name)

        find_function = find_function or self._find
        rows = find_function(ctx, attribs)
        rows = list(rows)
        if (len(rows) > 1):
            raise Exception("Found more than one row when searching for just one in table %s: %s" % (self.name, attribs))
        elif (len(rows) == 1):
            row = rows[0]
        else:
            row = None

        logger.debug("Lookup result on %s: %s = %s" % (self.name, attribs, row))
        return row

    def upsert(self, ctx, data, keys = []):
        """
        Upsert checks if the row exists and has changed. It does a lookup
        followed by an update or insert as appropriate.
        """

        # TODO: Check for AutoIncrement in keys, shall not be used

        # If keys
        qfilter = {}
        if (len(keys) > 0):
            for key in keys:
                try:
                    qfilter[key] = data[key]
                except KeyError as e:
                    raise Exception("Could not find attribute '%s' in data when storing row data: %s" % (key, data))
        else:
            pk = self.pk(ctx)
            qfilter[pk.name] = data[pk.name]

        # Do lookup
        if len(qfilter) > 0:

            row = self.lookup(ctx, qfilter)

            if (row):
                # Check row is identical
                for c in self.columns:
                    if c.type != "AutoIncrement":
                        v1 = row[c.name]
                        v2 = data[c.name]
                        if c.type == "Date":
                            v1 = row[c.name].strftime('%Y-%m-%d')
                            v2 = data[c.name].strftime('%Y-%m-%d')
                        if (isinstance(v1, str) or isinstance(v2, str)):
                            if (not isinstance(v1, str)): v1 = str(v1)
                            if (not isinstance(v2, str)): v2 = str(v2)
                        if (v1 != v2):
                            if (c.name not in self._lookup_changed_fields):
                                logger.warning("%s updating an entity that exists with different attributes, overwriting (field=%s, existing_value=%s, tried_value=%s)" % (self, c.name, v1, v2))
                                #self._lookup_changed_fields.append(c["name"])

                # Update the row
                row = self.update(ctx, data, keys)
                return row

        row_with_id = self.insert(ctx, data)
        return row_with_id

    def _prepare_row(self, ctx, data):

        row = {}

        for column in self.columns:
            if column.type != "AutoIncrement":
                try:
                    row[column.name] = data[column.name]
                except KeyError as e:
                    raise Exception("Missing attribute for column %s in table '%s' while inserting row: %s" % (e, self.name, data))

                # Checks
                if (column.type == "String") and (not isinstance(row[column.name], str)):
                    self._unicode_errors = self._unicode_errors + 1
                    if (ctx.debug):
                        logger.warning("Unicode column %r received non-unicode string: %r " % (column.name, row[column.name]))

        return row

    def insert(self, ctx, data):

        row = self._prepare_row(ctx, data)

        logger.debug("Inserting in table '%s' row: %s" % (self.name, row))
        res = self.connection.connection().execute(self.sa_table.insert(row))

        pk = self.pk(ctx)
        if pk:
            row[pk.name] = res.inserted_primary_key[0]

        self._inserts = self._inserts + 1
        SQLTable._inserts = SQLTable._inserts + 1

        if pk is not None:
            return row
        else:
            return row  # None

    def update(self, ctx, data, keys = []):

        row = self._prepare_row(ctx, data)

        # Automatically calculate lookup if necessary
        qfilter = {}
        if (len(keys) > 0):
            for key in keys:
                try:
                    qfilter[key] = data[key]
                except KeyError as e:
                    raise Exception("Could not find attribute '%s' in data when storing row data: %s" % (key, data))
        else:
            pk = self.pk(ctx)
            qfilter[pk.name] = data[pk.name]

        logger.debug("Updating in table '%s' row: %s" % (self.name, row))
        res = self.connection.connection().execute(self.sa_table.update(self._attribsToClause(qfilter), row))

        self._updates = self._updates +1
        SQLTable._updates = SQLTable._updates + 1

        if pk is not None:
            return row
        else:
            return None


class Transaction(Node):


    def __init__(self, connection, enabled=True):
        super().__init__()
        self.connection = connection
        self.enabled = enabled
        self._transaction = None

    def initialize(self, ctx):
        super(Transaction, self).initialize(ctx)
        ctx.comp.initialize(self.connection)
        self.enabled = parsebool(self.enabled)

    def finalize(self, ctx):
        ctx.comp.finalize(self.connection)
        #super(Transaction, self).finalize(ctx)

    def process(self, ctx, m):

        # Store
        if (self._transaction != None):
            raise Exception("Trying to start transaction while one already exists is not supported")

        if (self.enabled):
            logger.info("Starting database transaction")
            self._transaction = self.connection.connection().begin()
        else:
            logger.debug("Not starting database transaction (Transaction node is disabled)")

        yield m

        if (self.enabled):
            logger.info("Commiting database transaction")
            self._transaction.commit()
            self._transaction = None


class StoreRow(Node):


    def __init__(self, sqltable, store_mode=SQLTable.STORE_MODE_INSERT):
        super(StoreRow, self).__init__()
        self.sqltable = sqltable
        self.store_mode = store_mode
        # TODO: mappings are needed (here, not in the schema ¿? :)), standarize (Key/ValueMapping? NameMapping? ColumnMapping? AttributeMapping?)

    def initialize(self, ctx):
        super(StoreRow, self).initialize(ctx)
        ctx.comp.initialize(self.sqltable)

    def finalize(self, ctx):
        ctx.comp.finalize(self.sqltable)
        super(StoreRow, self).finalize(ctx)

    def process(self, ctx, m):

        # Store
        if self.store_mode == SQLTable.STORE_MODE_UPSERT:
            self.sqltable.upsert(ctx, m)
        elif self.store_mode == SQLTable.STORE_MODE_INSERT:
            self.sqltable.insert(ctx, m)

        yield m


class QueryLookup(Node):

    connection = None
    query = None

    def initialize(self, ctx):

        super(QueryLookup, self).initialize(ctx)
        ctx.comp.initialize(self.connection)

    def finalize(self, ctx):
        ctx.comp.finalize(self.connection)
        super(QueryLookup, self).finalize(ctx)

    def _rowtodict(self, row):

        d = {}
        for column,value in row.items():
            d[column] = value

        return d

    def _do_query(self, query):

        logger.debug ("Running query: %s" % query.strip())
        rows = self.connection.connection().execute(query)

        result = None
        for r in rows:
            if (result != None):
                raise Exception ("Error: %s query resulted in more than one row: %s" % (self, self.query) )
            result = self._rowtodict(r)

        # TODO: Optional fail?
        if (not result):
            raise Exception ("Error: %s query returned no results: %s" % (self, self.query) )

        return result

    def process(self, ctx, m):

        query = ctx.interpolate(self.query, m)

        result = self._do_query(query)

        if (result != None):
            m.update(result)
        yield m


class Query(Node):
    """
    Executes a SQL query on the given connection and returns the results.

    By default, each row is returned as a new message, generated by copying
    the input message and updating its fields with the columns that resulted
    from the SQL query.

    :param connection: The SQLConnection object to use.
    :param query: The SQL query string to execute.
    :param embed: If defined, the entire results of the query are returned
                  as an array contained in the message attribute named like
                  the value of the `embed` parameter (m[embed]=query_result_array).
    :param single: If True, the process will fail if the query returns more than one row.
    :param failifempty: if True, the process will fail if the query returns no row.
    """

    def __init__(self, connection, query, embed=False, single=False, failifempty=True):
        super().__init__()
        self.connection = connection
        self.query = query
        self.embed = embed
        self.single = single
        self.failifempty = failifempty

    def initialize(self, ctx):

        super(Query, self).initialize(ctx)
        ctx.comp.initialize(self.connection)

    def finalize(self, ctx):
        ctx.comp.finalize(self.connection)
        super(Query, self).finalize(ctx)

    def _rowtodict(self, row):

        d = {}
        for column, value in row.items():
            d[column] = value

        return d

    def process(self, ctx, m):

        query = ctx.interpolate(self.query, m)

        logger.debug("Running query: %s" % query.strip())
        rows = self.connection.connection().execute(query)

        try:

            if self.embed:
                result = []
                for r in rows:
                    result.append(self._rowtodict(r))

                if self.single and len(result) > 1:
                    raise Exception("Error: %s query resulted in more than one row: %s" % (self, query))
                if len(result) == 0:
                    if self.failifempty:
                        raise Exception("Error: %s query returned no results: %s" % (self, query))
                    else:
                        result = None

                m[self.embed] = result[0] if self.single else result
                yield m

            else:
                result = None
                for r in rows:
                    if self.single and result != None:
                        raise Exception("Error: %s query resulted in more than one row: %s" % (self, query))

                    m2 = ctx.copy_message(m)
                    result = self._rowtodict(r)

                    if result is not None:
                        m2.update(result)
                        yield m2

                if not result:
                    if self.failifempty:
                        raise Exception("Error: %s query returned no results: %s" % (self, query))
                    else:
                        yield m

        except ResourceClosedError as e:
            yield m

