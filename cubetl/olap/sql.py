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


from abc import ABCMeta, abstractmethod
import logging
import sys

from cubetl.core import Component, Node
from cubetl.core.exceptions import ETLConfigurationException, ETLException
from cubetl.olap import Measure, Key, HierarchyDimension, \
    Dimension
from cubetl.script import Eval
from cubetl.sql.cache import CachedSQLTable
from cubetl.sql.sql import SQLTable
from sqlalchemy.orm import mapper
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.sql.functions import func


logger = logging.getLogger(__name__)


class OlapMapping(Component):

    FUNCTION_DAY = "day"
    FUNCTION_MONTH = "month"
    FUNCTION_QUARTER = "quarter"
    FUNCTION_YEAR = "year"
    FUNCTION_WEEK = "week"

    def __init__(self, path, sqlcolumn, function=None):
        super().__init__()

        #self.entity = entity        # Entity to which the attribute belongs
        #self.attribute = attribute  # Mapped attribute
        self.path = path            # Attribute path to the mapped attribute (relative to the mapper entity)
        self.sqlcolumn = sqlcolumn  # SQLColumn of the SQLTable that contains the attribute
        self.function = function    # Extract function

    def __str__(self):
        return "OlapMapping(path=%s, sqlcolumn=%s, f=%s)" % (self.path, self.sqlcolumn, self.function)


class OlapSQLJoin(Component):

    def __init__(self, master_column, detail_column, alias=None):
        super().__init__()

        self.alias = alias if alias else master_column.sqltable.name
        self.master_entity = master_column
        self.detail_entity = detail_column


class OlapSQLMapping():
    """
    This is a mapping already resolved for querying, returned by sql_mappings.
    """
    def __init__(self, path, entity, attribute, sqltable, sqlcolumn, sqltable_alias, sqlcolumn_alias, function=None):
        super().__init__()

        #self.parent = parent  # X.x = x.x
        self.path = path            # Array of fields that reach this field, this included
        self.entity = entity        # Entity to which this attribute belongs
        self.attribute = attribute
        self.sqltable_alias = sqltable_alias
        self.sqlcolumn_alias = sqlcolumn_alias
        self.sqltable = sqltable
        self.sqlcolumn = sqlcolumn
        self.function = function

        #self.entity = entity
        #self.attribute = attribute  # Mapped attribute
        #self.sqlcolumn = sqlcolumn  # SQLColumn of the SQLTable that contains the attribute
        #self.function = function    # Extract function


    def __repr__(self):
        return "%s -> %s.%s" % (self.path, self.sqltable_alias, self.sqlcolumn_alias)


class TableMapper(Component):
    """
    Abstract base class for Olap Entity SQL Mappers.

    An OlapSQLMapper is a contianer of OlapMappings, which associate Olap entities and
    attributes to SQL tables and columns (and aliases).
    """

    __metaclass__ = ABCMeta

    STORE_MODE_LOOKUP = SQLTable.STORE_MODE_LOOKUP
    STORE_MODE_INSERT = SQLTable.STORE_MODE_INSERT
    STORE_MODE_UPSERT = SQLTable.STORE_MODE_UPSERT

    def __init__(self, entity, sqltable, mappings=None, lookup_cols=None):
        super().__init__()

        self.eval = []
        self.entity = entity
        self.sqltable = sqltable
        self.mappings = mappings if mappings else []

        self.lookup_cols = lookup_cols

        self.olapmapper = None

        self.auto_store = None
        self.store_mode = TableMapper.STORE_MODE_LOOKUP

        self._lookup_changed_fields = []
        self._uses_table = True

    def __str__(self, *args, **kwargs):

        if (self.entity != None):
            return "%s(%s)" % (self.__class__.__name__, self.entity.name)
        else:
            return super().__str__(*args, **kwargs)

    def sql_mappings(self, ctx):
        # Return own mappings
        result = []
        for mapping in self.mappings:
            #sqlmapping = OlapSQLMapping(mapping. self.entity, mapping.entity, self.sqltable.name if self.sqltable else None, mapping.sqlcolumn, mapping.function)
            sqlmapping = OlapSQLMapping([p.name for p in mapping.path],
                                        None, mapping.path[-1],
                                        self.sqltable, mapping.sqlcolumn,
                                        [], mapping.sqlcolumn.name,
                                        mapping.function)
            result.append(sqlmapping)

        # Add related dimension mappings
        # TODO: allow for a "publish: False" setting to avoid publishing dimensions recursively?
        for dimensionattribute in self.entity.get_dimensions():
            dimension = dimensionattribute.dimension
            mapper = self.olapmapper.entity_mapper(dimension, fail=False)
            if mapper:
                dim_mappings = mapper.sql_mappings(ctx)
                for mapping in dim_mappings:
                    sqlmapping = OlapSQLMapping([dimensionattribute.name] + mapping.path,
                                                mapping.entity, mapping.attribute,
                                                mapping.sqltable, mapping.sqlcolumn,
                                                [dimensionattribute.name] + mapping.sqltable_alias, mapping.sqlcolumn_alias,
                                                mapping.function)

                    #if sqlmapping.sqlcolumn is None:
                    #    self_olap_mapping = self.mappings[0]
                    #    sqlmapping = OlapSQLMapping(mapping.parent, mapping.field, self.entity.name, self.entity, mapping.function)

                    #print("MAPPING: %s" % sqlmapping)
                    result.append(sqlmapping)

        return result

    def sql_joins(self, ctx, master=None):
        """
        Joins related to this entity.
        """
        joins = []

        for dim_attr in self.entity.get_dimensions():
            dim = dim_attr.dimension
            dim_mapper = self.olapmapper.entity_mapper(dim, fail=False)
            if dim_mapper:
                entity_joins = dim_mapper.sql_joins(ctx, self.entity)
                for join in entity_joins:
                    join['alias'] = [dim_attr.name] + join['alias']
                    joins.append(join)

        # Embedded mappings
        pk = self.pk(ctx)
        if pk is None or pk.sqlcolumn is None:
            pass
        elif master is not None:
            # Search column name of the foreign key that references this primary key
            master_column_name = "<EXPORT ERROR>" #self.entity.name
            for column in self.olapmapper.entity_mapper(master).sqltable.columns:
                if hasattr(column, "fk_sqlcolumn"):
                    if column.fk_sqlcolumn == pk.sqlcolumn:
                        master_column_name = column.name

            joins.append({"alias": [],
                          "master_entity": master,
                          "master_column": master_column_name,
                          "detail_entity": (self.olapmapper.entity_mapper(self.entity.fact).pk(ctx).sqlcolumn.sqltable.name) if (hasattr(self.entity, "fact")) else self.pk(ctx).sqlcolumn.sqltable.name,
                          "detail_column": (self.olapmapper.entity_mapper(self.entity.fact).pk(ctx).sqlcolumn.name) if (hasattr(self.entity, "fact")) else self.pk(ctx).sqlcolumn.name,
                          })

        return joins


    def initialize(self, ctx):

        super().initialize(ctx)

        if (self.entity == None):
            raise Exception("No entity defined for %s" % self)

        ctx.comp.initialize(self.entity)

        # Apply a caching layer
        # TODO: shall at least be optional, also, columns are referenced to the backed table
        # another option is that everybody that wants caching adds the wrapper, or maybe that
        # tables natively support caching.
        if self._uses_table:
            self._sqltable = CachedSQLTable(sqltable=self.sqltable)
        #self._sqltable = self.sqltable

            # Assert that the sqltable is clean
            #if (len(self._sqltable.columns) != 0): raise AssertionError("SQLTable '%s' columns shall be empty!" % self._sqltable.name)

        # If lookup_cols is a string, split by commas
        if (isinstance(self.lookup_cols, str)): self.lookup_cols = [ key.strip() for key in self.lookup_cols.split(",") ]

        #Mappings.includes(ctx, self.mappings)
        for mapping in self.mappings:
            try:
                if mapping.path is None:
                    raise Exception("Mapping entity is None: %s" % self)
            except TypeError as e:
                raise Exception("Could not initialize mapping '%s' of '%s': %s" % (mapping, self, e))

        if self._uses_table:

            # If no key, use pk()
            if self.lookup_cols is None:
                pk = self.pk(ctx)
                if (pk is None) or (pk.sqlcolumn.type == "AutoIncrement"):
                    raise Exception("No lookup cols defined for %s" % self)
                self.lookup_cols = [pk]

            ctx.comp.initialize(self._sqltable)

    def finalize(self, ctx):
        if self._sqltable:
            ctx.comp.finalize(self._sqltable)
        ctx.comp.finalize(self.entity)
        super().finalize(ctx)

    def pk(self, ctx):
        #Returns the primary key mapping.

        #mappings = self._mappings(ctx)
        pk_mappings = [mapping for mapping in self.mappings if isinstance(mapping.path[-1], Key)]

        if (len(pk_mappings) > 1):
            #raise Exception("%s has multiple primary keys mapped: %s" % (self, pk_mappings))
            logger.warn("%s has multiple primary keys mapped: %s (ignoring)" % (self, pk_mappings))
            return None
        elif (len(pk_mappings) == 1):
            return pk_mappings[0]
        #elif (len(pk_mappings) == 0 and len(mappings) == 1):
        #    return mappings[0]
        else:
            return None

    def query_aggregate(self, ctx, drills, cuts, limit=5000):
        mappings = self.sql_mappings(ctx)
        joins = self.sql_joins(ctx, None)
        pk = self.pk(ctx)

        connection = self.sqltable.connection.connection()
        engine = self.sqltable.connection._engine

        # Build query
        Session = sessionmaker()
        Session.configure(bind=engine)
        session = Session()
        q = session.query()

        #q = q.add_columns(self.sqltable.sa_table.columns['is_bot_id'].label("x"))
        #q = q.add_entity(self.sqltable.sa_table)

        # Include measures
        for measure in [m for m in mappings if isinstance(m.field, Measure)]:
            sa_column = self.sqltable.sa_table.columns[measure.sqlcolumn.name]
            q = q.add_columns(func.avg(sa_column).label(measure.field.name + "_avg"))
            q = q.add_columns(func.sum(sa_column).label(measure.field.name + "_sum"))

        q = q.add_columns(func.count(self.sqltable.sa_table).label("record_count"))

        # Drills
        for dimension in [m for m in mappings if isinstance(m.field, Dimension)]:
            # We shoulld check the dimension-path here, with drills, and use key/lookup for drill
            if dimension.field.name in drills:
                sa_column = None
                try:
                    sa_column = self.sqltable.sa_table.columns[dimension.sqlcolumn.name]
                except KeyError as e:
                    raise ETLException("Unknown column in backend SQL table (table=%s, column=%s). Columns: %s" % (self.sqltable.sa_table, dimension.sqlcolumn.name, [c.name for c in self.sqltable.sa_table.columns]))
                q = q.add_columns(sa_column)
                q = q.group_by(sa_column)

        # Cuts
        # TODO: Filterng on any dimension attribute, not only the key
        #       (ie filter cities with type A or icon B), but then again
        #       that could be a different (nested) dimension.
        for dimension in [m for m in mappings if isinstance(m.field, Dimension)]:
            # We shoulld check the dimension-path here, with drills
            if dimension.field.name in cuts.keys():
                sa_column = self.sqltable.sa_table.columns[dimension.sqlcolumn.name]
                cut_value = cuts[dimension.field.name]
                q = q.filter(sa_column==cut_value)

        # Limit
        q = q.limit(5000)

        statement = q.statement
        logger.debug("Statement: %s", str(statement).replace("\n", " "))
        rows = connection.execute(statement).fetchall()

        return rows

    def store(self, ctx, m):

        # Resolve evals
        Eval.process_evals(ctx, m, self.eval)

        # Store automatically or include dimensions
        if self.auto_store is not None:
            logger.debug("Storing automatically: %s" % (self.auto_store))
            for ast in self.auto_store:
                did = self.olapmapper.entity_mapper(ast).store(ctx, m)
                # TODO: Review and use PK properly
                m[ast.name + "_id"] = did
        else:
            dimensions = self.entity.get_dimensions()
            if dimensions:
                logger.debug("Storing automatically: %s" % ([da.dimension.name for da in self.entity.get_dimensions()]))
                for dim_attr in self.entity.get_dimensions():
                    dim = dim_attr.dimension
                    mapper = self.olapmapper.entity_mapper(dim, False)
                    if mapper:
                        did = self.olapmapper.entity_mapper(dim).store(ctx, m)
                        # FIXME: shall use the correct foreign key column according to mappings
                        m[dim.name + "_id"] = did

        logger.debug("Storing entity in %s (mode: %s, lookup: %s)" % (self, self.store_mode, self.lookup_cols))

        data = {}
        mappings = self.sql_mappings(ctx)

        # First try to look it up
        for mapping in mappings:
            if (mapping.sqlcolumn.name in self.lookup_cols):
                if (mapping.sqlcolumn.type != "AutoIncrement"):
                    try:
                        data[mapping.sqlcolumn.name] = m[mapping.sqlcolumn.name]
                    except KeyError as e:
                        raise ETLException("Could not find key '%s' on message when storing data in %s (fields: %s)." % (mapping.sqlcolumn.name, self.entity, sorted([str(k) for k in m.keys()])))

        row = None
        if (self.store_mode == TableMapper.STORE_MODE_LOOKUP):
            row = self._sqltable.lookup(ctx, data)

        for mapping in mappings:
            #print(mapping.sqlcolumn.name + "->" + mapping.field.name)
            if (mapping.sqlcolumn.type != "AutoIncrement"):
                if mapping.sqlcolumn.name not in m:
                    raise Exception("Key '%s' does not exist in message when assigning data for column %s in %s (fields: %s)" % (mapping.field.name, mapping.sqlcolumn.name, self, [f for f in m.keys()]))
                data[mapping.sqlcolumn.name] = m[mapping.sqlcolumn.name]

        if (not row):
            if (ctx.debug2):
                logger.debug("Storing data in %s (data: %s)" % (self, data))
            if (self.store_mode in [TableMapper.STORE_MODE_LOOKUP, TableMapper.STORE_MODE_INSERT]):
                row = self._sqltable.insert(ctx, data)
            else:
                raise Exception("Update store mode used at %s (%s) not implemented (available 'lookup', 'insert')" % (self, self.store_mode))
        else:
            # Check row is identical to issue a warning
            # TODO: this shall be optional, check is expensive (no check, warning, fail)
            for mapping in mappings:
                if mapping.sqlcolumn.sqltable != self.sqltable:
                    continue
                if mapping.sqlcolumn.type != "AutoIncrement":
                    v1 = row[mapping.sqlcolumn.name]
                    v2 = data[mapping.sqlcolumn.name]
                    if (isinstance(v1, str) or isinstance(v2, str)):
                        if not isinstance(v1, str):
                            v1 = str(v1)
                        if not isinstance(v2, str):
                            v2 = str(v2)
                    if v1 != v2:
                        # Give warning just one time for each field
                        if (mapping.sqlcolumn not in self._lookup_changed_fields):
                            logger.warn("%s looked up an entity which exists with different attributes (field=%s, existing_value=%r, tried_value=%r) (reported only once per field)" % (self, mapping.sqlcolumn, v1, v2))
                            self._lookup_changed_fields.append(mapping.sqlcolumn)

        pk = self.pk(ctx)
        return row[pk.sqlcolumn.name] if pk else None

