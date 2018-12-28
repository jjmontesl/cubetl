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

    eval = []

    auto_store = None
    store_mode = STORE_MODE_LOOKUP

    _lookup_changed_fields = []

    _uses_table = True

    olapmapper = None

    def __init__(self, entity, sqltable, mappings=None, lookup_cols=None):
        super(TableMapper, self).__init__()

        self.eval = []
        self.entity = entity
        self.sqltable = sqltable
        self.mappings = mappings if mappings else []

        self.lookup_cols = lookup_cols

        self._lookup_changed_fields = []

    def __str__(self, *args, **kwargs):

        if (self.entity != None):
            return "%s(%s)" % (self.__class__.__name__, self.entity.name)
        else:
            return super(TableMapper, self).__str__(*args, **kwargs)

    '''
    def _mappings_join(self, ctx):

        pk = self.pk(ctx)

        if pk is None:
            raise Exception("%s has no primary key and cannot provide join columns." % self)

        ctype = pk["type"]
        if (ctype == "AutoIncrement"): ctype = "Integer"
        return [{"entity": self.entity,
                 "name": self.entity.name,
                 "column": self.entity.name,
                 "type": ctype,
                 #"value": '${ m["' + self.entity.name + "_id" + '"] }'
                 "value": pk['value'] if (pk['value']) else '${ m["' + self.entity.name + "_id" + '"] }'}]
    '''

    '''
    def _mappings(self, ctx):
        return self.mappings
    '''

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

    '''
    def _extend_mappings(self, ctx, mappings, newmappings):

        for nm in newmappings:
            found = None
            for m in mappings:
                if (not "entity" in m): raise Exception("No entity defined for mapping %s" % m)
                if (not "entity" in nm): raise Exception("No entity defined for mapping %s" % nm)
                if (not isinstance(m["entity"], Component)): raise Exception("No correct entity defined for mapping %s" % m)
                if (not isinstance(nm["entity"], Component)): raise Exception("No correct entity defined for mapping %s" % nm)

                if (m["name"] == nm["name"] and m["entity"].name == nm["entity"].name):
                    found = m
                    break

            if not found:
                mappings.append(nm)
            else:
                # Update missing properties
                if (not "type" in m and "type" in nm): m["type"] = nm ["type"]
                if (not "value" in m and "value" in nm): m["value"] = nm ["value"]
                if (not "label" in m and "label" in nm): m["label"] = nm ["label"]
                if (not "column" in m and "column" in nm): m["column"] = nm ["column"]

    def _ensure_mappings(self, ctx, mappings):
        """
        """

        return mappings

        for mapping in mappings:
            mapping["pk"] = (False if (not "pk" in mapping) else parsebool(mapping["pk"]))
            if (not "column" in mapping): mapping["column"] = mapping["name"]
            if (not "value" in mapping): mapping["value"] = None

            if (mapping["pk"] and not "type" in mapping):
                if (not "value" in mapping or mapping["value"] == None):
                    mapping["type"] = "AutoIncrement"

            if (not "column" in mapping): mapping["column"] = mapping["name"]
            if (not "type" in mapping): mapping["type"] = "String"

        return mappings
    '''

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
                self.lookup_cols = [ pk ]

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
            raise Exception("%s has multiple primary keys mapped: %s" % (self, pk_mappings))
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


'''
class CompoundDimensionMapper(TableMapper):
    """
    The CompoundDimensionMapper maps together two or more entities onto
    the same SQL table.
    """

    dimensions = []
    _created_mappers = []

    def __init__(self):
        super(CompoundDimensionMapper, self).__init__()
        self.dimensions = []
        self._created_mappers = []

    def initialize(self, ctx):

        if self.dimensions == []:
            self.dimensions = []
        if self._created_mappers == []:
            self._created_mappers = []

        super(CompoundDimensionMapper, self).initialize(ctx)

    def finalize(self, ctx):

        for cm in self._created_mappers:
            ctx.comp.finalize(cm)
        super(CompoundDimensionMapper, self).finalize(ctx)

    def _mappings(self, ctx):

        #logger.debug("Calculating eval (CompoundDimensionMapper) for %s" % self)

        if (len(self.dimensions) == 0):
            raise Exception("No dimensions found in %s" % self)

        #eval = super(CompoundDimensionMapper, self)._mappings(ctx)
        mappings = [mapping.copy() for mapping in self.mappings]

        for dimension in self.dimensions:
            dimension_mapper = self.olapmapper.entity_mapper(dimension, False)

            if (dimension_mapper == None):
                # Create dimension mapper
                logger.debug("No mapper found for %s in %s, creating a default embedded dimension mapper for it." % (dimension, self))
                dimension_mapper = EmbeddedDimensionMapper()
                dimension_mapper.entity = dimension
                dimension_mapper.olapmapper = self.olapmapper
                self.olapmapper.mappers.append(dimension_mapper)
                self._created_mappers.append(dimension_mapper)
                ctx.comp.initialize(dimension_mapper)

            dimension_mappings = dimension_mapper._mappings_join(ctx)

            if (self.entity):
                for mapping in dimension_mappings:
                    mapping["entity"] = self.entity

            #for dm in dimension_mappings:
            #    dm["pk"] = False
            #print [dimension["name"] + " " + dimension["type"] for dimension in eval]
            #print [dimension["name"] + " " + dimension["type"] for dimension in dimension_mappings]

            self._extend_mappings(ctx, mappings, dimension_mappings)

        return self._ensure_mappings(ctx, mappings)


class CompoundHierarchyDimensionMapper(CompoundDimensionMapper):
    """
    This mapper maps all dimension levels of a multi-level dimension
    on a CompoundDimensionMapper, this is, onto the same table.

    This mapper is appropriate, for example, for dates and other
    multi-level dimensions when creating one table per level is not
    desired.
    """

    def initialize(self, ctx):

        # FIXME: Init code added because Yaml didn't run init
        if self.dimensions == []:
            self.dimensions = []
        if self._created_mappers == []:
            self._created_mappers = []


        if (len(self.dimensions) != 0):
            raise Exception("Cannot define dimensions in %s. Only one HierarchyDimension can be set as entity." % (self))

        try:
            for level in self.entity.levels:
                self.dimensions.append(level)
        except AttributeError as e:
            raise Exception("DimensionMapper '%s' could not iterate over the levels of the entity '%s': %s" % (self, self.entity, e))

        super(CompoundHierarchyDimensionMapper, self).initialize(ctx)


class MultiTableHierarchyDimensionMapper(TableMapper):
    """
    This dimension mapper allows to refer to a hierarchy dimension that
    is spread among several tables.

    This mapper will add a column for each referred dimension level.
    """


    def initialize(self, ctx):

        if (self.table):
            raise Exception("Cannot define table in %s. All dimensions of a MultiTableHierarchyDimensionMapper must be mapped separately." % self)
        if (self.connection):
            raise Exception("Cannot define table in %s. All dimensions of a MultiTableHierarchyDimensionMapper must be mapped separately." % self)

        # Do not call parent.

        ctx.comp.initialize(self.entity)

    def finalize(self, ctx):
        # Do not call parent.
        ctx.comp.finalize(self.entity)

    def _mappings_join(self, ctx):

        mappings = []
        for dimension in self.entity.levels:
            dimension_mapper = self.olapmapper.entity_mapper(dimension, False)
            mappings_join = dimension_mapper._mappings_join(ctx)
            #for mj in mappings_join: mj["entity"] = self.entity
            self._extend_mappings(ctx, mappings, mappings_join)

        return mappings

    def _joins(self, ctx, master):

        joins = []
        for dim in self.entity.levels:
            dim_mapper = self.olapmapper.entity_mapper(dim)
            joins.extend(dim_mapper._joins(ctx, master))

        return joins


    def _mappings(self, ctx):
        raise Exception("Cannot provide mappings for %s. No table is related to this kind of mapper." % (self))

    def store(self, ctx, m):
        raise Exception("Cannot store on %s: storing of MultiTableHierarchyDimensionMapper should be done on each related dimension as appropriate (hint: you may need to use 'auto_store' on the FactMapper to avoid automatic storage of this dimension values)." % (self))


class EmbeddedDimensionMapper(DimensionMapper):
    """
    This mapper maps a dimension by embedding its attributes directly on the parent table.

    If the mapped dimension is a HierarchyDimension, all of its levels embedded.
    """



    def __init__(self, entity, sqltable, mappings=None):
        super().__init__(entity, sqltable, mappings)

    def finalize(self, ctx):
        ctx.comp.finalize(self.entity)

    def initialize(self, ctx):

        if (not self.entity):
            raise Exception("No entity defined for %s" % self)

        # No call to constructor. No need for connection and table
        ctx.comp.initialize(self.entity)

        # Check no PK in initialize?

        # If lookup_cols is a string, split by commas
        if (self.lookup_cols != None):
            raise Exception("No lookup_cols can be defined for an embedded dimension.")

        """
        if (hasattr(self.entity, "hierarchies")):
            logger.debug("Creating CompoundHierarchyDimensionMapper for %s." % (self))
            self._back_mapper = CompoundHierarchyDimensionMapper()
            self._back_mapper.entity = self.entity
            self._back_mapper.entity = self.entity
            self._back_mapper.olapmapper = self.olapmapper
            self.olapmapper.mappers.append(self._back_mapper)
            self._back_mapper._uses_table = False
            ctx.comp.initialize(self._back_mapper)

        else:
            # TODO, may also be a CompoundDimensionMapper if not hierarchies
            pass
        """


    """
    def sql_mappings(self, ctx):
        result = []
        for mapping in self.mappings:
            sqlmapping = OlapSQLMapping(self.entity, mapping.entity, None, None, mapping.function)
            result.append(sqlmapping)
        return result
    """

    def sql_mappings(self, ctx):
        result = []
        for mapping in self.mappings:
            sqlmapping = OlapSQLMapping(self.entity, mapping.entity, self.sqltable.name if self.sqltable else None, mapping.sqlcolumn, mapping.function)
            result.append(sqlmapping)
        return result

    """
    def _mappings_join(self, ctx):

        mappings = self._mappings(ctx)

        if self._back_mapper:
            back_mappings = self._back_mapper._mappings(ctx)
            for mapping in back_mappings:
                mapping["entity"] = self.entity

                self._extend_mappings(ctx, mappings, back_mappings)

        # Optionally remove any possible primary key
        if (self.remove_pk):
            mappings = [m for m in mappings if isinstance(m.entity, Key)]

        return mappings
    """

    def sql_joins(self, ctx, master):
        return []

    def dimension(self):
        return self.entity

    def pk(self, ctx):
        return None
        # TODO: this is incorrect, shall we retrieve the key from the Dimension (?)
        pk_mappings = [mapping for mapping in self.mappings if isinstance(mapping.entity, Key)]
        if len(pk_mappings) == 0:
            pk_mappings = [mapping for mapping in self.mappings]

        pk_mapping = pk_mappings[0]
        pk_mapping = OlapMapping(self.entity, pk_mapping.sqlcolumn, pk_mapping.function)

        return pk_mappings[0]

    def store(self, ctx, m):
        # Evaluate evals, but don't store anything
        Eval.process_evals(ctx, m, self.eval)


class FactDimensionMapper(EmbeddedDimensionMapper):

    def initialize(self, ctx):

        super().initialize(ctx)

        if (not self.entity):
            raise Exception("No entity defined for %s" % self)

        #if (self.sqltable):
        #    raise Exception("Cannot define table in %s." % self)

        # No call to constructor. No need for connection and table
        ctx.comp.initialize(self.entity)

        # Check no PK in initialize?
        self.table = self.olapmapper.entity_mapper(self.entity.fact).sqltable

        # If lookup_cols is a string, split by commas
        if (self.lookup_cols != None):
            raise Exception("No lookup_cols can be defined for an embedded dimension.")

    def finalize(self, ctx):
        ctx.comp.finalize(self.entity)
        # Do no call super (no table or connection)?

    def store(self, ctx, data):
        # TODO: This shall not even be called, and raise an exception instead?
        raise Exception("Cannot store an embedded dimension")

    #def _mappings(self, ctx):
    #    mapper = self.olapmapper.entity_mapper(self.entity.fact)
    #    return mapper._mappings(ctx)

    def sql_mappings(self, ctx):
        # Return own mappings
        result = []

        mapper = self.olapmapper.entity_mapper(self.entity.fact)
        fact_mappings = mapper.sql_mappings(ctx)
        for mapping in fact_mappings:
            sqlmapping = OlapSQLMapping(mapping.parent, mapping.field, mapping.alias, mapping.sqlcolumn, mapping.function)
            result.append(sqlmapping)
        return result

    def sql_joins(self, ctx, master=None):
        result = super()._joins(ctx, master)
        logger.info("%s joins: %s" % (self, result))
        return result

    def pk(self, ctx):
        mapper = self.olapmapper.entity_mapper(self.entity.fact)
        return mapper.pk(ctx)
'''

'''
class AliasDimensionMapper(DimensionMapper):
    """
    This mapping can have a single mapping, referencing the AliasDimension itself and
    the sql column that references it.
    """

    def __init__(self, entity):
        super().__init__(entity=entity, sqltable=None)
        self._uses_table = False

    def initialize(self, ctx):

        super().initialize(ctx)

        if not self.entity:
            raise Exception("No entity defined for %s" % self)

        if self.sqltable:
            raise Exception("Cannot define table in %s." % self)

        # No call to constructor. No need for connection and table
        ctx.comp.initialize(self.entity)

        # Check no PK in initialize?
        #self.table = self.olapmapper.entity_mapper(self.entity.entity).sqltable

        # If lookup_cols is a string, split by commas
        if self.lookup_cols:
            raise Exception("No lookup_cols can be defined for an embedded dimension (mapper=%s, lookup_cols=%s)." % (self, self.lookup_cols))

    def finalize(self, ctx):
        ctx.comp.finalize(self.entity)
        # Do no call super (no table or connection)?

    def store(self, ctx, data):
        # TODO: This shall not even be called, and raise an exception instead?
        raise Exception("Cannot store an embedded dimension")

    #def _mappings(self, ctx):
    #    # TODO: Aliased dimension might not be a factdimension
    #    mapper = self.olapmapper.entity_mapper(self.entity.dimension)
    #    return mapper._mappings(ctx)

    def sql_mappings(self, ctx):
        # Add own mappings
        result = []

        own_mappings = super().sql_mappings(ctx)
        for mapping in own_mappings:
            sqlmapping = OlapSQLMapping(mapping.parent, mapping.field, mapping.alias, mapping.sqlcolumn, mapping.function)
            result.append(sqlmapping)

        # Add mappings from the aliased dimension
        mapper = self.olapmapper.entity_mapper(self.entity.dimension)
        dim_mappings = mapper.sql_mappings(ctx)
        for mapping in dim_mappings:
            # TODO: We need to carry extra arguments for aliased mappings?
            # We should prefix dimensions?
            # Override parent, but not for dimensions
            """
            if isinstance(self.entity.dimension, FactDimension) and mapping.parent == self.entity.dimension.fact:
                sqlmapping = OlapSQLMapping(self.entity, mapping.field, self.entity.name, mapping.sqlcolumn, mapping.function)
            elif isinstance(mapper, EmbeddedDimensionMapper):
                sqlmapping = OlapSQLMapping(self.entity, mapping.field, self.entity.name, mapping.sqlcolumn, mapping.function)
            elif isinstance(self.entity.dimension, HierarchyDimension):
                sqlmapping = OlapSQLMapping(mapping.parent, mapping.field, self.entity.name, mapping.sqlcolumn, mapping.function)
            else:
                sqlmapping = OlapSQLMapping(mapping.parent, mapping.field, mapping.alias, mapping.sqlcolumn, mapping.function)

            if sqlmapping.sqlcolumn is None:
                sqlmapping = OlapSQLMapping(self.entity, mapping.field, self.entity.name, None, mapping.function)
            """

            # Replace alias for joined dimensions
            alias = mapping.alias
            alias = self.entity.name  # Joined dimensions use aliased name as table

            # For dimensions, the parent will become the aliased dimension name (this mapper entity),
            parent = mapping.parent
            if parent is None:
                parent = self.entity

            sqlmapping = OlapSQLMapping(parent, mapping.field, alias, mapping.sqlcolumn, mapping.function)
            result.append(sqlmapping)

        return result

    def sql_joins(self, ctx, master=None):
        result = super().sql_joins(ctx, master)
        for join in result:
            # TODO: Create and use OlapSQLJoin objects
            join['alias'] = self.entity.name

        logger.info("%s joins: %s" % (self, result))
        return result

    def dimension(self):
        mapper = self.olapmapper.entity_mapper(self.entity.dimension)
        dimension = mapper.dimension()
        return dimension

    def pk(self, ctx):
        mapper = self.olapmapper.entity_mapper(self.entity.dimension)
        pk = mapper.pk(ctx)
        #print(pk)
        return pk
'''
