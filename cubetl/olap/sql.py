import cubetl
from abc import ABCMeta, abstractmethod
from cubetl.core import Component, Node, Mappings
from cubetl.text.functions import parsebool
from cubetl.sql import SQLTable
from cubetl.sql.cache import CachedSQLTable
import logging
from cubetl.script import Eval

# Get an instance of a logger
logger = logging.getLogger(__name__)


class TableMapper(Component):
    """
    Abstract class.
    """

    __metaclass__ = ABCMeta

    STORE_MODE_LOOKUP = "lookup"
    STORE_MODE_INSERT = "insert"
    STORE_MODE_UPSERT = "upsert"

    entity = None

    connection = None
    table = None

    eval = []
    mappings = []

    lookup_cols = None

    auto_store = None
    store_mode = STORE_MODE_LOOKUP

    _sqltable = None
    _lookup_changed_fields = []

    _uses_table = True

    olapmapper = None

    def __init__(self):
        super(TableMapper, self).__init__()
        self.eval = []
        self.mappings = []
        self._lookup_changed_fields = []

    def __str__(self, *args, **kwargs):

        if (self.entity != None):
            return "%s(%s)" % (self.__class__.__name__, self.entity.name)
        else:
            return super(TableMapper, self).__str__(*args, **kwargs)

    def _mappings_join(self, ctx):

        pk = self.pk(ctx)
        ctype = pk["type"]
        if (ctype == "AutoIncrement"): ctype = "Integer"
        return [{
                  "entity": self.entity,
                  "name": self.entity.name,
                  "column": self.entity.name + "_id",
                  "type": ctype,
                  #"value": '${ m["' + self.entity.name + "_id" + '"] }'
                  "value": pk['value'] if (pk['value']) else '${ m["' + self.entity.name + "_id" + '"] }'
                 }]

    def _mappings(self, ctx):
        """
        Note: _ensure_mappings() shall be called only as the last
        step in the eval resolution chain, to avoid setting defaults
        before all consumers had an opportunity to override values.
        """

        #logger.debug("Calculating eval (TableMapper) for %s" % self)

        mappings = [mapping.copy() for mapping in self.mappings]
        return self._ensure_mappings(ctx, mappings)

    def _joins(self, ctx, master = None):
        """
        Joins related to this entity.
        """
        if (master != None):
            return [{
                  "master_entity": master,
                  "master_column": self.entity.name + "_id",
                  "detail_entity": self.entity,
                  "detail_column": (self.olapmapper.entity_mapper(self.entity.fact).pk(ctx)["column"]) if (hasattr(self.entity, "fact")) else self.pk(ctx)['column'],
                  }]
        else:
            return []

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


    def initialize(self, ctx):

        super(TableMapper, self).initialize(ctx)

        if self._uses_table:

            if (self.entity == None):
                raise Exception("No entity defined for %s" % self)
            if (self.connection == None):
                raise Exception("No connection defined for %s" % self)

            ctx.comp.initialize(self.entity)
            ctx.comp.initialize(self.connection)

            self._sqltable = CachedSQLTable()
            self._sqltable.name = self.table
            self._sqltable.connection = self.connection

            # Assert that the sqltable is clean
            #if (len(self._sqltable.columns) != 0): raise AssertionError("SQLTable '%s' columns shall be empty!" % self._sqltable.name)

        # If lookup_cols is a string, split by commas
        if (isinstance(self.lookup_cols, basestring)): self.lookup_cols = [ key.strip() for key in self.lookup_cols.split(",") ]

        Mappings.includes(ctx, self.mappings)
        for mapping in self.mappings:
            if (not "entity" in mapping):
                mapping["entity"] = self.entity


        if self._uses_table:

            mappings = self._mappings(ctx)
            for mapping in mappings:
                logger.debug("%s adding column from OLAP mapping: %s" % (self, mapping))
                self._sqltable.columns.append({ "name": mapping["column"] , "type": mapping["type"], "pk": mapping["pk"] })

            # If no key, use pk()
            if (self.lookup_cols == None):
                pk = self.pk(ctx)
                if ((pk == None) or (pk["type"] == "AutoIncrement")): raise Exception ("No lookup cols defined for %s " % self)
                self.lookup_cols = [ pk["name"] ]

            ctx.comp.initialize(self._sqltable)

    def finalize(self, ctx):
        if self._uses_table:
            ctx.comp.finalize(self._sqltable)
            ctx.comp.finalize(self.connection)
        ctx.comp.finalize(self.entity)
        super(TableMapper, self).finalize(ctx)

    def pk(self, ctx):
        #Returns the primary key mapping.

        pk_mappings = []
        for mapping in self._mappings(ctx):
            if ("pk" in mapping):
                if parsebool(mapping["pk"]):
                    pk_mappings.append(mapping)

        if (len(pk_mappings) > 1):
            raise Exception("%s has multiple primary keys mapped: %s" % (self, pk_mappings))
        elif (len(pk_mappings) == 1):
            return pk_mappings[0]
        else:
            return None

    def store(self, ctx, m):

        # Resolve evals
        Eval.process_evals(ctx, m, self.eval)

        # Store automatically or include dimensions
        if (self.auto_store != None):
            logger.debug("Storing automatically: %s" % (self.auto_store))
            for ast in self.auto_store:
                did = self.olapmapper.entity_mapper(ast).store(ctx, m)
                # TODO: Review and use PK properly
                if (did != None): m[ast.name + "_id"] = did
        elif (isinstance(self.entity, cubetl.olap.Fact)):
            logger.debug("Storing automatically: %s" % (self.entity.dimensions))
            for dim in self.entity.dimensions:
                did = self.olapmapper.entity_mapper(dim).store(ctx, m)
                # TODO: review this too, or use rarer prefix
                if (did != None): m[dim.name + "_id"] = did


        logger.debug("Storing entity in %s (mode: %s, lookup: %s)" % (self, self.store_mode, self.lookup_cols))

        data = {}
        mappings = self._mappings(ctx)

        # First try to look it up
        for mapping in mappings:
            if (mapping["column"] in self.lookup_cols):
                if (mapping["type"] != "AutoIncrement"):
                    if (mapping["value"] == None):
                        data[mapping["column"]] = m[mapping["name"]]
                    else:
                        data[mapping["column"]] = ctx.interpolate(m, mapping["value"])

        row = None
        if (self.store_mode == TableMapper.STORE_MODE_LOOKUP):
            row = self._sqltable.lookup(ctx, data)

        for mapping in mappings:
            if (mapping["type"] != "AutoIncrement"):
                if (mapping["value"] == None):
                    if (not mapping["name"] in m):
                        raise Exception("Field '%s' does not exist in message when assigning data for column %s in %s" % (mapping["name"], mapping["column"], self))
                    data[mapping["column"]] = m[mapping["name"]]
                else:
                    data[mapping["column"]] = ctx.interpolate(m, mapping["value"])


        if (not row):
            if (ctx.debug2): logger.debug("Storing data in %s (data: %s)" % (self, data))
            if (self.store_mode in [TableMapper.STORE_MODE_LOOKUP, TableMapper.STORE_MODE_INSERT]):
                row = self._sqltable.insert(ctx, data)
            else:
                raise Exception("Update store mode used at %s (%s) not implemented (available 'lookup', 'insert')" % (self, self.store_mode))
        else:
            # Check row is identical
            for mapping in self._mappings(ctx):
                if (mapping["type"] != "AutoIncrement"):
                    v1 = row[mapping['column']]
                    v2 = data[mapping['column']]
                    if (isinstance(v1, basestring) or isinstance(v2, basestring)):
                        if (not isinstance(v1, basestring)): v1 = str(v1)
                        if (not isinstance(v2, basestring)): v2 = str(v2)
                    if (v1 != v2):
                        if (mapping["column"] not in self._lookup_changed_fields):
                            logger.warn("%s looked up an entity which exists with different attributes (field=%s, existing_value=%s, tried_value=%s) (reported only once per field)" % (self, mapping["column"], v1, v2))
                            self._lookup_changed_fields.append(mapping["column"])

        return row[self.pk(ctx)["column"]]


class FactMapper(TableMapper):


    def _mappings(self, ctx):

        mappings = [mapping.copy() for mapping in self.mappings]
        for mapping in mappings:
            if (not "entity" in mapping):
                mapping["entity"] = self.entity

        for dimension in self.entity.dimensions:
            #if (not dimension.name in [mapping["name"] for mapping in self.mappings]):
            dimension_mapper = self.olapmapper.entity_mapper(dimension)
            dimension_mappings = dimension_mapper._mappings_join(ctx)

            # TODO: Check if entity/attribute is already mapped?
            self._extend_mappings(ctx, mappings, dimension_mappings)

        for measure in self.entity.measures:
            self._extend_mappings(ctx, mappings, [{
                                  "name": measure["name"] ,
                                  "type": measure["type"] if ("type" in measure  and  measure["type"] != None) else "Float",
                                  "entity": self.entity
                                  }])
        for attribute in self.entity.attributes:
            self._extend_mappings(ctx, mappings, [{
                                  "name": attribute["name"],
                                  "type": attribute["type"],
                                  "entity": self.entity
                                  }])

        self._ensure_mappings (ctx, mappings)
        return mappings

    def _joins(self, ctx, master = None):
        """
        Joins that can be done with this entity.
        """

        joins = super(FactMapper, self)._joins(ctx, master)
        for dim in self.entity.dimensions:
            dim_mapper = self.olapmapper.entity_mapper(dim)
            joins.extend(dim_mapper._joins(ctx, self.entity))

        return joins


class DimensionMapper(TableMapper):


    def _mappings(self, ctx):

        Mappings.includes(ctx, self.mappings)
        mappings = [mapping.copy() for mapping in self.mappings]
        for mapping in mappings:
            if (not "entity" in mapping):
                mapping["entity"] = self.entity

        for attribute in self.entity.attributes:

            # Add dimension attributes as fields for the mapper if not existing
            mapping = { "name": attribute["name"], "entity": self.entity }
            if ("type" in attribute and attribute["type"] != None):
                mapping["type"] = attribute["type"]
            self._extend_mappings(ctx, mappings, [ mapping ])

        self._ensure_mappings(ctx, mappings)
        return mappings


class CompoundDimensionMapper(TableMapper):

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
    """This maps all dimension levels on a CompoundDimensionMapper."""


    def initialize(self, ctx):

        if self.dimensions == []:
            self.dimensions = []
        if self._created_mappers == []:
            self._created_mappers = []

        if (len(self.dimensions) != 0):
            raise Exception("Cannot define dimensions in %s. Only one HierarchyDimension can be set as entity." % (self))

        for level in self.entity.levels:
            self.dimensions.append(level)

        super(CompoundHierarchyDimensionMapper, self).initialize(ctx)


class MultiTableHierarchyDimensionMapper(TableMapper):


    def initialize(self, ctx):

        if (self.table):
            raise Exception("Cannot define table in %s. All dimensions of a MultiTableHierarchyDimensionMapper must be mapped manually." % self)
        if (self.connection):
            raise Exception("Cannot define table in %s. All dimensions of a MultiTableHierarchyDimensionMapper must be mapped manually." % self)

        # Do not call parent.

    def finalize(self, ctx):
        # Do not call parent.
        pass

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
        raise Exception("Cannot store on %s. Stores should be done on each related dimension as appropriate." % (self))


class EmbeddedDimensionMapper(DimensionMapper):

    key = ""
    remove_pk = True

    _back_mapper = None

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


    def _mappings_join(self, ctx):

        mappings = self._mappings(ctx)

        if self._back_mapper:
            back_mappings = self._back_mapper._mappings(ctx)
            for mapping in back_mappings:
                mapping["entity"] = self.entity

                self._extend_mappings(ctx, mappings, back_mappings)

        # Optionally remove any possible primary key
        if (self.remove_pk):
            mappings = [m for m in mappings if m['pk'] == False]

        return mappings

    def _joins(self, ctx, master):

        return []

    def pk(self, ctx):
        # Check no PK in initialize?
        #raise Exception("Method pk() not implemented for %s" % self)
        return None

    def store(self, ctx, m):
        # Evaluate evals, but don't store anything
        Eval.process_evals(ctx, m, self.eval)



class FactDimensionMapper(FactMapper):


    def initialize(self, ctx):

        if (not self.entity):
            raise Exception("No entity defined for %s" % self)

        if (self.table):
            raise Exception("Cannot define table in %s." % self)
        if (self.connection):
            raise Exception("Cannot define connection in %s." % self)


        # No call to constructor. No need for connection and table
        ctx.comp.initialize(self.entity)

        # Check no PK in initialize?
        self.table = self.olapmapper.entity_mapper(self.entity.fact).table

        # If lookup_cols is a string, split by commas
        if (self.lookup_cols != None):
            raise Exception("No lookup_cols can be defined for an embedded dimension.")

    def finalize(self, ctx):
        ctx.comp.finalize(self.entity)
        # Do no call super (no table or connection)?


    def store(self, ctx, data):
        # TODO: This shall not even be called, and raise an exception instead?
        #raise Exception ("Cannot store an embedded dimension")
        pass



