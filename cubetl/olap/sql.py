import cubetl
from abc import ABCMeta, abstractmethod
from cubetl.core import Component, Node
from cubetl.functions.text import parsebool
from cubetl.sql import SQLTable
from cubetl.sql.cache import CachedSQLTable
import logging
from yaml.events import MappingStartEvent

# Get an instance of a logger
logger = logging.getLogger(__name__)

# Abstract class
class TableMapper(Component):
    
    __metaclass__ = ABCMeta
    
    def __init__(self):
        
        super(TableMapper, self).__init__()
    
        self.entity = None

        self.connection = None
        self.table = None
        
        self.mappings = []
        
        self.lookup_cols = None
        
        self.auto_store = None
        
        self._sqltable = None    

    def __str__(self, *args, **kwargs):
        
        return "%s(%s)" % (self.__class__.__name__, self.entity.name)
    
    def _mappings_join(self, ctx):
        
        pk = self.pk(ctx)
        ctype = pk["type"]
        if (ctype == "AutoIncrement"): ctype = "Integer"
        return [{
                  "entity": self.entity.name,
                  "name": self.entity.name,
                  "column": self.entity.name + "_id",
                  "value": pk['value'], # '${ m["' + self.entity.name + "_id" + '"] }',
                  "type": ctype 
                 }]
    
    def _mappings(self, ctx):
        
        mappings = [mapping.copy() for mapping in self.mappings]
        self._ensure_mappings (ctx, mappings)
        return mappings
    
    def _extend_mappings(self, ctx, mappings, newmappings):

        for nm in newmappings:
            found = False
            for m in mappings:
                if (not "entity" in m):
                    raise Exception("No entity defined for mapping %s" % m)
                if (not "entity" in nm):
                    raise Exception("No entity defined for mapping %s" % nm)
                if (m["name"] == nm["name"] and m["entity"] == nm["entity"]):
                    found = True
                    break
            
            if not found:
                mappings.append(nm)        
    
    def _ensure_mappings(self, ctx, mappings):
        
        for mapping in mappings:
            mapping["pk"] = (False if (not "pk" in mapping) else parsebool(mapping["pk"]))  
            if (not "entity" in mapping): mapping["entity"] = self.entity.name  
            if (not "column" in mapping): mapping["column"] = mapping["name"]
            if (not "value" in mapping): mapping["value"] = None
            if (not "type" in mapping): mapping["type"] = "String"
                 
    def initialize(self, ctx):
        
        super(TableMapper, self).initialize(ctx)
        
        ctx.comp.initialize(self.entity)
        ctx.comp.initialize(self.connection)
        
        self._sqltable = CachedSQLTable()
        self._sqltable.name = self.table
        self._sqltable.connection = self.connection    

        for mapping in self._mappings(ctx):
            self._sqltable.columns.append({ "name": mapping["column"] , "type": mapping["type"], "pk": mapping["pk"] })
            
        # If lookup_cols is a string, split by commas
        if (isinstance(self.lookup_cols, basestring)): self.lookup_cols = [ key.strip() for key in self.lookup_cols.split(",") ]

        # If no key, use pk()
        if (self.lookup_cols == None):
            pk = self.pk(ctx)
            if (pk == None): raise Exception ("No pk (primary key) or lookup keys defined for %s " % self)
            self.lookup_cols = [ pk["name"] ]
            
        ctx.comp.initialize(self._sqltable)     
    
    def finalize(self, ctx):
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
            raise Exception("Entity %s has multiple primary keys mapped: %s" % (self.name, pk_mappings))
        elif (len(pk_mappings) == 1):
            return pk_mappings[0]
        else:
            return None    
    
    def store(self, ctx, m):

        # Store automatically or include dimensions
        if (self.auto_store != None):
            logger.debug("Storing automatically: %s" % (self.auto_store))
            for ast in self.auto_store:
                did = self.olapmapper.getEntityMapper(ast).store(ctx, m)
                # TODO: Review and use PK properly
                if (did != None): m[ast.name + "_id"] = did
        elif (isinstance(self.entity, cubetl.olap.Fact)):
            logger.debug("Storing automatically: %s" % (self.entity.dimensions))
            for dim in self.entity.dimensions:
                did = self.olapmapper.getEntityMapper(dim).store(ctx, m)
                # TODO: review this too, or use rarer prefix
                if (did != None): m[dim.name + "_id"] = did

        
        logger.debug("Storing entity in %s (lookup: %s)" % (self, self.lookup_cols))
        
        data = {}

        # First try to look it up
        for mapping in self._mappings(ctx):
            if (mapping["column"] in self.lookup_cols):
                if (mapping["type"] != "AutoIncrement"):
                    if (mapping["value"] == None):
                        data[mapping["column"]] = m[mapping["name"]]
                    else:
                        data[mapping["column"]] = ctx.interpolate(m, mapping["value"])
        
        row = self._sqltable.lookup(ctx, data) 
        
        for mapping in self._mappings(ctx):
            if (mapping["type"] != "AutoIncrement"):
                if (mapping["value"] == None):
                    if (not mapping["name"] in m):
                        raise Exception("Field '%s' does not exist in message when assigning data for column %s in %s" % (mapping["name"], mapping["column"], self))
                    data[mapping["column"]] = m[mapping["name"]]
                else:
                    data[mapping["column"]] = ctx.interpolate(m, mapping["value"])

        if (not row):
            row = self._sqltable.insert(ctx, data)
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
                        logger.warn("%s looked up an entity which exists with different attributes (field=%s, existing_value=%s, tried_value=%s)" % (self, mapping["column"], v1, v2))
        
        return row[self.pk(ctx)["column"]]    
    
    
    def attributes(self, ctx):
        
        pass
    
    
class FactMapper(TableMapper):
    
    def __init__(self):
        
        super(FactMapper, self).__init__()
            
    def _mappings(self, ctx):
        
        mappings = [mapping.copy() for mapping in self.mappings]
        for mapping in mappings:
            if (not "entity" in mapping): mapping["entity"] = self.entity.name
        
        for dimension in self.entity.dimensions:
            #if (not dimension.name in [mapping["name"] for mapping in self.mappings]):
            dimension_mapper = self.olapmapper.getEntityMapper(dimension)
            dimension_mappings = dimension_mapper._mappings_join(ctx)
            
            # TODO: Check if entity/attribute is already mapped?
            self._extend_mappings(ctx, mappings, dimension_mappings)
            
        for measure in self.entity.measures:
            self._extend_mappings(ctx, mappings, [{ 
                                  "name": measure["name"] , 
                                  "type": measure["type"] if ("type" in measure) else "Float",
                                  "entity": self.entity.name
                                  }])
        for attribute in self.entity.attributes:
            self._extend_mappings(ctx, mappings, [{
                                  "name": attribute["name"], 
                                  "type": attribute["type"],
                                  "entity": self.entity.name
                                  }])
        
        self._ensure_mappings (ctx, mappings)
        return mappings

class DimensionMapper(TableMapper):
    
    def __init__(self):
        
        super(DimensionMapper, self).__init__()
        
    def _mappings(self, ctx):
        
        mappings = [mapping.copy() for mapping in self.mappings]
        for mapping in mappings:
            if (not "entity" in mapping): mapping["entity"] = self.entity.name
        for attribute in self.entity.attributes:
            mapping = { "name": attribute["name"], "entity": self.entity.name }
            if ("type" in attribute): mapping["type"] = attribute["type"]
            self._extend_mappings(ctx, mappings, [ mapping ])
        
        self._ensure_mappings (ctx, mappings)
        return mappings    

class CompoundDimensionMapper(TableMapper):
    
    def __init__(self):
        
        super(CompoundDimensionMapper, self).__init__()
        
        self.dimensions = []
        
        self._created_mappers = []

    def finalize(self, ctx):
        
        for cm in self._created_mappers:
            ctx.comp.finalize(cm)
        super(CompoundDimensionMapper, self).finalize(ctx)    
    
    def _mappings(self, ctx):
        
        if (len(self.dimensions) == 0):
            raise Exception("No dimensions found in %s" % self)
        
        mappings = super(CompoundDimensionMapper, self)._mappings(ctx)
        
        for dimension in self.dimensions:
            dimension_mapper = self.olapmapper.getEntityMapper(dimension, False)
            
            if (dimension_mapper == None):
                # Create dimension mapper
                logger.debug("No mapper found for %s in %s, creating a default EmbeddeDimension mapper for it." % (dimension, self))
                dimension_mapper = EmbeddedDimensionMapper()
                dimension_mapper.entity = dimension
                self.olapmapper.mappers.append(dimension_mapper)
                self._created_mappers.append(dimension_mapper)
                ctx.comp.initialize(dimension_mapper)
            
            dimension_mappings = dimension_mapper._mappings_join(ctx)
            #for dm in dimension_mappings:
            #    dm["pk"] = False
            self._extend_mappings(ctx, mappings, dimension_mappings)
        
        return mappings    

class CompoundHierarchyDimensionMapper(CompoundDimensionMapper):
    """This maps all dimension levels on a CompoundDimensionMapper.""" 
    
    def __init__(self):
        
        super(CompoundHierarchyDimensionMapper, self).__init__()

    def initialize(self, ctx):
        
        if (len(self.dimensions) != 0):
            raise Exception("Cannot define dimensions in %s. Only one HierarchyDimension can be set as entity." % self)
        
        for level in self.entity.levels:
            self.dimensions.append(level)
        
        super(CompoundHierarchyDimensionMapper, self).initialize(ctx)
        

class MultiTableHierarchyDimensionMapper(TableMapper):
    
    def __init__(self):
        
        super(MultiTableHierarchyDimensionMapper, self).__init__()
        
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
            dimension_mapper = self.olapmapper.getEntityMapper(dimension, False)
            self._extend_mappings(ctx, mappings, dimension_mapper._mappings_join(ctx))
            
        return mappings
    
    def _mappings(self, ctx):
        
        raise Exception("Cannot provide mappings for %s. No table is related to this kind of mapper." % (self))
        
    def store(self, ctx, m):
        raise Exception("Cannot store on %s. Stores should be done on each related dimension as appropriate." % (self))
    

class EmbeddedDimensionMapper(DimensionMapper):
    
    def __init__(self):

        super(EmbeddedDimensionMapper, self).__init__()

    def finalize(self, ctx):
        ctx.comp.finalize(self.entity)

    def initialize(self, ctx):

        # No call to constructor. No need for connection and table 
        ctx.comp.initialize(self.entity)
        
        # Check no PK in initialize?
        
        # If lookup_cols is a string, split by commas
        if (self.lookup_cols != None):
            raise Exception("No lookup_cols can be defined for an embedded dimension.")

    def _mappings_join(self, ctx):
        
        return self._mappings(ctx)
        
    def pk(self, ctx):
        # Check no PK in initialize?
        raise Exception("Method pk() not implemented for %s" % self)
    
    def store(self, ctx, data):
        # TODO: This shall not even be called, and raise an exception instead?
        #raise Exception ("Cannot store an embedded dimension") 
        pass



        
"""
class SQLFactDimensionMapper(SQLEmbeddedDimensionMapper):
    
    def __init__(self):
        
        super(SQLFactDimensionMapper, self).__init__()
        
        self.dimension = None
        self.mappings = []

    def finalize(self, ctx):
        ctx.comp.finalize(self.olapmapper.getFactMapper(self.dimension.fact))
        ctx.comp.finalize(self.dimension)
        super(SQLFactDimensionMapper, self).finalize(ctx)
        
    def initialize(self, ctx):
        
        ctx.comp.initialize(self.dimension)
        ctx.comp.initialize(self.olapmapper.getFactMapper(self.dimension.fact))
        
        if (len(self.mappings) != 1):
            raise Exception("SQLFactDimensionMapper must contain a single mapping for the Fact-to-Fact relation.")
        
        for mapping in self.mappings:
            if (not (("name" in mapping) and (mapping["name"] == self.dimension.fact.name))):
                logger.warn("Attribute 'name' for the mapping must exist and match the associated fact name: " + self.dimension.fact.name)
            
            mapping["name"] = self.dimension.fact.name
            mapping["pk"] = False

            if (not "column" in mapping): mapping["column"] = self.dimension.fact.name + "_id"
            if (not "value" in mapping): mapping["value"] = None 
            if (not "type" in mapping): 
                # Infer type
                mapping["type"] = self.olapmapper.getFactMapper(self.dimension.fact).pk(ctx)["type"]
                
        super(SQLFactDimensionMapper, self).initialize(ctx)
                 
    def pk(self, ctx):
        raise Exception("Method pk() not implemented for %s" % self)
    
    def store(self, ctx, data):
        
        mapping = self.mappings[0]
        
        if (mapping["value"] == None):
            return data[mapping["column"]]
        else:
            return ctx.interpolate(data, mapping["value"])
        
"""


