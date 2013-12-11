import logging
from cubetl.olap import Dimension, Fact, DimensionMapper, FactMapper
from cubetl.sql.cache import CachedSQLTable
from cubetl.functions.text import parsebool
from cubetl.sql import SQLTable

# Get an instance of a logger
logger = logging.getLogger(__name__)

    
class SQLDimensionMapper(DimensionMapper):
    
    def __init__(self):
        
        super(SQLDimensionMapper, self).__init__()
        
        self.dimension = None
        
        self.table = None
        self.connection = None
        
        self.mappings = []
        
        self._sqltable = None
        
        self.lookup_cols = None 
    
    def finalize(self, ctx):
        
        ctx.comp.finalize(self.dimension)
        ctx.comp.finalize(self._sqltable)
        
        super(SQLDimensionMapper, self).finalize(ctx)
    
    def initialize(self, ctx):
        
        super(SQLDimensionMapper, self).initialize(ctx)
        
        ctx.comp.initialize(self.dimension)

        self._sqltable = CachedSQLTable()
        self._sqltable.name = self.table
        self._sqltable.connection = self.connection

        print self
        print self.mappings

        for attribute in self.dimension.attributes:
            if (not attribute["name"] in [mapping["name"] for mapping in self.mappings]):
                mapping = { "name": attribute["name"] }
                if ("type" in attribute): mapping["type"] = attribute["type"]
                self.mappings.append(mapping)

        for mapping in self.mappings:
            mapping["pk"] = False if (not "pk" in mapping) else parsebool(mapping["pk"])  
            if (not "column" in mapping): mapping["column"] = mapping["name"]
            if (not "value" in mapping): mapping["value"] = None 
            if (not "type" in mapping): 
                # Infer type
                if (self.dimension.has_attribute(mapping["name"])):
                    mapping["type"] = self.dimension.attribute(mapping["name"])["type"]
                else:
                    if (mapping["pk"]):
                        mapping["type"] = "AutoIncrement"

            # Add to underlying table
            self._sqltable.columns.append({ "name": mapping["column"] , "type": mapping["type"], "pk": mapping["pk"] })
        
        
        # Get pk:
        pk = self.pk(ctx)
        if (pk == None):
            raise Exception ("No pk (primary key) defined for %s " % self)
        
        # If key is a string, split by commas
        if (isinstance(self.lookup_cols, basestring)): self.lookup_cols = [ key.strip() for key in self.lookup_cols.split(",") ]
        # If no key, use pk()
        if (self.lookup_cols == None):
            self.lookup_cols = [ pk["name"] ]
            
        ctx.comp.initialize(self._sqltable)
            
    def pk(self, ctx):
        """
        Returns the primary key mapping.
        """

        pk_mappings = []
        for mapping in self.mappings:
            if ("pk" in mapping):
                if parsebool(mapping["pk"]):
                    pk_mappings.append(mapping)
                        
        if (len(pk_mappings) > 1):
            raise Exception("Dimension %s has multiple primary keys mapped: %s" % (self.name, pk_mappings))
        elif (len(pk_mappings) == 1):
            return pk_mappings[0]
        else:
            return None

    def store(self, ctx, m):
        
        logger.debug("Storing dimension %s" % self)
        
        data = {}
        row = {}
        
        # First try to look it up
        for mapping in self.mappings:
            if (mapping["column"] in self.lookup_cols):
                if (mapping["type"] != "AutoIncrement"):
                    if (mapping["value"] == None):
                        data[mapping["column"]] = m[mapping["name"]]
                    else:
                        data[mapping["column"]] = ctx.interpolate(m, mapping["value"])
        
        row = self._sqltable.lookup(ctx, data)
        
        if (not row):
            for mapping in self.mappings:
                if (not mapping["column"] in self.lookup_cols):
                    if (mapping["type"] != "AutoIncrement"):
                        if (mapping["value"] == None):
                            data[mapping["column"]] = m[mapping["name"]]
                        else:
                            data[mapping["column"]] = ctx.interpolate(m, mapping["value"])

            row = self._sqltable.insert(ctx, data)
    
        logger.debug("%s stored data: %s (lookup: %s)" % (self, row, self.lookup_cols)) 
            
        if (not self.pk(ctx)["name"] in row):
            raise Exception("No primary key set when storing dimension entry: %s" % data)

        return row[self.pk(ctx)["name"]]
        
class SQLEmbeddedDimensionMapper(DimensionMapper):
    
    def __init__(self):

        super(SQLEmbeddedDimensionMapper, self).__init__()
        
        self.dimension = None
        self.mappings = []

    def finalize(self, ctx):
        ctx.comp.finalize(self.dimension)
        super(SQLEmbeddedDimensionMapper, self).finalize(ctx)
    
    def initialize(self, ctx):
        
        super(SQLEmbeddedDimensionMapper, self).initialize(ctx)
        
        ctx.comp.initialize(self.dimension)
        
        # Check there's at least one "relative" PK (either the data or a separate column). 
        # PKs shall not be transferred to parent table, however.
        for mapping in self.mappings:
            if (not "value" in mapping): mapping["value"] = None 
            if (not "type" in mapping): 
                # Infer type
                mapping["type"] = self.dimension.attribute(mapping["name"])["type"]
        
                 
    def pk(self, ctx):
        raise Exception("Method pk() not implemented for %s" % self)
    
    def store(self, ctx, data):
        # TODO: This shall not even be called, and raise an exception instead.
        # Is misleading: these dimensions don't store anything themselves
        return

    
class SQLFactMapper(FactMapper):
    
    def __init__(self):
        
        super(SQLFactMapper, self).__init__()
    
        self.fact = None

        self.connection = None
        self.table = None
        
        self.mappings = []
        
        self.lookup_cols = None
        
        self._sqltable = None
        
    def finalize(self, ctx):
        ctx.comp.finalize(self._sqltable)
        ctx.comp.finalize(self.connection)
        ctx.comp.finalize(self.fact)
        super(SQLFactMapper, self).finalize(ctx)
        
    def initialize(self, ctx):
        
        super(SQLFactMapper, self).initialize(ctx)
        
        ctx.comp.initialize(self.fact)
        ctx.comp.initialize(self.connection)
        
        self._sqltable = CachedSQLTable()
        self._sqltable.name = self.table
        self._sqltable.connection = self.connection
        
        for dimension in self.fact.dimensions:
            if (not dimension.name in [mapping["name"] for mapping in self.mappings]):
                dimmapper = self.olapmapper.getDimensionMapper(dimension)
                if (isinstance(dimmapper, SQLEmbeddedDimensionMapper)):
                    ctx.comp.initialize(dimmapper)
                    self.mappings.extend(dimmapper.mappings) 
                else:
                    ctype = dimmapper.pk(ctx)["type"]
                    if (ctype == "AutoIncrement"): ctype = "Integer"
                    self.mappings.append({
                                          "name": dimension.name,
                                          "column": dimension.name + "_id",
                                          "value": '${ m["' + dimension.name + "_id" + '"] }',
                                          "type": ctype
                                          })

        for measure in self.fact.measures:
            if (not measure["name"] in [mapping["name"] for mapping in self.mappings]):
                self.mappings.append({ 
                                      "name": measure["name"] , 
                                      "type": measure["type"] if ("type" in measure) else "Float"
                                      })
        for attribute in self.fact.attributes:
            if (not attribute["name"] in [mapping["name"] for mapping in self.mappings]):
                self.mappings.append({
                                      "name": attribute["name"], 
                                      "type": attribute["type"]
                                      })
        
        
        for mapping in self.mappings:
            if (not "column" in mapping): mapping["column"] = mapping["name"]
            if (not "value" in mapping): mapping["value"] = None 
            if (not "type" in mapping): mapping["type"] = "String"
            mapping["pk"] = False if (not "pk" in mapping) else parsebool(mapping["pk"])  
                 
            self._sqltable.columns.append({ "name": mapping["column"] , "type": mapping["type"], "pk": mapping["pk"] })
            
        # Get pk:
        pk = self.pk(ctx)
        if (pk == None): raise Exception ("No pk (primary key) defined for %s " % self)
        
        # If key is a string, split by commas
        if (isinstance(self.lookup_cols, basestring)): self.lookup_cols = [ key.strip() for key in self.lookup_cols.split(",") ]
        
        # If no key, use pk()
        if (self.lookup_cols == None):
            self.lookup_cols = [ pk["name"] ]
            
        ctx.comp.initialize(self._sqltable)            
            
    def pk(self, ctx):
        """
        Returns the primary key mapping.
        """

        pk_mappings = []
        for mapping in self.mappings:
            if ("pk" in mapping):
                if parsebool(mapping["pk"]):
                    pk_mappings.append(mapping)
                        
        if (len(pk_mappings) > 1):
            raise Exception("Fact %s has multiple primary keys mapped: %s" % (self.name, pk_mappings))
        elif (len(pk_mappings) == 1):
            return pk_mappings[0]
        else:
            return None
    
    def store(self, ctx, m):

        logger.debug("Storing fact in %s (lookup: %s)" % (self, self.lookup_cols))
        
        data = {}
        row = {}

        # First try to look it up
        for mapping in self.mappings:
            if (mapping["column"] in self.lookup_cols):
                if (mapping["type"] != "AutoIncrement"):
                    if (mapping["value"] == None):
                        data[mapping["column"]] = m[mapping["name"]]
                    else:
                        data[mapping["column"]] = ctx.interpolate(m, mapping["value"])
        
        row = self._sqltable.lookup(ctx, data) 
        
        if (not row):
            for mapping in self.mappings:
    
                if (mapping["type"] != "AutoIncrement"):
                    if (mapping["value"] == None):
                        if (not mapping["name"] in m):
                            raise Exception("Field '%s' does not exist in message when assigning Fact data for column %s in %s" % (mapping["name"], mapping["column"], self))
                        data[mapping["column"]] = m[mapping["name"]]
                    else:
                        data[mapping["column"]] = ctx.interpolate(m, mapping["value"])

            row = self._sqltable.insert(ctx, data)
        
        return row[self.pk(ctx)["column"]]
        
        

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
        
