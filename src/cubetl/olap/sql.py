import logging
from lxml import etree
from cubetl.olap import Dimension, Fact, DimensionMapper, FactMapper
from cubetl.sql.cache import CachingSQLTable
from cubetl.functions.text import parsebool

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
    
    def signal(self, ctx, s):
        
        super(SQLDimensionMapper, self).signal(ctx, s)
        
        self.dimension.signal(ctx, s)
        
        # If finalizing, close transaction
        if (s == "initialize"):
            self._initialize(ctx)
        
        # Signal table last
        self._sqltable.signal(ctx, s)
    
    def _initialize(self, ctx):
        
        if (self._sqltable): return

        self._sqltable = CachingSQLTable()
        self._sqltable.name = self.table
        self._sqltable.connection = self.connection
        
        for attribute in self.dimension.attributes:
            if (not attribute["name"] in [mapping["name"] for mapping in self.mappings]):
                mapping = { "name": attribute["name"] }
                if ("type" in attribute): mapping["type"] = attribute["type"]
                self.mappings.append(mapping)

        for detail in self.dimension.details:
            if (not detail["name"] in [mapping["name"] for mapping in self.mappings]):
                mapping = { "name": detail["name"] }
                if ("type" in detail): mapping["type"] = detail["type"]
                self.mappings.append(mapping)
        
        for mapping in self.mappings:
            mapping["pk"] = False if (not "pk" in mapping) else parsebool(mapping["pk"])  
            if (not "column" in mapping): mapping["column"] = mapping["name"]
            if (not "value" in mapping): mapping["value"] = None 
            if (not "type" in mapping): 
                # Infer type
                if (mapping["pk"]):
                    mapping["type"] = "AutoIncrement"
                else:
                    mapping["type"] = self.dimension.attribute(mapping["name"])["type"]

            # Add to underlying table
            self._sqltable.columns.append({ "name": mapping["column"] , "type": mapping["type"], "pk": mapping["pk"] })
            
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
        
        data = {}
        keys = []
        for mapping in self.mappings:
            
            if (mapping["name"] in [attr["name"] for attr in self.dimension.attributes]):
                keys.append (mapping["name"])
                
            if (mapping["type"] != "AutoIncrement"):
                if (mapping["value"] == None):
                    data[mapping["column"]] = m[mapping["name"]]
                else:
                    data[mapping["column"]] = ctx.interpolate(m, mapping["value"])

        row = self._sqltable.store(ctx, data, keys)
        if (not self.pk(ctx)["name"] in row):
            raise Exception("No primary key set when storing dimension entry: %s" % data)

        return row[self.pk(ctx)["name"]]
        
class SQLEmbeddedDimensionMapper(SQLDimensionMapper):
    
    def __init__(self):

        super(SQLEmbeddedDimensionMapper, self).__init__()
        
        self.dimension = None
        self.mappings = []

    def signal(self, ctx, s):
        
        self.dimension.signal(ctx, s)
        
        # If finalizing, close transaction
        if (s == "initialize"):
            self.initialize(ctx)
        
    def initialize(self, ctx):
        
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
        
        self._sqltable = None
        
    def signal(self, ctx, s):
        
        self.fact.signal(ctx, s)
        self.connection.signal(ctx, s)
        
        # If finalizing, close transaction
        if (s == "initialize"):
            self._initialize(ctx)
            
        self._sqltable.signal(ctx, s)        
        
    def _initialize(self, ctx):
        
        if (self._sqltable): return
        
        self._sqltable = CachingSQLTable()
        self._sqltable.name = self.table
        self._sqltable.connection = self.connection
        
        for dimension in self.fact.dimensions:
            if (not dimension.name in [mapping["name"] for mapping in self.mappings]):
                dimmapper = self.olapmapper.getDimensionMapper(dimension)
                if (isinstance(dimmapper, SQLEmbeddedDimensionMapper)):
                    dimmapper.initialize(ctx)
                    self.mappings.extend(dimmapper.mappings) 
                else:
                    self.mappings.append({
                                          "name": dimension.name,
                                          "column": dimension.name + "_id",
                                          "value": '${ m["' + dimension.name + "_id" + '"] }',
                                          "type": dimmapper.pk(ctx)["type"]
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
                                      "type": "String"
                                      })
        
        
        for mapping in self.mappings:
            if (not "column" in mapping): mapping["column"] = mapping["name"]
            if (not "value" in mapping): mapping["value"] = None 
            if (not "type" in mapping): mapping["type"] = "String"
            mapping["pk"] = False if (not "pk" in mapping) else parsebool(mapping["pk"])  
                 
            self._sqltable.columns.append({ "name": mapping["column"] , "type": mapping["type"], "pk": mapping["pk"] })            
            
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
        
        # Insert or update data
        row = {}
        for mapping in self.mappings:

            if (mapping["value"] == None):
                row[mapping["column"]] = m[mapping["name"]]
            else:
                row[mapping["column"]] = ctx.interpolate(m, mapping["value"])

        row = self._sqltable.store(ctx, row)
        return row[self.pk(ctx)["name"]]
        
        

class SQLFactDimensionMapper(SQLEmbeddedDimensionMapper):
    
    def __init__(self):

        super(SQLFactDimensionMapper, self).__init__()
        
        self.dimension = None
        self.mappings = []

    def signal(self, ctx, s):
        
        self.dimension.signal(ctx, s)
        self.olapmapper.getFactMapper(self.dimension.fact).signal(ctx, s)
        
        if (s == "initialize"):
            self._initialize(ctx)
            
        
    def _initialize(self, ctx):
        
        if (len(self.mappings) != 1):
            raise Exception("SQLFactDimensionMapper must contain a single mapping for the Fact-to-Fact relation.")
        
        for mapping in self.mappings:
            if "name" in mapping:
                if (mapping["name"] != self.dimension.fact.name):
                    logger.warn("Attribute 'name' for the mapping must exist and match the associated fact name: " + self.dimension.fact.name)
            
            mapping["name"] = self.dimension.fact.name
            mapping["pk"] = False

            if (not "column" in mapping): mapping["column"] = self.dimension.fact.name + "_id"
            if (not "value" in mapping): mapping["value"] = None 
            if (not "type" in mapping): 
                # Infer type
                mapping["type"] = self.olapmapper.getFactMapper(self.dimension.fact).pk(ctx)["type"]
                 
    def pk(self, ctx):
        raise Exception("Method pk() not implemented for %s" % self)
    
    def store(self, ctx, data):
        
        mapping = self.mappings[0]
        
        if (mapping["value"] == None):
            return data[mapping["column"]]
        else:
            return ctx.interpolate(data, mapping["value"])
        
