import logging
from lxml import etree
from cubetl.core import Node, Component

# Get an instance of a logger
logger = logging.getLogger(__name__)

class Dimension(Component):
    
    def __init__(self):
        
        self.name = None
        self.label = None
        self.attributes = []
        self.details = []
        
    def signal(self, ctx, s):
        
        if (s == "initialize"):
            self._initialize(ctx)        

    def _initialize(self, ctx):
        if (self.label == None): self.label = self.name
        for attr in self.attributes:
            if (not "label" in attr):
                if ((len(self.attributes) == 1) and (attr["name"] == self.name)):
                    attr["label"] = self.label
                else:
                    attr["label"] = attr["name"]

    def attribute(self, search):
        att = [attr for attr in self.attributes if attr["name"] == search]
        if (len(att) != 1):
            raise Exception("Could not find attribute %s in dimension %s" % (search, self.name))
        
        return att[0]
        

class Fact(Component):
    
    def __init__(self):
        
        self.name = None
        self.label = None
        
        self.dimensions = []
        self.attributes = []
        self.measures = []
        
    def signal(self, ctx, s):
        
        if (s == "initialize"):
            self._initialize(ctx)        

    def _initialize(self, ctx):
        if (self.label == None): self.label = self.name
        for attr in self.attributes:
            if (not "label" in attr):
                attr["label"] = attr["name"]        


class FactDimension(Dimension):
    
    def __init__(self):
        
        super(FactDimension, self).__init__()
        
        self.fact = None
        
    def signal(self, ctx, s):
        
        # Propagate signal to mappers and delegated mappers
        self.fact.signal(ctx, s)
        
        if (s == "initialize"):
            if (len(self.attributes) > 0):
                raise Exception("Cannot define attributes for a FactDimension (it's defined by the linked fact)")
            if (len(self.details) > 0):
                raise Exception("Cannot define details for a FactDimension (it's defined by the linked fact)")

    def attribute(self, search):
        att = [attr for attr in self.attributes if attr["name"] == search]
        if (len(att) != 1):
            raise Exception("Could not find attribute %s in dimension %s" % (search, self.name))
        
        return att[0]

class DimensionMapper(Component):
    
    def __init__(self):
        self.olapmapper = None

class FactMapper(Component):
    
    def __init__(self):
        self.olapmapper = None

class OlapMapper(Component):
    
    def __init__(self):
        self.mappings = []
        self.include = []

    def signal(self, ctx, s):
        
        if (s == "initialize"):
            for mapper in self.mappings:
                mapper.olapmapper = self

        for incl in self.include:
            incl.signal(ctx, s)

        # Propagate signal to mappers and delegated mappers
        for mapper in self.mappings:
            mapper.signal(ctx, s)

    def getDimensionMapper(self, dim, fail = True):
        for mapper in self.mappings:
            if (isinstance(mapper, DimensionMapper)):
                if (mapper.dimension.name == dim.name):
                    return mapper
        
        for inc in self.include:
            mapper = inc.getDimensionMapper(dim, False)
            if (mapper): return mapper

        if fail:
            raise Exception("No OLAP mapper found for dimension: %s" % dim.name)
        
        return None
        
    def getFactMapper(self, fact, fail = True):
        for mapper in self.mappings:
            if (isinstance(mapper, FactMapper)):
                if (mapper.fact.name == fact.name):
                    return mapper
                
        for inc in self.include:
            mapper = inc.getFactMapper(fact, False)
            if (mapper): return mapper

        if fail:
            raise Exception("No OLAP mapper found for fact: %s" % fact.name)

        return None

class StoreFact(Node):
    
    def signal(self, ctx, s):

        # Propagate signal to mappers and delegated mappers
        self.mapper.signal(ctx, s)
    
    def process(self, ctx, m):
        
        logger.debug ("Storing fact %s" % (self.fact.name))
        
        # Store dimensions
        # TODO: Shall be optional
        for dim in self.fact.dimensions:
            did = self.mapper.getDimensionMapper(dim).store(ctx, m)
            # TODO: review this too, or use rarer prefix
            if (did != None): m[dim.name + "_id"] = did
        
        # Store
        fid = self.mapper.getFactMapper(self.fact).store(ctx, m)
        if (fid != None): m[self.fact.name + "_id"] = fid
        
        
        yield m
