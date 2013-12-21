import logging
from lxml import etree
from cubetl.core import Node, Component
from cubetl.olap.sql import FactMapper

# Get an instance of a logger
logger = logging.getLogger(__name__)

class Dimension(Component):
    """A flat dimension.

    Note: This represents a Flat dimension (no hierarchies, only one level of attributes). 
    """
    
    def __init__(self):
        
        super(Dimension, self).__init__()
        
        self.name = None
        self.label = None
        self.attributes = []
        self.role = None
        
    def initialize(self, ctx):
        logger.debug("Initializing %s" % self.name)
        super(Dimension, self).initialize(ctx)
        
        if (self.label == None): self.label = self.name
        for attr in self.attributes:
            if (not "label" in attr):
                if ((len(self.attributes) == 1) and (attr["name"] == self.name)):
                    attr["label"] = self.label
                else:
                    attr["label"] = attr["name"]
        
    def has_attribute(self, search):
        return (search in [attr["name"] for attr in self.attributes])
    
    def attribute(self, search):
        att = [attr for attr in self.attributes if attr["name"] == search]
        
        if (len(att) > 1):
            raise Exception("More than one attribute with name '%s' found in dimension %s" % (search, self.name))
        if (len(att) == 0):
            raise Exception("Could not find attribute '%s' in dimension %s" % (search, self.name))
        
        return att[0]
       
class HierarchyDimension(Dimension):
    """A non-flat dimension, forming one or more hierarchies.
    
    References subdimensions (levels), usually forming hierarchies.
    """
    
    def __init__(self):
        
        super(HierarchyDimension, self).__init__()
        
        self.levels = []
        self.hierarchies = []
        
    def initialize(self, ctx):
        logger.debug("Initializing %s" % self.name)
        super(HierarchyDimension, self).initialize(ctx)
        
        if (len(self.attributes) > 0):
            raise Exception ("%s is a HierarchyDimension and cannot have attributes." % (self))
        
        if (self.label == None): self.label = self.name
        

class Fact(Component):
    
    def __init__(self):
        
        super(Fact, self).__init__()
        
        self.name = None
        self.label = None
        
        self.dimensions = []
        self.attributes = []
        self.measures = []
        
    def initialize(self, ctx):
        
        super(Fact, self).initialize(ctx)
        
        if (self.label == None): self.label = self.name
        for attr in self.attributes:
            if (not "label" in attr):
                attr["label"] = attr["name"] 


class FactDimension(Dimension):
    
    def __init__(self):
        
        super(FactDimension, self).__init__()
        
        self.fact = None
        
    def finalize(self, ctx):
        ctx.comp.finalize(self.fact)
        super(FactDimension, self).finalize(ctx)
        
    def initialize(self, ctx):
        
        super(FactDimension, self).initialize(ctx)
        ctx.comp.initialize(self.fact)
        
        if (len(self.attributes) > 0):
            raise Exception("Cannot define attributes for a FactDimension (it's defined by the linked fact)")

    def attribute(self, search):
        att = [attr for attr in self.fact.attributes if attr["name"] == search]
        if (len(att) != 1):
            raise Exception("Could not find attribute %s in fact dimension %s" % (search, self.name))
        
        return att[0]


        
        
class OlapMapper(Component):
    
    def __init__(self):
        
        super(OlapMapper, self).__init__()
        
        self.mappers = []
        self.include = []

    def initialize(self, ctx):
        
        super(OlapMapper, self).initialize(ctx)

        for incl in self.include:
            ctx.comp.initialize(incl)
        for mapper in self.mappers:
            # TODO: if we do this, mappers shall be "prototype", in case there are several references 
            mapper.olapmapper = self
            ctx.comp.initialize(mapper)

    def finalize(self, ctx):
        for incl in self.include:
            ctx.comp.finalize(incl)        
        for mapper in self.mappers:
            ctx.comp.finalize(mapper)
        super(OlapMapper, self).finalize(ctx)

    """
    def getDimensionMapper(self, dim, fail = True):
        
        # Our mappings first
        for mapper in self.mappers:
            if (isinstance(mapper, DimensionMapper)):
                if (not isinstance(mapper.dimension, FactDimension)):
                    if (mapper.dimension.name == dim.name):
                        return mapper
                else:
                    if (mapper.dimension.fact.name == dim.name):
                        return mapper
        
        # Included mappers
        for inc in self.include:
            mapper = inc.getDimensionMapper(dim, False)
            if (mapper): return mapper

        if fail:
            raise Exception("No OLAP mapper found for dimension: %s" % dim.name)
        
        return None
    """
        
    def getEntityMapper(self, entity, fail = True):
        """Returns the OlapMapper that handles a fact or dimension.
        
        Included mappers are processed after local ones, so mapping
        definitions for different entities can be overrided.
        """
        
        for mapper in self.mappers:
            if (mapper.entity.name == entity.name):
                return mapper
                
        for inc in self.include:
            mapper = inc.getEntityMapper(entity, False)
            if (mapper): return mapper

        if fail:
            raise Exception("No OLAP mapper found for: %s" % entity.name)

        return None


class StoreFact(Node):
    
    def initialize(self, ctx):
        super(StoreFact, self).initialize(ctx)
        ctx.comp.initialize(self.mapper)
         
    def finalize(self, ctx):
        ctx.comp.finalize(self.mapper)
        super(StoreFact, self).finalize(ctx)
    
    def process(self, ctx, m):
        
        logger.debug ("Storing fact %s" % (self.fact.name))
        
        # Store dimensions
        # TODO: Shall be optional
        for dim in self.fact.dimensions:
            did = self.mapper.getEntityMapper(dim).store(ctx, m)
            # TODO: review this too, or use rarer prefix
            if (did != None): m[dim.name + "_id"] = did
        
        # Store
        # TODO: We shall not collect the ID here possibly
        fid = self.mapper.getEntityMapper(self.fact).store(ctx, m)
        if (fid != None): m[self.fact.name + "_id"] = fid
        
        yield m
        
