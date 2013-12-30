import logging
from springpython.context import InitializingObject
import cubetl
from springpython.config._config_base import ReferenceDef
from copy import deepcopy

# Get an instance of a logger
logger = logging.getLogger(__name__)

class Component(object):
    """
    Base class for all components. 
    """

    def __init__(self):
        pass
        
    def get_id(self):
        
        obj_list = cubetl.container.get_objects_by_type(object)
        ids = [ k for (k, v) in obj_list.items() if v == self ]
        return ids[0] if (len(ids) > 0) else None  
        
    def initialize(self, ctx):
        pass
        
    def finalize(self, ctx):
        pass
    
    def __str__(self, *args, **kwargs):
        
        cid = self.get_id()
        if (not cid and hasattr(self, "name")): cid = self.name 
        if (not cid): cid = id(self)
        
        return "%s(%s)" % (self.__class__.__name__, cid)
             
        #return object.__str__(self, *args, **kwargs)
    
        
class Node(Component):
    """
    Base class for all control flow nodes. 
    
    These must implement a process(ctx, m) method that
    accepts and yield messages.
    """

    def process(self, ctx, m):
        
        yield m
        
        
class ContextProperties(object):
    
    #def after_properties_set(self):   

    def load_properties(self, ctx):
        
        for attr in self.__dict__:
        
            value = getattr(self, attr)
            logger.debug("Setting context property %s = %s" % (attr, value))
            ctx.props[attr] = value
            
    
class Mappings(Component):
    """
    Serves as a holder for mappings, which can be included from other mappings.
    
    This component tries to make mappings more reusable, by providing a way to reference
    them 
    """
    
    def __init__(self):
        
        super(Mappings, self).__init__()
        
        self.mappings = None
        
    def initialize(self, ctx):
        
        super(Mappings, self).initialize(ctx)
        Mappings.includes(ctx, self.mappings)
        
    def finalize(self, ctx):
        super(Mappings, self).finalize(ctx) 
        
    @staticmethod
    def includes(ctx, mappings):
        
        mapping = True
        while mapping:
            pos = 0
            mapping = None
            for m in mappings:
                if (isinstance(m, Mappings)):
                    mapping = m
                    break
                else:
                    pos = pos + 1
                    
            if (mapping):
                # It's critical to copy mappings
                ctx.comp.initialize(mapping)
                mappings[pos:pos+1] = deepcopy(mapping.mappings)
            
            
    