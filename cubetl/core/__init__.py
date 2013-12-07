import logging
from springpython.context import InitializingObject

# Get an instance of a logger
logger = logging.getLogger(__name__)

class Component(object):
    """
    Base class for all components. 
    """

    def __init__(self):
        pass
        
    def initialize(self, ctx):
        pass
        
    def finalize(self, ctx):
        pass
    
    def __str__(self, *args, **kwargs):
        
        cid = id(self)
        if (hasattr(self, "name")): cid = self.name
            
        return "%s %s" % (self.__class__.__name__, cid)
             
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
            
    
    