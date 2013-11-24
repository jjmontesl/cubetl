import logging

# Get an instance of a logger
logger = logging.getLogger(__name__)

class Component(object):
    """
    Base class for all components. 
    
    These must implement a signal(ctx, s) method that
    accepts messages.
    """

    def signal(self, ctx, s):
        pass
    
        
class Node(Component):
    """
    Base class for all control flow nodes. 
    
    These must implement a process(ctx, m) method that
    accepts and yield messages.
    
    It is critical that no returns are used within
    nodes process() method.
    """

    def signal(self, ctx, s):
        super(Node, self).signal(ctx, s)

    def process(self, ctx, m):
        
        yield m
        