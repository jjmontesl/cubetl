
import logging
from cubetl.core import Node

# Get an instance of a logger
logger = logging.getLogger(__name__)

class Print(Node):
    """
    This class simply prints a message. Accepts an 'eval' property to evaluate.
    A default instance can be found in CubETL default objects.
    """
    
    def __init__(self):
        self.eval = None
    
    def signal(self, ctx, s):
        
        super(Print, self).signal(ctx, s)
        
        logger.debug("Signal %s reached Log node %s" % (s, self))    
    
    def process(self, ctx, m):
        
        if (self.eval):
            print ctx.interpolate(m, self.eval)
        else:
            print m
        
        yield m
        
