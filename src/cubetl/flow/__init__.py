import logging
from cubetl.core import Node
import copy
from cubetl.functions.text import parsebool

# Get an instance of a logger
logger = logging.getLogger(__name__)


class Chain(Node):
    
    def __init__(self):
        
        super(Chain, self).__init__()
        
        self.fork = False
        self.steps=[]
    
    def signal(self, ctx, s):
        for p in self.steps:
            p.signal(ctx, s)
    
    def _process(self, steps, ctx, m):
        
        if (len(steps) <= 0):
            yield m
            return
        
        if ctx.debug2:
            logger.debug ("Processing step: %s" % (steps[0]))

        result_msgs = steps[0].process(ctx, m)
        for m in result_msgs:
            result_msgs2 = self._process(steps[1:], ctx, m)
            for m2 in result_msgs2:
                yield m2
    
    def process(self, ctx, m):

        if (not self.fork):
            result_msgs = self._process(self.steps, ctx, m)
            for m in result_msgs:
                yield m
        else:
            logger.debug("Forking flow")
            #m2 = copy.deepcopy (m)
            m2 = copy.copy (m)
            result_msgs = self._process(self.steps, ctx, m2)
            count = 0
            for mdis in result_msgs:
                count = count + 1
            
            logger.debug("Forked flow end - discarded %d messages" % count)
            yield m
            
class Filter(Node):
    
    def __init__(self):
        
        super(Filter, self).__init__()
        
        self.condition = None
    
    def process(self, ctx, m):

        if (self.condition == None):
            raise Exception("Filter node with no condition.")
        
        if (parsebool(ctx.interpolate(m, self.condition))):
            yield m
        else:
            if (ctx.debug2): logger.debug("Filtering out message")
            return
