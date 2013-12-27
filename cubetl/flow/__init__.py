import logging
from cubetl.core import Node
import copy
from cubetl.functions.text import parsebool
from cubetl.script import Eval

# Get an instance of a logger
logger = logging.getLogger(__name__)


class Chain(Node):
    
    def __init__(self):
        
        super(Chain, self).__init__()
        
        self.fork = False
        self.steps=[]
    
    def initialize(self, ctx):
        super(Chain, self).initialize(ctx)
        for p in self.steps:
            ctx.comp.initialize(p)
        
    def finalize(self, ctx):
        for p in self.steps:
            ctx.comp.finalize(p)
        super(Chain, self).finalize(ctx)
    
    def _process(self, steps, ctx, m):
        
        if (len(steps) <= 0):
            yield m
            return
        
        if ctx.debug2:
            logger.debug ("Processing step: %s" % (steps[0]))

        result_msgs = ctx.comp.process(steps[0], m)
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
            m2 = ctx.copy_message(m)
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

        
class Iterator(Node):
    
    def __init__(self):
        
        super(Iterator, self).__init__()
        
        self.name = None
        self.values = None

    def initialize(self, ctx):
        super(Iterator, self).initialize(ctx)
        
        if (self.name == None):
            raise Exception("Iterator field 'name' not set in node %s" % (self))
        
        if (self.values == None):
            raise Exception("Iterator field 'values' not set in node %s" % (self))
        
    def process(self, ctx, m):

        val_list = [ v.strip() for v in self.values.split(",") ]
        for val in val_list:
            # Copy message and set value
            logger.debug("Iterating: %s = %s" % (self.name, val))
            m2 = ctx.copy_message(m)
            m2[self.name] = val
            yield m2

            
class SplitEval(Node):
    """
    Note that evaluation is done via ctx.interpolate(), and so
    requires expressions to be delimited by ${}.
    """
    
    def __init__(self):
        
        super(SplitEval, self).__init__()
    
        self.instances = []
    
    def process(self, ctx, mo):

        logger.debug ("MultiEval (%s mappings)" % len(self.instances))
        
        for instance in self.instances:
            
            m = ctx.copy_message(mo)

            Eval.process_mappings(ctx, m, instance)
            
            yield m
                  
