import logging
from cubetl.core import Node

# Get an instance of a logger
logger = logging.getLogger(__name__)

class Script(Node):
    
    def __init__(self):
        
        super(Script, self).__init__()
        
        self.script = None
    
    def process(self, ctx, m):

        exec self.code in { "m": m, "ctx": self }
        
        yield m
        
class Eval(Node):
    """
    Note that evaluation is done via ctx.interpolate(), and so
    requires expressions to be delimited by ${}.
    """
    
    def __init__(self):
        
        super(Eval, self).__init__()
    
        self.mappings = []
    
    def process(self, ctx, m):

        # TODO: Mix functionalityh with XMLExtract which is very similar

        logger.debug ("EvalExtract (%s mappings)" % len(self.mappings))
        for mapping in self.mappings:
            
            if ("eval" in mapping):
                m[mapping["name"]] = ctx.interpolate(m, mapping["eval"])
            else:
                logging.warn("EvalExtract mapping with no 'eval' keyword: doing nothing.")
                
            if ("default" in mapping):
                if ((not mapping["name"] in m) or
                    (m[mapping["name"]] == None)
                    (m[mapping["name"]].strip() == "")):
                    
                    m[mapping["name"]] = ctx.interpolate(m, mapping["default"])                
            
        yield m
        
