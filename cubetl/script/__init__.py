import logging
from cubetl.core import Node
import cubetl

# Get an instance of a logger
logger = logging.getLogger(__name__)

class Script(Node):
    
    def __init__(self):
        
        super(Script, self).__init__()
        
        self.script = None
        self.refs = {}
    
    def process(self, ctx, m):

        exec self.code in { "m": m, "ctx": ctx, "cubetl": cubetl, "refs": self.refs }
        
        yield m
        
class Eval(Node):
    """
    Note that evaluation is done via ctx.interpolate(), and so
    requires expressions to be delimited by ${}.
    """
    
    def __init__(self):
        
        super(Eval, self).__init__()
    
        self.mappings = []
    
    @staticmethod
    def process_mappings(ctx, m, mappings, data = {}):
        
        if (len(mappings) == 0):
            if (m != data):
                m.update(data)
        
        else:
        
            for mapping in mappings:
                
                if ("value" in mapping):
                    m[mapping["name"]] = ctx.interpolate(m, mapping["value"], { "d": data })
                else:
                    if (mapping["name"] in data):
                        m[mapping["name"]] = data[mapping["name"]]
                    else:
                        if (not "default" in mapping): 
                            logging.warn("Mapping with no value and no default: %s" % mapping)
                        m[mapping["name"]] = None
                
                if ("default" in mapping):
                    if (not m[mapping["name"]]):
                        m[mapping["name"]] = ctx.interpolate(data, mapping["default"])  
            

    def process(self, ctx, m):

        Eval.process_mappings(ctx, m, self.mappings)
            
        yield m
        
