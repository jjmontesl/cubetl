
import cubetl
import logging

from cubetl.functions import text
import sys
import traceback

from repoze.lru import LRUCache
from cubetl.core.components import Components

# Get an instance of a logger
logger = logging.getLogger(__name__)

class Context():
    
    def __init__(self):
    
        self.args = {}
        
        self.debug = False
        self.debug2 = False
        
        self.config_files = []     
        
        self.startprocess = None
        
        self.props = {}
        
        self._globals = {
                         "text": text
                         } 
        
        self._compiled = LRUCache(512)  # TODO: Configurable
        
        self.comp = Components(self)
    
    def interpolate(self, m, value):
        
        # TODO: Naive interpolation

        # TODO: We are enforcing unicode working around Python Spring seems to give strings, not unicode
        # This shall not be necessary and it's bad practice
        result = unicode(value) 

        pos = result.find("${")
        while (pos >= 0):
            pos_end = result.find("}")
            expr = result[pos+2:pos_end].strip()
            
            compiled = self._compiled.get(expr)
            try:
                if (not compiled):
                    compiled = compile(expr, '', 'eval')
                    self._compiled.put(expr, compiled)
                res = eval (compiled, self._globals , { "m": m, "ctx": self })
                if (self.debug2):
                    logger.debug ('Evaluated: %s = %r' % (expr, res))
            except Exception, e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                raise Exception('Error evaluating expression "%s" on message: %s' % (expr, (traceback.format_exception_only(exc_type, exc_value))) ) 
            
            
            if ((pos>0) or (pos_end < len(result) - 1)):
                result = result[0:pos] + unicode(res) + result[pos_end+1:]
                pos = result.find("${")
            else:
                # Keep non-string types 
                result = res
                pos = -1
                
        return result

        
        
