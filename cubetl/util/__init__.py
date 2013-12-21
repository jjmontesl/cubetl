
from IPython.core.formatters import HTMLFormatter
from cubetl.core import Node
from pygments import highlight
from pygments.formatters.terminal import TerminalFormatter
from pygments.lexers.web import JsonLexer
import json
import logging
import pprint
import simplejson
import sys
from pygments.lexers.agile import PythonLexer

# Get an instance of a logger
logger = logging.getLogger(__name__)

class Assert(Node):
    """
    Evaluates and expression and 
    """
    
    def __init__(self):
        self.eval = None
        self.message = None
    
    def process(self, ctx, m):
        
        value = ctx.interpolate(m, self.eval)
        
        if (not value):
            if (self.message):
                logger.error(ctx.interpolate(m, self.message))
            raise Exception("Assertion failed: %s = %s" % (self.eval, value))
        
        yield m


class Print(Node):
    """
    This class simply prints a message. Accepts an 'eval' property to evaluate.
    A default instance can be found in CubETL default objects.
    """
    
    def __init__(self):
        self.eval = None
    
    def initialize(self, ctx):
        
        super(Print, self).initialize(ctx)
        
        logger.debug("Initializing Print node %s" % (self))    
    
    def process(self, ctx, m):
        
        if (not ctx.quiet):
        
            if (self.eval):
                print ctx.interpolate(m, self.eval)
            else:
                print m
        
        yield m

class PrettyPrint(Node):
    """
    This class simply prints a message. Accepts an 'eval' property to evaluate.
    A default instance can be found in CubETL default objects.
    """
    
    def __init__(self):
        
        self.eval = None
        
        self.depth = 2
        self.indent = 4
        self.truncate_line = 120
        
        self._pp = None
    
    def initialize(self, ctx):
        
        super(PrettyPrint, self).initialize(ctx)
        
        if (ctx.debug2): logger.debug("Initializing Print node %s" % (self))   
         
        self._pp = pprint.PrettyPrinter(indent=self.indent, depth = self.depth)
        
        self._python_lexer = PythonLexer()
        self._terminal_formatter = TerminalFormatter()
    
    def process(self, ctx, m):
    
        if (not ctx.quiet): 
        
            if (self.eval):
                obj = ctx.interpolate(m, self.eval)
            else:
                obj = m
            
            res = self._pp.pformat(obj)
            
            if (self.truncate_line):
                truncated = ""
                for line in res.split("\n"):
                    if (len(line) > self.truncate_line):
                        line = line[:self.truncate_line - 2] + ".."
                    truncated = truncated + line + "\n"
                res = truncated 
                        
            if sys.stdout.isatty():                    
                print highlight(res, self._python_lexer, self._terminal_formatter)
            else:
                print res
        
        yield m
        
