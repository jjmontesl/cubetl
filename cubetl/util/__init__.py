
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
from pygments.formatters.terminal256 import Terminal256Formatter
#from bunch import Bunch

# Get an instance of a logger
logger = logging.getLogger(__name__)


class Assert(Node):
    """
    Evaluates and expression and
    """

    eval = None
    message = None

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

    eval = None

    truncate_line = 120

    condition = None

    _lexer = None
    _formatter = None


    def initialize(self, ctx):

        super(Print, self).initialize(ctx)

        self._lexer = PythonLexer()
        #self._formatter = TerminalFormatter()
        self._formatter = Terminal256Formatter()

        logger.debug("Initializing Print node %s" % (self))

    def _prepare_res(self, obj):
        return str(obj)

    def process(self, ctx, m):

        if (not ctx.quiet):

            do_print = True

            if (self.condition):
                cond = ctx.interpolate(m, self.condition)
                if (not cond):
                    do_print = False

            if do_print:

                if (self.eval):
                    obj = ctx.interpolate(m, self.eval)
                else:
                    obj = m

                res = self._prepare_res(obj)

                if (self.truncate_line):
                    truncated = []
                    for line in res.split("\n"):
                        if (len(line) > self.truncate_line):
                            line = line[:self.truncate_line - 2] + ".."
                        truncated.append(line)
                    res = "\n".join(truncated)


                if sys.stdout.isatty():
                    print(highlight(res, self._lexer, self._formatter)) #[:-1]
                    #print(res)
                else:
                    print(res)

        yield m


class PrettyPrint(Print):
    """
    This class prints an object using pretty print.
    """

    depth = 2
    indent = 4

    _pp = None

    def initialize(self, ctx):

        super(PrettyPrint, self).initialize(ctx)

        self._pp = pprint.PrettyPrinter(indent=self.indent, depth=self.depth)

    def _prepare_res(self, obj):

        #res = str(obj)
        #if isinstance(obj, Bunch):
        #    obj = obj.toDict()
        res = self._pp.pformat(obj)

        return res

