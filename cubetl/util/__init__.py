# CubETL
# Copyright (c) 2013-2019 Jose Juan Montes

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


from pygments import highlight
from pygments.formatters.terminal import TerminalFormatter
from pygments.formatters.terminal256 import Terminal256Formatter
from pygments.lexers.agile import PythonLexer
from pygments.lexers.web import JsonLexer
import json
import logging
import pprint
import simplejson
import sys

from cubetl.core import Node


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

        value = ctx.interpolate(self.eval, m)

        if (not value):
            if (self.message):
                logger.error(ctx.interpolate(self.message, m))
            raise Exception("Assertion failed: %s = %s" % (self.eval, value))

        yield m


class Print(Node):
    """
    This class simply prints a message. Accepts an 'eval' property to evaluate.
    A default instance can be found in CubETL default objects.
    """

    def __init__(self, eval=None, condition=None, truncate_line=120, style='friendly'):
        super().__init__()

        self.eval = eval
        self.truncate_line = truncate_line
        self.condition = condition

        #['friendly', 'perldoc', 'vs', 'xcode', 'abap', 'autumn', 'bw', 'lovelace', 'paraiso-light', 'algol', 'arduino', 'rrt', 'algol_nu', 'paraiso-dark', 'colorful', 'manni', 'pastie', 'emacs', 'igor', 'trac', 'vim', 'murphy', 'rainbow_dash', 'default', 'tango', 'native', 'fruity', 'monokai', 'borland']
        self.style = style

        self._lexer = None
        self._formatter = None

    def initialize(self, ctx):

        super(Print, self).initialize(ctx)

        self._lexer = PythonLexer()
        #self._formatter = TerminalFormatter()
        self._formatter = Terminal256Formatter(style=self.style)

        logger.debug("Initializing Print node %s" % (self))

    def _prepare_res(self, ctx, m, obj):
        return str(obj)

    def process(self, ctx, m):

        if (not ctx.quiet):

            do_print = True

            if (self.condition):
                cond = ctx.interpolate(self.condition, m)
                if (not cond):
                    do_print = False

            if do_print:

                if (self.eval):
                    obj = ctx.interpolate(self.eval, m)
                else:
                    obj = m

                res = self._prepare_res(ctx, m, obj)

                if (self.truncate_line):
                    truncated = []
                    for line in res.split("\n"):
                        if (len(line) > self.truncate_line):
                            line = line[:self.truncate_line - 2] + ".."
                        truncated.append(line)
                    res = "\n".join(truncated)

                if sys.stdout.isatty():
                    print(highlight(res, self._lexer, self._formatter)[:-1])
                    #print(res)
                else:
                    print(res)

        yield m


class PrettyPrint(Print):
    """
    This class prints an object using pretty print, which allows for indenting.
    """

    def __init__(self, depth=3, indent=4):
        super().__init__()
        self.depth = depth
        self.indent = indent
        self._pp = None

    def initialize(self, ctx):

        super().initialize(ctx)

        self._pp = pprint.PrettyPrinter(indent=self.indent, depth=self.depth, width=self.truncate_line)

    def _prepare_res(self, ctx, m, obj):

        #res = str(obj)
        #if isinstance(obj, Bunch):
        #    obj = obj.toDict()
        res = self._pp.pformat(obj)

        return res

