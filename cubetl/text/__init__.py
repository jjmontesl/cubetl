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
import re
import simplejson
import sys

from cubetl.core import Node
from cubetl.core.exceptions import ETLException
from cubetl.text.functions import *


# Get an instance of a logger
logger = logging.getLogger(__name__)




class LineReader(Node):
    """
    Splits text into lines.
    """


    def process(self, ctx, m):
        # TODO: Implement
        raise NotImplementedError()


class RegExp(Node):
    """
    Splits text into lines.
    """

    ERRORS_IGNORE = 'ignore'
    ERRORS_WARN = 'warn'
    ERRORS_FAIL = 'fail'

    def __init__(self, regexp, names=None, data='${ m["data"] }', errors=ERRORS_FAIL):
        super().__init__()
        self.regexp = regexp
        self.names = names
        self.data = data
        self.errors = errors
        self._error_count = 0

    def initialize(self, ctx):
        super(RegExp, self).initialize(ctx)
        self._error_count = 0

    def process(self, ctx, m):

        data = ctx.interpolate(self.data, m)

        #matches = re.search(self.regexp, data)
        matches = re.findall(self.regexp, data)

        if len(matches) == 0:
            if self.errors == RegExp.ERRORS_FAIL:
                raise ETLException("Failed to match regular expresion %s on value: %s" % (self.regexp, data))
            else:
                if self.errors == RegExp.ERRORS_WARN:
                    logger.warning("Failed to match regular expresion %s on value: %s", self.regexp, data)
                return

        matches = matches[0]
        if (ctx.debug2):
            logger.debug("Searching regexp %s into %s found %d matches" % (self.regexp, data, len(matches)))

        # Copy matches to message
        if isinstance(matches, str):
            matches = [ matches ]
        for i in range(0, len(matches)):
            match_name = "regexp_match_" + str(i + 1)
            if (self.names):
                match_name = self.names.split(",")[i].strip()
            m[match_name] = matches[i]

        yield m

