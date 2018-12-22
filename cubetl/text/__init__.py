
from cubetl.core import Node
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


    regexp = None
    names = None
    data = '${ m["data"] }'


    def initialize(self, ctx):

        super(RegExp, self).initialize(ctx)

        if (self.regexp == None):
            raise Exception("Missing regexp attribute for %s" % self)

    def process(self, ctx, m):
        # TODO: Implement

        data = ctx.interpolate(m, self.data)


        #matches = re.search(self.regexp, data)
        matches = re.findall(self.regexp, data)[0]
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

