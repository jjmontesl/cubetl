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


import logging
from cubetl.core import Node
from cubetl.pcaxis.ext.pcaxis import parse
import itertools


# Get an instance of a logger
logger = logging.getLogger(__name__)


class PCAxisParser(Node):

    def process(self, ctx, m):

        logger.debug("Parsing PCAxis")
        m["pcaxis"] = parse(m["data"])

        logger.info("Loaded PCAxis table: %r" % (m['pcaxis']))

        yield m


class PCAxisIterator(Node):

    def process(self, ctx, m):

        table = m['pcaxis']

        #m['pca'] = {}

        values = [dimension.values for dimension in table.dimensions]
        for element in itertools.product(*values):

            container = {}

            for idx, val in enumerate(element):
                container[table.dimensions[idx].title] = val

            value = table.get(*element)
            try:
                container['value'] = value
            except ValueError as e:
                logger.warning("PCAxisIterator could not parse value: %r (cell: %s)", value, container)
                value = None

            m.update(container)
            yield m

