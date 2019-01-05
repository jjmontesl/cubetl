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
from cubetl.core import Node, Component
from cubetl.core.exceptions import ETLConfigurationException

# Get an instance of a logger
logger = logging.getLogger(__name__)


class MDXQuery(Node):
    """
    Not yet implemented.
    """

    def __init__(self, mapper, query):
        super().__init__()
        self.mapper = mapper
        self.query = """
            SELECT
                { [Measures].[Sales Amount],
                    [Measures].[Tax Amount] } ON COLUMNS,
                { [Date].[Fiscal].[Fiscal Year].&[2002],
                    [Date].[Fiscal].[Fiscal Year].&[2003] } ON ROWS
            FROM [Adventure Works]
            WHERE ( [Sales Territory].[Southwest] )
        """

    def initialize(self, ctx):
        super().initialize(ctx)
        ctx.comp.initialize(self.mapper)

    def finalize(self, ctx):
        ctx.comp.finalize(self.mapper)
        super().finalize(ctx)

    def process(self, ctx, m):

        query = ctx.interpolate(m, self.query)
        logger.debug("MDX Query: %s" % (query))

        # Resolve drilldown and cuts to objects
        cells = MDXQuery.query(query)

        for cell in cells:
            m2 = ctx.copy_message(m)
            m2.update(cell)
            yield m2

