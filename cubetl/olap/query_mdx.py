import logging
from cubetl.core import Node, Component
from cubetl.core.exceptions import ETLConfigurationException

# Get an instance of a logger
logger = logging.getLogger(__name__)


class MDXQuery(Node):

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

