import logging
from cubetl.core import Node, Component
from cubetl.core.exceptions import ETLConfigurationException

# Get an instance of a logger
logger = logging.getLogger(__name__)


class OlapQueryAggregate(Node):

    def __init__(self, mapper, fact, drills=None, cuts=None):
        super().__init__()
        self.mapper = mapper
        self.fact = fact
        self.drills = drills or []
        self.cuts = cuts or []

    def initialize(self, ctx):
        super().initialize(ctx)
        ctx.comp.initialize(self.mapper)

    def finalize(self, ctx):
        ctx.comp.finalize(self.mapper)
        super().finalize(ctx)

    def process(self, ctx, m):

        entity = ctx.interpolate(m, self.fact)
        logger.debug("OLAP Query Aggregate: %s" % (entity.name))

        # Resolve drilldown and cuts to objects
        drills = self.drills
        cuts = self.cuts

        mapper = self.mapper.entity_mapper(entity)
        cells = mapper.query_aggregate(ctx, drills, cuts)

        for cell in cells:
            m2 = ctx.copy_message(m)
            m2.update(cell)
            yield m2

