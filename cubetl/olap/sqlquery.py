import logging
from cubetl.core import Node, Component
from cubetl.core.exceptions import ETLConfigurationException
#from cubetl.olap.sql import FactMapper
from past.builtins import basestring

# Get an instance of a logger
logger = logging.getLogger(__name__)


class OlapQueryAggregate(Node):

    def initialize(self, ctx):
        super(OlapQueryAggregate, self).initialize(ctx)
        ctx.comp.initialize(self.mapper)

    def finalize(self, ctx):
        ctx.comp.finalize(self.mapper)
        super(OlapQueryAggregate, self).finalize(ctx)

    def process(self, ctx, m):

        entity = ctx.interpolate(m, self.entity)
        logger.debug("OLAP Query Aggregate: %s" % (entity.name))

        # Store
        # TODO: We shall not collect the ID here possibly
        fid = self.mapper.entity_mapper(entity).store(ctx, m)
        if (fid is not None):
            m[entity.name + "_id"] = fid

        yield m
