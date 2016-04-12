# CubETL

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
        container = m

        values = [dimension.values for dimension in table.dimensions]
        for element in itertools.product(*values):
            for idx, val in enumerate(element):
                container[table.dimensions[idx].title] = val

            container['value'] = float(table.get(*element))

            yield m
