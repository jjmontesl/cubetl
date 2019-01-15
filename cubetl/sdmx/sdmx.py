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
from cubetl.olap import OlapMapper, Dimension, Fact, DimensionAttribute, Measure
from cubetl.sdmx.format import SDMXDSD, SDMXData
from cubetl.core import Node, Component
from cubetl import olap
from cubetl.text.functions import labelify


# Get an instance of a logger
logger = logging.getLogger(__name__)


class SDMXToOLAP(Component):
    """
    """

    @staticmethod
    def sdmx2olap(ctx, path_dsd, fact_name, fact_label=None, prefix='smdx2olap', entity_rename=None):


        if not entity_rename:
            entity_rename = {}

        def rename_entity(name):
            return entity_rename.get(name, name)

        path_dsd = ctx.interpolate(path_dsd)
        dsd = SDMXDSD(path_dsd)

        attributes = []

        logger.debug("Processing SDMX definition (DSD): %s (fact: %s)", path_dsd, fact_name)

        for dimension_key, dimension in dsd.data['timedimensions'].items():

            logger.debug("Processing time dimension: %s (%s)", dimension_key, dimension)

            entity_name = rename_entity(dimension_key)
            entity_label = dimension["concept"]["label"]

            # Create dimension
            dim_urn = "%s.fact.%s.dim.%s" % (prefix, fact_name, rename_entity(dimension_key))
            dimension = olap.Dimension(name=entity_name, label=entity_label, role="time", attributes=[
                olap.Attribute(name=entity_name, type="String", label=entity_label)])
            ctx.add(dim_urn, dimension)

            attributes.append(DimensionAttribute(dimension, entity_name, entity_label))

        for dimension_key, dimension in dsd.data['dimensions'].items():

            logger.debug("Processing time dimension: %s (%s)", dimension_key, dimension)

            entity_name = rename_entity(dimension_key)
            entity_label = dimension["concept"]["label"]

            # Create dimension
            dim_urn = "%s.fact.%s.dim.%s" % (prefix, fact_name, rename_entity(dimension_key))
            dimension = olap.Dimension(
                name=entity_name,
                label=entity_label,
                label_attribute=entity_name + "_label",
                role="time",
                attributes=[olap.Attribute(name=entity_name + "_code", type="String", label=entity_label + " Code"),
                            olap.Attribute(name=entity_name + "_label", type="String", label=entity_label)])
            ctx.add(dim_urn, dimension)

            attributes.append(DimensionAttribute(dimension, entity_name, entity_label))

        for measure_key, measure in dsd.data['measures'].items():
            attributes.append(Measure(name=measure_key, label=measure["label"], type="Float"))

        fact_urn = "%s.fact.%s" % (prefix, fact_name)
        fact = olap.Fact(name=fact_name, label=fact_label, attributes=attributes)
        ctx.add(fact_urn, fact)


class SDMXFileReader(Node):

    def __init__(self, path_dsd, path_sdmx):
        super().__init__()
        self.path_dsd = path_dsd
        self.path_sdmx = path_sdmx

    def process(self, ctx, m):

        path_dsd = ctx.interpolate(self.path_dsd)
        path_sdmx = ctx.interpolate(self.path_sdmx)

        logger.debug("Reading cells from SDMX file: %s" % path_sdmx)

        sdmx = SDMXData(path_dsd, path_sdmx)

        for cell in sdmx.read(ctx):
            m2 = ctx.copy_message(m)
            m2.update(cell)
            yield m2



