# CubETL

import logging
from cubetl.olap import OlapMapper, Dimension, Fact
from cubetl.sdmx.format import SDMXDSD, SDMXData
from lxml import etree
from cubetl.core import Node
from cubetl.xml import XmlPullParser
from cubetl.olap.sql import DimensionMapper, FactMapper, EmbeddedDimensionMapper


# Get an instance of a logger
logger = logging.getLogger(__name__)



class SDMXOlapMapper(OlapMapper):

    path_dsd = None
    path_sdmx = None

    fact_label = None
    id_prefix = None
    table_prefix = ""
    entity_prefix = ""
    entity_rename = None

    def initialize(self, ctx):


        if self.id_prefix is None:
            self.id_prefix = self.id

        self.model = {}
        self.olapmapper = None

        # Create mappers from SDMX
        self.load_sdmx(ctx)
        self.generate_model()
        self.generate_mapper()

        #for c in self.model.values():
        #    ctx.comp.initialize(c)

        super(SDMXOlapMapper, self).initialize(ctx)


    def load_sdmx(self, ctx):

        path_dsd = ctx.interpolate(ctx, self.path_dsd)
        self.dsd = SDMXDSD(path_dsd)

    def rename_entity(self, name):
        if self.entity_rename and name in self.entity_rename:
            return self.entity_rename[name]
        else:
            return name

    def generate_model(self):

        for dimension_key, dimension in self.dsd.data['timedimensions'].items():
            olapdimension = Dimension()
            olapdimension.id = self.id_prefix + self.rename_entity(dimension_key)
            olapdimension.name = self.entity_prefix + self.rename_entity(dimension_key)
            olapdimension.role = "time"
            olapdimension.label = dimension["concept"]["label"]
            olapdimension.attributes = [ { "name": dimension_key, "type": "String" } ]
            self.model[olapdimension.id] = olapdimension

        for dimension_key, dimension in self.dsd.data['dimensions'].items():
            olapdimension = Dimension()
            olapdimension.id = self.id_prefix + self.rename_entity(dimension_key)
            olapdimension.name = self.entity_prefix + self.rename_entity(dimension_key)
            olapdimension.label = dimension["concept"]["label"]
            olapdimension.attributes = [ { "name": dimension_key + "_code", "type": "String" },
                                         { "name": dimension_key + "_label", "type": "String" } ]
            self.model[olapdimension.id] = olapdimension

        measures = []
        for measure_key, measure in self.dsd.data['measures'].items():
            measures.append({"name": measure_key, "label": measure["label"], "type": "Float"})

        fact = Fact()
        fact.id = self.id_prefix + self.fact_name
        fact.name = self.entity_prefix + self.rename_entity(self.fact_name)
        fact.label = self.fact_label if self.fact_label else fact.name
        fact.dimensions = ( sorted([d for d in self.model.values() if isinstance(d, Dimension) and d.role == "time"], key=lambda x: x.label) +
                            sorted([d for d in self.model.values() if isinstance(d, Dimension) and not d.role == "time"], key=lambda x: x.label) )
        fact.measures = measures
        self.model[fact.id] = fact
        self.fact = fact

    def generate_mapper(self):
        self.olapmapper = self
        #self.olapmapper.mappers = []

        for dimension_key, sdmxdimension in self.dsd.data['dimensions'].items():
            did = self.id_prefix + self.rename_entity(dimension_key)
            d = self.model[did]
            mapper = DimensionMapper()
            mapper.entity = d
            mapper.table = self.table_prefix + d.name
            mapper.connection = self.connection
            mapper.lookup_cols = [ dimension_key + "_code" ]
            mapper.mappings = [{ "name": dimension_key + "_code", "pk": True, "type": "String", "value": "${m['%s']}" % (dimension_key + "_code")},
                               { "name": dimension_key + "_label", "type": "String", "value": "${m['%s']}" % (dimension_key + "_label")}]
            self.olapmapper.mappers.append(mapper)

        for dimension_key, sdmxdimension in self.dsd.data['timedimensions'].items():
            did = self.id_prefix + self.rename_entity(dimension_key)
            d = self.model[did]
            mapper = EmbeddedDimensionMapper()
            mapper.entity = d
            #mapper.table = self.table_prefix + d.name
            #mapper.connection = self.connection
            #mapper.lookup_cols = [ d.name ]
            #mapper.mappings = [{ "name": dimension_key, "pk": True, "type": "String", "value": "${m['%s']}" % (dimension_key)}]
            self.olapmapper.mappers.append(mapper)

        mapper = FactMapper()
        mapper.entity = self.fact
        mapper.table = self.table_prefix + self.fact.name
        mapper.connection = self.connection
        mapper.lookup_cols = [d.entity.name + "_code" for d in self.olapmapper.mappers]
        mapper.store_mode = self.store_mode
        mapper.autostore = [d.entity for d in self.olapmapper.mappers]
        mapper.mappings = [{ "name": self.fact.name + "_code", "pk": True, "type": "AutoIncrement"}]

        self.olapmapper.mappers.append(mapper)


class SDMXFileReader(Node):

    path_dsd = None
    path_sdmx = None

    def initialize(self, ctx):

        # Create mappers from SDMX

        super(SDMXFileReader, self).initialize(ctx)

    def process(self, ctx, m):

        path_dsd = ctx.interpolate(ctx, self.path_dsd)
        path_sdmx = ctx.interpolate(ctx, self.path_sdmx)

        logger.debug("Reading and processing SDMX file: %s" % path_sdmx)

        sdmx = SDMXData(path_dsd, path_sdmx)

        for fact in sdmx.read(ctx):
            m2 = ctx.copy_message(m)
            m2.update(fact)
            yield m2



