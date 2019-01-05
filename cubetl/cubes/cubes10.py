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


from pygments.lexers.data import JsonLexer
import json
import logging
import os
import sys

from cubetl import olap
from cubetl.core import Node
from cubetl.core.exceptions import ETLConfigurationException
from cubetl.olap import Fact, Dimension
from cubetl.template.jinja import JinjaTemplateRenderer
from cubetl.util import Print
from cubetl.sql.sql import Connection
from collections import OrderedDict


# Get an instance of a logger
logger = logging.getLogger(__name__)


class Cubes10ModelWriter(Node):
    """
    Exports OLAP configuration to Databrewery Cubes OLAP Server model format
    (and optionally a server configuration file). These files can then be used
    to run a server that can serve OLAP queries for such model.

    Note that, since Cubes does not support nested dimensions, the model is
    'expanded' so it can be managed by Cubes.
    """

    def __init__(self, olapmapper, model_path=None, config_path=None, add_data=False):
        super().__init__()
        self.olapmapper = olapmapper
        self.model_path = model_path
        self.config_path = config_path
        self.ignore_serialize_errors = False
        self.add_data = add_data

        self._print = None
        self._template_renderer = None

    def initialize(self, ctx):

        super(Cubes10ModelWriter, self).initialize(ctx)

        self._olapmapper = ctx.interpolate(None, self.olapmapper)
        ctx.comp.initialize(self._olapmapper)

        self._print = Print()
        self._print.depth = None
        self._print.truncate_line = None
        self._print._lexer = JsonLexer()
        #self._print.eval = '${ m["cubesmodel_json"] }'
        ctx.comp.initialize(self._print)

        config_path = ctx.interpolate(None, self.config_path)
        if config_path:
            template_path = os.path.dirname(__file__) + "/cubes.config.template"
            logger.info("Reading cubes config template from: %s", template_path)
            template_text = open(template_path).read()
            self._template_renderer = JinjaTemplateRenderer(template=template_text)

    def finalize(self, ctx):

        ctx.comp.finalize(self._print)

        ctx.comp.finalize(self._olapmapper)
        super(Cubes10ModelWriter, self).finalize(ctx)

    def process(self, ctx, m):

        model = {"dimensions": OrderedDict(),
                 "cubes": []}

        # FIXME: repeated parameter to track base mapper, elsewhere found from ctx,.. all olapmapper including incorrect
        connection = self._exportolapmapper(ctx, model, self._olapmapper, self._olapmapper)
        model["dimensions"] = [v for v in model["dimensions"].values()]

        # Prepare result
        model_json = json.dumps(model,
                                indent=4,
                                sort_keys=True,
                                default=str if self.ignore_serialize_errors else None)

        # Add to message
        if self.add_data:
            m["cubesmodel"] = model
            m["cubesmodel_json"] = model_json

        # Send to print node
        #print m["cubesmodel_json"]
        res = ctx.comp.process(self._print, model_json)
        for m2 in res:
            pass

        model_path = ctx.interpolate(m, self.model_path)
        if model_path:
            logger.info("Writing Cubes server model to: %s", model_path)
            with open(model_path, "w") as f:
                f.write(model_json)

        config_path = ctx.interpolate(m, self.config_path)
        if config_path:
            connection_url = connection.url if connection else None
            logger.info("Writing Cubes server config to: %s", config_path)
            config_text = self._template_renderer.render(ctx, {'model_path': model_path, 'db_url': connection_url})
            with open(config_path, "w") as f:
                f.write(config_text)

        yield m

    def _get_cube_joins(self, ctx, olapmapper, mapper):

        c_joins = []
        for join in mapper.sql_joins(ctx, None):

            master_table_aliased = olapmapper.entity_mapper(join["master_entity"]).sqltable.name
            if len(join['alias']) > 1:
                master_table_aliased = "_".join(join['alias'][:-1])

            c_join = {"master": master_table_aliased + "." + join["master_column"],
                      "detail": join["detail_entity"] + "." + join["detail_column"],
                      "alias": "_".join(join['alias'])}
            c_joins.append(c_join)

        # Cubes 1.0 requires joins to be declared in reverse order of dependency
        c_joins.reverse()

        return c_joins

    def _get_cube_mappings(self, ctx, mapper, base_mapper=None, parent_mapper=None):
        """
        Calculates Cubes mappings, which map Cubes entity attributes to database
        tables. This is used in the 'mappings' section of Cubes configuration.
        Returns a dictionary where keys are the mapped entity.attribute and
        values are the alias.column.
        """

        logger.debug("Exporting mappings: %s", mapper)

        c_mappings = {}
        if base_mapper is None:
            base_mapper = mapper
        if parent_mapper is None:
            parent_mapper = mapper

        # Generate mappings for this mapper, possibly altering recursed mappings.

        mappings = mapper.sql_mappings(ctx)

        for mapping in mappings:
            #print(mapping)
            # Options are:
            # cube_name.detail = alias.column     # for details
            # dimension.attribute = alias.column  # for dimension attributes
            #c_mappings[mapping["entity"].name + "." + mapping['field'] = mapping['alias'] + "." + mapping['sqlcol'].name
            try:

                # Flatten path to 2 levels as Cubes does not support nested dimensions
                if len(mapping.path) > 2:
                    mapping_path = "_".join(mapping.path[:-1]) + "." + mapping.path[-1]
                else:
                    mapping_path = ".".join(mapping.path)

                if len(mapping.sqltable_alias) > 0:
                    mapping_sqltable_alias = "_".join(mapping.sqltable_alias)
                else:
                    mapping_sqltable_alias = mapping.sqltable.name

                c_mappings[mapping_path] = mapping_sqltable_alias + "." + mapping.sqlcolumn_alias
                if mapping.function:
                    c_mappings[mapping_path] = {
                        'table': mapping_sqltable_alias,
                        'column': mapping.sqlcolumn_alias,  # mapping_sqltable_alias + "." +  ...
                        'extract': mapping.function
                    }
            except:
                logger.error("Cannot export mapping: %s", mapping)
                raise

        return c_mappings

    def _get_dimensions_recursively(self, entity):

        result = entity.get_dimensions_recursively()

        # Flatten dimensions as Cubes does not support nested dimension

        return result

    def _get_attributes(self, entity):
        """
        Attributes are not obtained recursively. Recursed attributes are actually
        dimension attributes.
        """

        # NOTE: This is a major point of complexity: recursed dimensions
        # may incur in name conflicts and infinite loops.
        result = []

        '''
        # Commented out: recursed attributes are actually dimension attributes
        for dimension in entity.dimensions:
            if isinstance(dimension, AliasDimension):
                # FIXME: the aliased dimension may not be a FactDimension?
                referenced_attributes = self._get_attributes_recursively(dimension.dimension.fact)
                result.extend(referenced_attributes)
        '''

        for attribute in entity.get_attributes():
            result.append(attribute)

        return result

    def _export_cube(self, ctx, model, olapmapper, mapper):

        cubename = mapper.entity.name
        if (cubename in [cube["name"] for cube in model["cubes"]]): return

        logger.info("Exporting cube [entity: %s, cube: %s]" % (mapper.entity, mapper.entity.name))

        cube = {}
        cube["_comment"] = "Generated by CubETL (entity: %s)" % (mapper.entity.name)
        cube["name"] = cubename
        cube["label"] = mapper.entity.label
        cube["key"] = mapper.pk(ctx).sqlcolumn.name  # if mapper.pk(ctx) else None
        cube["dimensions"] = []

        # Add dimensions
        mapped_dimensions = self._get_dimensions_recursively(mapper.entity)
        for mapped_dimension in mapped_dimensions:
            # Flatten dimensions as Cubes10 does not support nested dimensions
            # if len(mapped_dimension.path) > 2: continue
            mapped_dimension_name = "_".join(mapped_dimension.path)
            cube["dimensions"].append(mapped_dimension_name)

            if (mapped_dimension_name not in model["dimensions"]):
                c_dim = self._export_dimension(ctx, olapmapper, model, mapped_dimension.entity, mapped_dimension_name, mapped_dimension.label)
                model["dimensions"][mapped_dimension_name] = c_dim

        # Add measures
        cube["measures"] = []
        #measures = self._get_measures_recursively(mapper.entity)
        for measure in mapper.entity.get_measures():
            c_measure = {}
            c_measure["name"] = measure.name
            c_measure["label"] = measure.label
            cube["measures"].append(c_measure)

        cube["aggregates"] = [ ]
        for measure in mapper.entity.get_measures():
            for func in ["sum", "avg", "max", "min"]:
                c_aggregate = {"name": measure.name + "_" + func,
                               "label": measure.label + " " + func[0].upper() + func[1:],
                               "function": func,
                               "measure": measure.name}
                cube["aggregates"].append(c_aggregate)
        c_aggregate = { "name": "record_count", "label": "Record Count", "function": "count" }
        cube["aggregates"].append(c_aggregate)

        # Joins
        cube["joins"] = self._get_cube_joins(ctx, olapmapper, mapper)

        # Details
        cube["details"] = []
        attributes = self._get_attributes(mapper.entity)
        for attribute in attributes:
            cube["details"].append(attribute.name)
        # Add PK as detail
        cube["details"].append(mapper.pk(ctx).sqlcolumn.name)  # (do not call pk() again) if mapper.pk(ctx):

        # Mappings
        cube["mappings"] = self._get_cube_mappings(ctx, mapper)

        model["cubes"].append(cube)

    def _export_level(self, ctx, olapmapper, entity):
        level = {}
        logger.debug("Exporting level: %s" % entity)

        level["name"] = entity.name
        level["label"] = entity.label
        level["label_attribute"] = entity.label_attribute
        level["order_attribute"] = entity.order_attribute
        level["attributes"] = []
        if entity.info:
            level["info"] = dict(entity.info)

        for attribute in entity.attributes:

            #print(attribute.name)
            if isinstance(attribute, Dimension):
                continue

            level["attributes"].append(attribute.name)

            if isinstance(attribute, olap.Attribute) and level["label_attribute"] is None:
                level["label_attribute"] = attribute.name

            # Cubesviewer dates
            if (entity.role == "year"):
                level["role"] = "year"
            elif (entity.role == "quarter"):
                level["role"] = "quarter"
            elif (entity.role == "month"):
                level["role"] = "month"
            elif (entity.role == "week"):
                level["role"] = "week"
            elif (entity.role == "day"):
                level["role"] = "day"
            elif (entity.role):
                level["role"] = entity.role

        #if level["label_attribute"] is None:
        #    level["label_attribute"] = pk.field.name if hasattr(pk, "field") else pk.entity.name
        if level["order_attribute"] is None:
            level["order_attribute"] = level["label_attribute"]

        # Level key
        if 'key' not in level:
            mapper = olapmapper.entity_mapper(entity, fail=False)
            if mapper:
                pk = mapper.pk(ctx)
                if pk:
                    level["key"] = pk.sqlcolumn.name
                    level["attributes"].insert(0, pk.sqlcolumn.name)

        return level

    def _export_dimension(self, ctx, olapmapper, model, dimension, alias_name=None, alias_label=None):

        logger.debug("Exporting dimension %s (name: %s)", dimension, alias_name)

        dim = {}
        dim["_comment"] = "Generated by CubETL"
        dim["name"] = alias_name or dimension.name
        dim["label"] = alias_label or dimension.label
        dim["levels"] = []

        # Attributes are levels
        if not dimension.hierarchies:
            c_lev = self._export_level(ctx, olapmapper, dimension)
            dim["levels"].append(c_lev)
        else:

            levels = []
            for level_attribute in dimension.get_dimensions():
                level = level_attribute.dimension
                c_lev = self._export_level(ctx, olapmapper, level)
                #level_mapper = mapper.olapmapper.entity_mapper(level)
                #c_lev = self._export_level(ctx, level_mapper)
                dim["levels"].append(c_lev)
                levels.append(level)

            # Hierarchies
            finest_hierarchy = None
            if (len(dimension.hierarchies) > 0):
                dim["hierarchies"] = []
                for hierarchy in dimension.hierarchies:

                    if ((finest_hierarchy is None) or (len(hierarchy.levels) > len(finest_hierarchy.levels))):
                        finest_hierarchy = hierarchy

                    # Define hierarchy
                    chierarchy = {"name": hierarchy.name,
                                  "label": hierarchy.label,
                                  "levels": [lev for lev in hierarchy.levels]}
                    dim["hierarchies"].append(chierarchy)

            # Add cubesviewer datefilter info
            if (dimension.role == "date"):
                dim["role"] = "time"
                dim["info"] = {
                    "cv-datefilter": True,
                    "cv-datefilter-hierarchy": finest_hierarchy.name
                }

        return dim

    def _exportolapmapper(self, ctx, model, basemapper, olapmapper):

        connection = None

        # Call includes
        for inc in olapmapper.include:
            cons = self._exportolapmapper(ctx, model, basemapper, inc)
            connection = connection or cons

        # Export mappers
        for mapper in olapmapper.mappers:
            if isinstance(mapper.entity, Fact):
                self._export_cube(ctx, model, basemapper, mapper)
                connection = mapper.sqltable.connection

        # Return the connection
        return connection
