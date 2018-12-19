import logging
import json
from cubetl.core import Node
from cubetl.olap import FactDimension, HierarchyDimension, Fact, AliasDimension
from cubetl.olap.sql import FactMapper, CompoundDimensionMapper,\
    MultiTableHierarchyDimensionMapper, EmbeddedDimensionMapper, DimensionMapper,\
    CompoundHierarchyDimensionMapper, AliasDimensionMapper
from pygments.formatters.terminal import TerminalFormatter
from pygments import highlight
from pygments.lexers.web import JsonLexer, JavascriptLexer
from cubetl.util import PrettyPrint, Print
from cubetl.core.exceptions import ETLConfigurationException
from cubetl import olap
import sys

# Get an instance of a logger
logger = logging.getLogger(__name__)


class Cubes10ModelWriter(Node):

    olapmapper = None
    _print = None

    def __init__(self):
        super(Cubes10ModelWriter, self).__init__()

    def initialize(self, ctx):

        super(Cubes10ModelWriter, self).initialize(ctx)
        ctx.comp.initialize(self.olapmapper)

        self._print = Print()
        self._print.depth = None
        self._print.truncate_line = None
        self._print._lexer = JsonLexer()
        self._print.eval = '${ m["cubesmodel_json"] }'
        ctx.comp.initialize(self._print)

    def finalize(self, ctx):

        ctx.comp.finalize(self._print)

        ctx.comp.finalize(self.olapmapper)
        super(Cubes10ModelWriter, self).finalize(ctx)

    def _get_joins_recursively(self, ctx, mapper):

        c_joins = []

        for join in mapper._joins(ctx, None):

            c_join = {
                      "master": mapper.olapmapper.entity_mapper(join["master_entity"]).sqltable.name + "." + join["master_column"],
                      "detail": join["detail_entity"] + "." + join["detail_column"],
                      "alias": join['alias'] if 'alias' in join else join["detail_entity"]
                      }
            c_joins.append(c_join)

            """
            if (not isinstance(dimmapper, SQLEmbeddedDimensionMapper)):
                if (ctx.debug2): logger.debug ("Exporting join for dimension %s for cube %s" % (dim.name, fact_mapper.fact.name))
                join = {}
                join["master"] = fact_mapper.table + "." + fieldmapping["column"]
                join["detail"] = dimmapper.table + "." + dimmapper.pk(ctx)["column"]
                join["alias"] = dimmapper.table
                joins.append(join)
            elif (isinstance(dimmapper, SQLFactDimensionMapper)):
                if (ctx.debug2): logger.debug ("Exporting fact join for dimension %s for cube %s" % (dim.name, fact_mapper.fact.name))
                join = {}
                join["master"] = fact_mapper.table + "." + fieldmapping["column"]
                join["detail"] = self.olapmapper.getFactMapper(dimmapper.dimension.fact).table + "." + self.olapmapper.getFactMapper(dimmapper.dimension.fact).pk(ctx)["column"]
                join["alias"] = self.olapmapper.getFactMapper(dimmapper.dimension.fact).table
                joins.append(join)

                joins.extend(self._get_joins_recursively(ctx, self.olapmapper.getFactMapper(dimmapper.dimension.fact)))
            """

        # TODO: FIXME: Hack because Cubes 1.0 requires joins to be declared in reverse order of dependency
        c_joins.reverse()

        return c_joins

    def _get_cube_mappings(self, ctx, mapper, base_mapper=None, parent_mapper=None):
        """
        Calculates Cubes mappings, which map Cubes entity attributes to database
        tables. This is used in the 'mappings' section of Cubes configuration.
        Returns a dictionary where keys are the mapped entity.attribute and
        values are the alias.column.
        """

        logger.debug("Exporting mappings: %s" % mapper)

        c_mappings = {}
        if base_mapper is None:
            base_mapper = mapper
        if parent_mapper is None:
            parent_mapper = mapper

        # Generate mappings for this mapper, possibly altering recursed mappings.

        mappings = mapper.sql_mappings(ctx)
        print("MAPPINGS:")
        print(mappings)

        for mapping in mappings:
            # Options are:
            # cube_name.detail = alias.column     # for details
            # dimension.attribute = alias.column  # for dimension attributes
            #c_mappings[mapping["entity"].name + "." + mapping['field'] = mapping['alias'] + "." + mapping['sqlcol'].name
            c_mappings[mapping.parent.name + "." + mapping.field.name] = mapping.alias + "." + mapping.sqlcolumn.name

        '''

        if (isinstance(mapper, FactMapper)):

            for dim in mapper.entity.dimensions:
                if isinstance(dim, AliasDimension):
                    # For AliasDimensions, the table is aliased
                    # TODO: this should come from OlapMapper, and not here ? (olapmapper will also need to expand mappings recursively)

                    dim_mapper = mapper.olapmapper.entity_mapper(dim.dimension)
                    sub_mappings = self._get_cube_mappings_recursively(ctx, dim_mapper, base_mapper, mapper)

                    print(dim.dimension)

                    for sm in sub_mappings.items():
                        # Account for "extracted" fields, that return a dictionary:
                        key = parent_mapper.entity.name + "." + sm[1].split(".")[1]
                        if isinstance(sm[1], dict):
                            c_mappings[key] = {"column": mapper.entity.name + "." + sm[1]["column"].split(".")[1],
                                               "extract": sm[1]["extract"]}
                        else:
                            c_mappings[key] = mapper.entity.name + "." + sm[1].split(".")[1]
                else:
                dim_mapper = mapper.olapmapper.entity_mapper(dim)
                sub_mappings = self._get_cube_mappings(ctx, dim_mapper, base_mapper, mapper)
                c_mappings.update(sub_mappings)

        elif (isinstance(mapper, MultiTableHierarchyDimensionMapper)):
            for dim in mapper.entity.levels:
                dim_mapper = self.olapmapper.entity_mapper(dim, False)
                sub_mappings = self._get_mappings(ctx, dim_mapper, base_mapper, mapper)
                for k, v in sub_mappings.items():
                    mapping_entityattribute = mapper.entity.name + "." + k.split(".")[1]
                    if mapping_entityattribute in c_mappings:
                        raise ETLConfigurationException("Attribute '%s' in %s is being mapped more than once. This can happen if the same column name is used by more than one levels in the hierarchy (hint: this happens often due to the same 'id' or 'names' attribute name being used by several dimensions in the hierarchy: use different names)." % (mapping_entityattribute, mapper))
                    c_mappings[mapping_entityattribute] = v

        elif (isinstance(mapper, EmbeddedDimensionMapper)):
            mappings = mapper._mappings_join(ctx)
            for mapping in mappings:
                #if "extract" in mapping:
                #    c_mappings[mapper.entity.name + "." + mapping["name"]] = {
                #        "column": parent_mapper.table + "." + mapping["column"],
                #        "extract": mapping["extract"]
                #        }
                #else:
                    c_mappings[mapper.entity.name + "." + mapping.entity.name] = parent_mapper.sqltable.name + "." + mapping.sqlcolumn.name

        elif (isinstance(mapper, DimensionMapper) or (isinstance(mapper, CompoundHierarchyDimensionMapper))):
            mappings = mapper._mappings(ctx)
            for mapping in mappings:
                c_mappings[mapper.entity.name + "." + mapping.entity.name] = mapping.sqlcolumn.sqltable.name + "." + mapping.sqlcolumn.name
        else:
            raise Exception("Unknown mapper type for cubes export: %s" % mapper)

        joins = mapper._joins(ctx, None)
        mappings = mapper._mappings(ctx)

        # TODO: Why is this section for? comment!
        for mapping in mappings:
            #if (mapping["entity"] == mapper.entity):
            #    c_mappings[mapping["entity"].name + "." + mapping["name"]] = mapper.table + "." + mapping["column"]
            if (mapping.entity not in [join['detail_entity'] for join in joins]):
                c_mappings[mapping.entity.name + "." + mapping.sqlcolumn.name] = mapping.sqlcolumn.sqltable.name + "." + mapping.sqlcolumn.name
            else:

                dim_mapper = mapper.olapmapper.entity_mapper(mapping.entity)
                sub_mappings = self._get_mappings(ctx, dim_mapper)

                if (isinstance(dim_mapper, MultiTableHierarchyDimensionMapper)):
                    # Enforce current entity (for hierarchies, this causes that all levels are
                    # mapped for the current dimension, forcing them to have different attribute names)
                    for k,v in sub_mappings.items():
                        c_mappings[mapping["entity"].name + "." + k.split(".")[1]] = v
                else:
                    c_mappings.update(sub_mappings)
        '''

        return c_mappings

    def _get_dimensions_recursively(self, entity):
        # NOTE: This is a major point of complexity: recursed dimensions
        # may incur in name conflicts and infinite loops.

        # NOTE: This shall be resolved by "olap" (since this would need to be used elsewhere)
        result = []
        for dimension in entity.dimensions:

            if isinstance(dimension, AliasDimension):
                # FIXME: the aliased dimension may not be a FactDimension?
                referenced_dimensions = self._get_dimensions_recursively(dimension.dimension.fact)
                logger.info("Recursively including %s dimensions: %s", dimension, referenced_dimensions)
                result.extend(referenced_dimensions)

            result.append(dimension)

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

        for attribute in entity.attributes:
            result.append(attribute)

        return result

    def _export_cube(self, ctx, model, mapper):

        cubename = mapper.entity.name
        if (cubename in [cube["name"] for cube in model["cubes"]]): return

        logger.debug("Exporting cube [entity: %s, cube: %s]" % (mapper.entity, mapper.entity.name))

        cube = {}
        cube["_comment"] = "Generated by CubETL"
        cube["name"] = cubename
        cube["label"] = mapper.entity.label
        cube["key"] = mapper.pk(ctx).sqlcolumn.name
        cube["dimensions"] = []

        # Add dimensions
        dimensions = self._get_dimensions_recursively(mapper.entity)
        for dimension in dimensions:

            cube["dimensions"].append(dimension.name)

            if (dimension.name not in [dim["name"] for dim in model["dimensions"]]):
                c_dim = self._export_dimension(ctx, model, mapper.olapmapper.entity_mapper(dimension))
                model["dimensions"].append(c_dim)

        # Add measures
        cube["measures"] = []
        #measures = self._get_measures_recursively(mapper.entity)
        for measure in mapper.entity.measures:
            c_measure = {}
            c_measure["name"] = measure.name
            c_measure["label"] = measure.label
            cube["measures"].append(c_measure)

        cube["aggregates"] = [ ]
        for measure in mapper.entity.measures:
            for func in ["sum", "avg", "max", "min"]:
                c_aggregate = {"name": measure.name + "_" + func,
                               "label": measure.label + " " + func[0].upper() + func[1:],
                               "function": func,
                               "measure": measure.name}
                cube["aggregates"].append(c_aggregate)
        c_aggregate = { "name": "record_count", "label": "Record Count", "function": "count" }
        cube["aggregates"].append(c_aggregate)

        # Joins
        cube["joins"] = self._get_joins_recursively(ctx, mapper)

        # Details
        cube["details"] = []
        attributes = self._get_attributes(mapper.entity)
        for attribute in attributes:
            cube["details"].append(attribute.name)
        # Add PK as detail
        cube["details"].append(mapper.pk(ctx).sqlcolumn.name)

        # Mappings
        cube["mappings"] = self._get_cube_mappings(ctx, mapper)

        model["cubes"].append(cube)

    def _export_level(self, ctx, mapper):

        level = {}
        logger.debug("Exporting level %s" % mapper.entity)

        mappings = mapper.sql_mappings(ctx)
        pk = mapper.pk(ctx)
        if not pk:
            pk = mappings[0]
        level["name"] = mapper.entity.name
        level["label"] = mapper.entity.label
        level["label_attribute"] = None
        level["order_attribute"] = None
        level["attributes"] = []
        level["key"] = pk.sqlcolumn.name
        if hasattr(mapper.entity, 'order_attribute'):
            level['order_attribute'] = mapper.entity.order_attribute

        for mapping in mappings:

            level["attributes"].append(mapping.field.name)

            if isinstance(mapping.field, olap.Attribute) and level["label_attribute"] is None:
                level["label_attribute"] = mapping.field.name
                level["order_attribute"] = mapping.field.name

            # Cubesviewer dates
            if (mapper.entity.role == "year"):
                level["role"] = "year"
            elif (mapper.entity.role == "quarter"):
                level["role"] = "quarter"
            elif (mapper.entity.role == "month"):
                level["role"] = "month"
            elif (mapper.entity.role == "week"):
                level["role"] = "week"
            elif (mapper.entity.role == "day"):
                level["role"] = "day"

        if level["label_attribute"] is None:
            level["label_attribute"] = pk.sqlcolumn.name

        return level

    def _export_dimension(self, ctx, model, mapper):

        logger.debug("Exporting dimension %s" % (mapper.entity))

        dim = {}
        dim["_comment"] = "Generated by CubETL"
        dim["name"] = mapper.entity.name
        dim["label"] = mapper.entity.label
        dim["levels"] = []

        # Aliased dimensions
        if (mapper.entity != mapper.entity):
            mapper = mapper.olapmapper.entity_mapper(mapper.entity)

        # Attributes are levels

        if (not hasattr(mapper.entity, "hierarchies")):
            c_lev = self._export_level(ctx, mapper)
            dim["levels"].append(c_lev)
        else:

            levels = []
            for level in mapper.entity.levels:
                level_mapper = mapper.olapmapper.entity_mapper(level)
                c_lev = self._export_level(ctx, level_mapper)
                dim["levels"].append(c_lev)
                levels.append(level)

            # Hierarchies
            finest_hierarchy = None
            if (len(mapper.entity.hierarchies) > 0):
                dim["hierarchies"] = []
                for hierarchy in mapper.entity.hierarchies:

                    if ((finest_hierarchy == None) or (len(hierarchy["levels"]) > len(finest_hierarchy["levels"]))):
                        finest_hierarchy = hierarchy

                    # Define hierarchy
                    chierarchy = {
                                  "name": hierarchy["name"],
                                  "label": hierarchy["label"] if ("label" in hierarchy) else hierarchy["name"],
                                  "levels": [ lev.name for lev in hierarchy["levels"] ]
                                  }
                    dim["hierarchies"].append(chierarchy)

            # Add cubesviewer datefilter info
            if (mapper.entity.role == "date"):
                dim["role"] = "time"
                dim["info"] = {
                    "cv-datefilter": True,
                    "cv-datefilter-hierarchy": finest_hierarchy["name"]
                }

        return dim

    def _exportolapmapper(self, ctx, model, olapmapper):

        # Call includes
        for inc in olapmapper.include:
            self._exportolapmapper(ctx, model, inc)

        # Export mappers
        for mapper in olapmapper.mappers:
            if isinstance(mapper, FactMapper):
                self._export_cube(ctx, model, mapper)

    def process(self, ctx, m):

        model = {"dimensions": [],
                 "cubes": []}

        self._exportolapmapper(ctx, model, self.olapmapper)

        # Prepare result
        m["cubesmodel"] = model
        m["cubesmodel_json"] = json.dumps(model, indent=4, sort_keys=True)

        # Send to print node
        #print m["cubesmodel_json"]
        res = ctx.comp.process(self._print, m)
        for m in res:
            pass

        #print m["cubesmodel_json"]
        #_python_lexer = JsonLexer()
        #_terminal_formatter = TerminalFormatter()
        #print highlight(m["cubesmodel_json"], _python_lexer, _terminal_formatter)

        yield m

