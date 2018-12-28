import logging
import json
from cubetl.core import Node
from cubetl.olap import HierarchyDimension, Fact, \
    Dimension
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
    """
    Exports OLAP configuration to Databrewery Cubes OLAP Server model format
    (and optionally a server configuration file). These files can then be used
    to run a server that can serve OLAP queries for such model.

    Note that, since Cubes does not support nested dimensions, the model is
    'flattened' so it can be managed by Cubes.
    """

    def __init__(self, olapmapper, model_path=None, config_path=None):
        super().__init__()
        self.olapmapper = olapmapper
        self.model_path = model_path
        self.config_path = config_path
        self.ignore_serialize_errors = False

        self._print = None

    def initialize(self, ctx):

        super(Cubes10ModelWriter, self).initialize(ctx)

        self._olapmapper = ctx.interpolate(None, self.olapmapper)
        ctx.comp.initialize(self._olapmapper)

        self._print = Print()
        self._print.depth = None
        self._print.truncate_line = None
        self._print._lexer = JsonLexer()
        self._print.eval = '${ m["cubesmodel_json"] }'
        ctx.comp.initialize(self._print)

    def finalize(self, ctx):

        ctx.comp.finalize(self._print)

        ctx.comp.finalize(self._olapmapper)
        super(Cubes10ModelWriter, self).finalize(ctx)

    def process(self, ctx, m):

        model = {"dimensions": [],
                 "cubes": []}

        self._exportolapmapper(ctx, model, self._olapmapper)

        # Prepare result
        m["cubesmodel"] = model
        m["cubesmodel_json"] = json.dumps(model,
                                          indent=4,
                                          sort_keys=True,
                                          default=str if self.ignore_serialize_errors else None)

        # Send to print node
        #print m["cubesmodel_json"]
        res = ctx.comp.process(self._print, m)
        for m in res:
            pass

        model_path = ctx.interpolate(m, self.model_path)
        if model_path:
            logger.info("Writing Cubes model path to: %s", model_path)
            with open(model_path, "w") as f:
                f.write(m["cubesmodel_json"])

        #print m["cubesmodel_json"]
        #_python_lexer = JsonLexer()
        #_terminal_formatter = TerminalFormatter()
        #print highlight(m["cubesmodel_json"], _python_lexer, _terminal_formatter)

        yield m

    def _get_cube_joins(self, ctx, mapper):

        c_joins = []
        for join in mapper.sql_joins(ctx, None):

            master_table_aliased = mapper.olapmapper.entity_mapper(join["master_entity"]).sqltable.name
            if len(join['alias']) > 1:
                master_table_aliased = "_".join(join['alias'][:-1])

            c_join = {"master": master_table_aliased + "." + join["master_column"],
                      "detail": join["detail_entity"] + "." + join["detail_column"],
                      "alias": "_".join(join['alias'])}
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

        logger.debug("Exporting mappings: %s", mapper)

        c_mappings = {}
        if base_mapper is None:
            base_mapper = mapper
        if parent_mapper is None:
            parent_mapper = mapper

        # Generate mappings for this mapper, possibly altering recursed mappings.

        mappings = mapper.sql_mappings(ctx)

        for mapping in mappings:
            print(mapping)
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
                        'column': mapping_sqltable_alias + "." + mapping.sqlcolumn_alias,
                        'extract': mapping.function
                    }
            except:
                logger.error("Cannot export mapping: %s", mapping)
                raise

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

    def _export_cube(self, ctx, model, mapper):

        cubename = mapper.entity.name
        if (cubename in [cube["name"] for cube in model["cubes"]]): return

        logger.debug("Exporting cube [entity: %s, cube: %s]" % (mapper.entity, mapper.entity.name))

        cube = {}
        cube["_comment"] = "Generated by CubETL (entity: %s)" % (mapper.entity.name)
        cube["name"] = cubename
        cube["label"] = mapper.entity.label
        cube["key"] = mapper.pk(ctx).sqlcolumn.name
        cube["dimensions"] = []

        # Add dimensions
        mapped_dimensions = self._get_dimensions_recursively(mapper.entity)
        for mapped_dimension in mapped_dimensions:
            # Flatten dimensions as Cubes10 does not support nested dimensions
            mapped_dimension_name = "_".join(mapped_dimension.path)
            cube["dimensions"].append(mapped_dimension_name)

            if (mapped_dimension_name not in [dim["name"] for dim in model["dimensions"]]):
                c_dim = self._export_dimension(ctx, model, mapped_dimension.entity, mapped_dimension_name, mapped_dimension.label)
                model["dimensions"].append(c_dim)

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
        cube["joins"] = self._get_cube_joins(ctx, mapper)

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

    def _export_level(self, ctx, entity):
        level = {}
        logger.debug("Exporting level: %s" % entity)

        level["name"] = entity.name
        level["label"] = entity.label
        level["label_attribute"] = None
        level["order_attribute"] = None
        level["attributes"] = []
        #level["key"] = entity.key.field.name if hasattr(entity.key, "field") else pk.entity.name
        if hasattr(entity, 'order_attribute'):
            level['order_attribute'] = entity.order_attribute

        for attribute in entity.attributes:

            #print(attribute.name)
            if isinstance(attribute, Dimension):
                continue

            level["attributes"].append(attribute.name)

            if isinstance(attribute, olap.Attribute) and level["label_attribute"] is None:
                level["label_attribute"] = attribute.name
                level["order_attribute"] = attribute.name

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

        #if level["label_attribute"] is None:
        #    level["label_attribute"] = pk.field.name if hasattr(pk, "field") else pk.entity.name

        return level

    def _export_dimension(self, ctx, model, dimension, alias_name=None, alias_label=None):

        logger.debug("Exporting dimension %s", dimension)

        dim = {}
        dim["_comment"] = "Generated by CubETL"
        dim["name"] = alias_name or dimension.name
        dim["label"] = alias_label or dimension.label
        dim["levels"] = []

        # Resolve aliased dimensions
        #dimension = mapper.entity
        #dimension_mapper = mapper.olapmapper.entity_mapper(dimension)

        # Attributes are levels
        if not dimension.hierarchies:
            c_lev = self._export_level(ctx, dimension)
            dim["levels"].append(c_lev)
        else:

            levels = []
            for level_attribute in dimension.get_dimensions():
                level = level_attribute.dimension
                c_lev = self._export_level(ctx, level)
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
                                  "levels": [ lev for lev in hierarchy.levels]}
                    dim["hierarchies"].append(chierarchy)

            # Add cubesviewer datefilter info
            if (dimension.role == "date"):
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
            if isinstance(mapper.entity, Fact):
                self._export_cube(ctx, model, mapper)


