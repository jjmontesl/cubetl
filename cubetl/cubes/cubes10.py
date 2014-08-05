import logging
import json
from cubetl.core import Node
from cubetl.olap import FactDimension, HierarchyDimension
from cubetl.olap.sql import FactMapper, CompoundDimensionMapper,\
    MultiTableHierarchyDimensionMapper, EmbeddedDimensionMapper, DimensionMapper,\
    CompoundHierarchyDimensionMapper
from pygments.formatters.terminal import TerminalFormatter
from pygments import highlight
from pygments.lexers.web import JsonLexer, JavascriptLexer
from cubetl.util import PrettyPrint, Print

# Get an instance of a logger
logger = logging.getLogger(__name__)

class Cubes10ModelWriter(Node):
    
    def __init__(self):
        
        super(Cubes10ModelWriter, self).__init__()
        
        self.olapmapper = None
        self.__dict__["print"] = True
        
        self._print = None
    
    def initialize(self, ctx):
        
        super(Cubes10ModelWriter, self).initialize(ctx)
        ctx.comp.initialize(self.olapmapper)
        
        if (self.__dict__["print"]):
            self._print = Print()
            self._print.depth = None
            self._print.truncate_line = None
            self._print._lexer = JsonLexer()
            self._print.eval = '${ m["cubesmodel_json"] }'
            ctx.comp.initialize(self._print)

    def finalize(self, ctx):
        if (self.__dict__["print"]): ctx.comp.finalize(self._print)
        
        ctx.comp.finalize(self.olapmapper)
        super(Cubes10ModelWriter, self).initialize(ctx)

    def _get_dimensions(self, fact):
        result = []
        for dim in fact.dimensions:
            result.append(dim.name)
                
        return result

    def _get_joins(self, ctx, mapper):
        
        c_joins = []
        
        for join in mapper._joins(ctx, None):
            
            c_join = {
                      "master": mapper.olapmapper.entity_mapper(join["master_entity"]).table + "." + join["master_column"],
                      "detail": mapper.olapmapper.entity_mapper(join["detail_entity"]).table + "." + join["detail_column"],
                      "alias":  mapper.olapmapper.entity_mapper(join["detail_entity"]).entity.name
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
                
                joins.extend(self._get_joins(ctx, self.olapmapper.getFactMapper(dimmapper.dimension.fact)))
            """
            
        return c_joins

    def _get_mappings(self, ctx, mapper, base_table = None):
        
        logger.debug("Exporting mappings: %s" % mapper)

        c_mappings = {}
        
        if (isinstance(mapper, FactMapper)):
            for dim in mapper.entity.dimensions:
                dim_mapper = mapper.olapmapper.entity_mapper(dim)
                sub_mappings = self._get_mappings(ctx, dim_mapper, mapper.table)
                c_mappings.update(sub_mappings)

        elif (isinstance(mapper, MultiTableHierarchyDimensionMapper)):
            for dim in mapper.entity.levels:
                dim_mapper = self.olapmapper.entity_mapper(dim, False)
                sub_mappings = self._get_mappings(ctx, dim_mapper, dim_mapper.entity.name)
                for k,v in sub_mappings.items():
                    c_mappings[mapper.entity.name + "." + k.split(".")[1]] = v
                
        elif (isinstance(mapper, EmbeddedDimensionMapper)):
            mappings = mapper._mappings_join(ctx)
            for mapping in mappings:
                c_mappings[base_table + "." + mapping["name"]] = base_table + "." + mapping["column"]
                
        elif (isinstance(mapper, DimensionMapper) or (isinstance(mapper, CompoundHierarchyDimensionMapper))):
            mappings = mapper._mappings(ctx)
            for mapping in mappings:
                c_mappings[mapper.entity.name + "." + mapping["name"]] = mapper.entity.name + "." + mapping["column"]
        
        else:
            raise Exception ("Unknown mapper type for cubes export: %s" % mapper)
            
        """
        joins = mapper._joins(ctx, None)
        mappings = mapper._mappings(ctx)
        
        #logger.info( mapper)
        #logger.info( mappings)
        for mapping in mappings:
            #if (mapping["entity"] == mapper.entity):
            #    c_mappings[mapping["entity"].name + "." + mapping["name"]] = mapper.table + "." + mapping["column"]
            if (mapping["entity"] not in [join["detail_entity"] for join in joins]):
                c_mappings[mapping["entity"].name + "." + mapping["name"]] = mapper.table + "." + mapping["column"]
            else:

                dim_mapper = mapper.olapmapper.entity_mapper(mapping["entity"])
                sub_mappings = self._get_mappings(ctx, dim_mapper)
                
                if (isinstance(dim_mapper, MultiTableHierarchyDimensionMapper)):
                    # Enforce current entity (for hierarchies, this causes that all levels are
                    # mapped for the current dimension, forcing them to have different attribute names)
                    for k,v in sub_mappings.items():
                        c_mappings[mapping["entity"].name + "." + k.split(".")[1]] = v
                else:
                    c_mappings.update(sub_mappings)
        """        
                    
        return c_mappings

    def _export_cube(self, ctx, model, mapper):
        
        if (mapper.entity.name in [cube["name"] for cube in model["cubes"]]): return

        logger.debug ("Exporting cube %s" % (mapper.entity.name))
        
        cube = {}
        cube["dimensions"] = []
        cube["_comment"] = "Generated by CubETL"
        cube["name"] = mapper.entity.name
        cube["label"] = mapper.entity.label
        cube["key"] = mapper.pk(ctx)["name"]

        # Add dimensions        
        for dimension in mapper.entity.dimensions:
            
            cube["dimensions"].append(dimension.name)

            if (not dimension.name in [dim["name"] for dim in model["dimensions"]]):
                c_dim = self._export_dimension(ctx, model, mapper.olapmapper.entity_mapper(dimension))
                model["dimensions"].append(c_dim)
        
        # Add measures
        cube["measures"] = []
        for measure in mapper.entity.measures:
            c_measure = {}
            c_measure["name"] = measure["name"]
            c_measure["label"] = measure["label"]
            cube["measures"].append(c_measure)
        
        cube["aggregates"] = [ ]
        for measure in mapper.entity.measures:
            for func in ["sum", "avg", "max", "min"]:
                c_aggregate = { "name": measure["name"] + "_" + func, "label": measure["name"] + " " + func, "function": func, "measure": measure["name"] }
                cube["aggregates"].append(c_aggregate)
        c_aggregate = { "name": "record_count", "label": "Record Count", "function": "count" }
        cube["aggregates"].append(c_aggregate)
        
        # Joins
        cube["joins"] = self._get_joins(ctx, mapper)

        # Details
        cube["details"] = []
        for attribute in mapper.entity.attributes:
            cube["details"].append(attribute["name"])
            
        # Mappings
        cube["mappings"] = self._get_mappings(ctx, mapper)
        
        model["cubes"].append(cube)

    def _export_level(self, ctx, mapper):
        
        level = {}
        
        pk = mapper.pk(ctx)
        mappings = mapper._mappings(ctx)
        
        level["name"] = mapper.entity.name
        level["label"] = mapper.entity.label
        level["attributes"] = []
        level["key"] = pk["name"] if pk else mappings[0]["name"] 
        level["label_attribute"] = mappings[1]["name"] if len(mappings) >= 2 else mappings[0]["name"]
        
        for mapping in mappings:

            level["attributes"].append(mapping["name"])
            
            # Cubesviewer dates
            if (mapper.entity.role == "year"): level["info"] = { "cv-datefilter-field": "year" }
            elif (mapper.entity.role == "quarter"): level["info"] = { "cv-datefilter-field": "quarter" }
            elif (mapper.entity.role == "month"): level["info"] = { "cv-datefilter-field": "month" }
            elif (mapper.entity.role == "week"): level["info"] = { "cv-datefilter-field": "week" }
            elif (mapper.entity.role == "day"): level["info"] = { "cv-datefilter-field": "day" }
        
        return level
                
    def _export_dimension(self, ctx, model, mapper):
        
        logger.debug ("Exporting dimension %s" % (mapper.entity.name))
        
        dim = {}
        dim["_comment"] = "Generated by CubETL"
        dim["name"] = mapper.entity.name 
        dim["label"] = mapper.entity.label
        dim["levels"] = []

        # Attributes are levels
        
        if (not hasattr(mapper.entity, "hierarchies")):
            c_lev = self._export_level(ctx, mapper)
            dim["levels"].append (c_lev)
        else:
            
            levels = []
            for level in mapper.entity.levels:
                level_mapper = mapper.olapmapper.entity_mapper(level)
                c_lev = self._export_level(ctx, level_mapper)
                dim["levels"].append (c_lev)
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
            if (isinstance(mapper, FactMapper)):
                self._export_cube(ctx, model, mapper)
    
    def process(self, ctx, m):
        
        model = { 
                 "dimensions": [], 
                 "cubes": [] 
                 }
        
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
        
