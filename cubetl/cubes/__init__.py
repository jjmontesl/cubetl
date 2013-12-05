import logging
import json
from cubetl.core import Node
from cubetl.olap.sql import SQLFactMapper, SQLEmbeddedDimensionMapper,\
    SQLFactDimensionMapper
from cubetl.olap import FactMapper, DimensionMapper, FactDimension

# Get an instance of a logger
logger = logging.getLogger(__name__)

class CubesModelWriter(Node):
    
    def __init__(self):
        
        super(CubesModelWriter, self).__init__()
        
        self.olapmapper = None
    
    def initialize(self, ctx):
        
        super(CubesModelWriter, self).initialize(ctx)
        ctx.comp.initialize(self.olapmapper)

    def finalize(self, ctx):
        ctx.comp.finalize(self.olapmapper)
        super(CubesModelWriter, self).initialize(ctx)

    def _get_dimensions(self, fact):
        result = []
        for dim in fact.dimensions:
            if (isinstance(dim, FactDimension)):
                result.extend(self._get_dimensions(dim.fact))
            else:
                result.append(dim.name)
                
        return result

    def _get_joins(self, ctx, fact_mapper):
        
        joins = []
        
        for dim in fact_mapper.fact.dimensions:
            fieldmapping = [ mapping for mapping in fact_mapper.mappings if mapping["name"] == dim.name][0]
            dimmapper = self.olapmapper.getDimensionMapper(dim)
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
                
        return joins

    def _get_mappings(self, ctx, fact_mapper):
        mappings = {}
        for dim in fact_mapper.fact.dimensions:
            dimmapper = self.olapmapper.getDimensionMapper(dim)
            if (not isinstance(dimmapper, SQLEmbeddedDimensionMapper)):
                for mapping in dimmapper.mappings:
                    if (len(dimmapper.mappings) == 1):
                        mappings[dim.name] = dimmapper.table + "." + mapping["column"]
                    else:
                        mappings[dim.name + "." + mapping["name"]] = dimmapper.table + "." + mapping["column"]
            else:
                if (isinstance(dimmapper, SQLFactDimensionMapper)):
                    mappings.update(self._get_mappings(ctx, self.olapmapper.getFactMapper(dimmapper.dimension.fact)))
                else:
                    for mapping in dimmapper.mappings:
                        if (len(dimmapper.mappings) == 1):
                            mappings[dim.name] = fact_mapper.table + "." + mapping["column"]
                        else:
                            mappings[dim.name + "." + mapping["name"]] = fact_mapper.table + "." + mapping["column"]
                        
                    
        return mappings
         
        

    def _export_cube(self, ctx, model, mapper):
        
        logger.debug ("Exporting cube %s" % (mapper.fact.name))
        
        cube = {}
        cube["name"] = mapper.fact.name
        cube["label"] = mapper.fact.name
        cube["key"] = mapper.pk(ctx)["name"]

        # Add dimensions        
        cube["dimensions"] = self._get_dimensions(mapper.fact)
        
        # Add measures
        cube["measures"] = []
        for measure in mapper.fact.measures:
            c_measure = {}
            c_measure["name"] = measure["name"]
            c_measure["aggregations"] = ["sum", "avg", "max", "min"]
            cube["measures"].append(c_measure)
        
        # Joins
        cube["joins"] = self._get_joins(ctx, mapper)

        # Details
        cube["details"] = []
        for attribute in mapper.fact.attributes:
            cube["details"].append(attribute["name"])
            
        # Mappings
        cube["mappings"] = self._get_mappings(ctx, mapper)
        
        model["cubes"].append(cube)

    def _export_dimension(self, ctx, model, mapper):
        
        logger.debug ("Exporting dimension %s" % (mapper.dimension.name))
        
        dim = {}
        dim["comment"] = "Generated by CubETL"
        dim["name"] = mapper.dimension.name 
        dim["label"] = mapper.dimension.label
        dim["levels"] = []

        # Attributes are levels 
        levels_map = {}
        for attrib in mapper.dimension.attributes:
            level = {}
            level["name"] = attrib["name"]
            level["label"] = attrib["label"]
            if (isinstance(mapper, SQLEmbeddedDimensionMapper)):
                level["attributes"] = [ attrib["name"] ]
                level["key"] = attrib["name"]
                #level["key"] = mapper.pk(ctx)["name"]
            else:
                if (len(mapper.dimension.attributes) > 1):
                    level["attributes"] = [ attrib["name"] ]
                    level["key"] = attrib["name"]
                else:
                    level["attributes"] = [ mapper.pk(ctx)["name"], attrib["name"] ]
                    level["key"] = mapper.pk(ctx)["name"]
            level["label_attribute"] = attrib["name"]
            
            # Cubesviewer dates
            if ("role" in attrib):
                if (attrib["role"] == "year"): level["info"] = { "cv-datefilter-field": "year" }
                elif (attrib["role"] == "quarter"): level["info"] = { "cv-datefilter-field": "quarter" }
                elif (attrib["role"] == "month"): level["info"] = { "cv-datefilter-field": "month" }
                elif (attrib["role"] == "week"): level["info"] = { "cv-datefilter-field": "week" }
                elif (attrib["role"] == "day"): level["info"] = { "cv-datefilter-field": "day" }
            
            dim["levels"].append(level)
            levels_map[level["name"]] = level
            
        # Hierarchies
        finest_hierarchy = None
        if (len(mapper.dimension.hierarchies) > 0):
            dim["hierarchies"] = []
            for hierarchy in mapper.dimension.hierarchies:

                # Calculate levels and finest level
                for lev in hierarchy["levels"]:
                    if (not lev in levels_map):
                        raise Exception("Hierarchy level '%s' not found in attributes defined for dimension '%s'" % (lev, mapper.dimension.name))
                
                if ((finest_hierarchy == None) or (len(hierarchy["levels"]) > len(finest_hierarchy["levels"]))):
                    finest_hierarchy = hierarchy
                    
                # Define hierarchy
                chierarchy = {
                              "name": hierarchy["name"],
                              "label": hierarchy["label"] if ("label" in hierarchy) else hierarchy["name"],
                              "levels": [ lev for lev in hierarchy["levels"] ] 
                              }
                dim["hierarchies"].append(chierarchy)
        
        # Add cubesviewer datefilter info
        if (mapper.dimension.role == "date"):
            dim["info"] = {
                           "cv-datefilter": True,
                           "cv-datefilter-hierarchy": finest_hierarchy["name"]
                           }
        
        model["dimensions"].append(dim)

    def _exportmapper(self, ctx, model, mapper):
        if (isinstance(mapper, DimensionMapper)):
            self._export_dimension(ctx, model, mapper)
        if (isinstance(mapper, FactMapper)):
            self._export_cube(ctx, model, mapper)
            
    def _exportolapmapper(self, ctx, model, olapmapper):
        
        # Call includes
        for inc in olapmapper.include:
            self._exportolapmapper(ctx, model, inc)

        # Export mappers
        for mapper in olapmapper.mappers:
            self._exportmapper(ctx, model, mapper)
        
    
    def process(self, ctx, m):
        
        model = { 
                 "dimensions": [], 
                 "cubes": [] 
                 }
        
        self._exportolapmapper(ctx, model, self.olapmapper)
        
        # Prepare result
        m["cubesmodel"] = model
        m["cubesmodel_json"] = json.dumps(model, indent=4, sort_keys=True)
        print m["cubesmodel_json"]
        
        
        yield m
        
        