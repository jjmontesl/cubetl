import logging
import json
from cubetl.core import Node, Component
from cubetl.olap.sql import SQLFactMapper, SQLEmbeddedDimensionMapper,\
    SQLFactDimensionMapper
from cubetl.olap import FactMapper, DimensionMapper, FactDimension
from cubetl.templates import Template
import os
import cgi
import re
import inspect
from cubetl.core.context import Context

# Get an instance of a logger
logger = logging.getLogger(__name__)

class JsGraphIt(Component):
    
    def __init__(self):
        
        super(JsGraphIt, self).__init__()
    
        self.nodes = []
        self.connectors = []
        
        self.template = os.path.dirname(os.path.realpath(__file__)) + "/templates/jsgraph-default.html"
        
    def initialize(self, ctx):
        
        super(JsGraphIt, self).initialize(ctx)
        self._template = Template()
        self._template.template = self.template
        ctx.comp.initialize(self._template)

    def finalize(self, ctx):
        ctx.comp.finalize(self._template)
        super(JsGraphIt, self).finalize(ctx)
        
    def add_node (self, ctx, oid, title, html):
        self.nodes.append({ 
                            "id": oid,
                            "title": title,
                            "html": html
                          })

    def add_connector (self, ctx, from_id, to_id):
        self.connectors.append({
                                "from_id": from_id,
                                "to_id": to_id
                                })
    
    def render(self, ctx):
        return self._template.render(ctx, nodes = self.nodes, connectors = self.connectors)


class CubETLObjectGraph(Node):
    
    def __init__(self):
        
        super(CubETLObjectGraph, self).__init__()
        
        self._jsgraphit = None
        self._visited_nodes = []
        
        self.filter_class = [ Component, Context ]
        self.filter_class_regexp = 'cubetl.*' 
    
    def initialize(self, ctx):
        
        super(CubETLObjectGraph, self).initialize(ctx)
        
        self._jsgraphit = JsGraphIt()
        ctx.comp.initialize(self._jsgraphit)

    def finalize(self, ctx):
        ctx.comp.finalize(self._jsgraphit)
        super(CubETLObjectGraph, self).finalize(ctx)
    
    def _add_object(self, ctx, o):
        
        if (o == None):
            return ("val", "None")
        elif (isinstance(o, float)):
            return ("val", str(float))
        elif (isinstance(o, int)):
            return ("val", str(o))
        elif (isinstance(o, basestring)):
            return ("val", cgi.escape( o[:20] ) )
    
        elif (isinstance(o, dict)):
            
            if (len(o.keys()) == 0): return ("val", "Dict (Empty)")
            if (o in self._visited_nodes): return ("rel", o)
            self._visited_nodes.append(o)
            
            tattributes = []        
            for attr in o.keys():
                value = o[attr]
                ao = self._add_object(ctx, value)
                
                if (ao[0] == "val"):
                    tattributes.append ("<b>" + str(attr) + "</b>: " + ao[1])
                elif (ao[0] == "rel"):
                    tattributes.append ("<b>" + str(attr) + "</b>: rel " + str(id(ao[1])))
                    self._jsgraphit.add_connector(ctx, id(o), id(value))
                    
            title = "Dict"
            self._jsgraphit.add_node(ctx, id(o), title, "<br />\n".join(tattributes))
            return ("rel", o)
        
        
        elif (isinstance(o, list)):
            return ("val", "LIST")
    
        elif (inspect.ismodule(o)):
            return ("val", str(o))
        
        elif (isinstance(o, object)):
            
            valid_class = False
            for parent_class in self.filter_class:
                if (isinstance(o, parent_class)): valid_class = True

            valid_class_name = False            
            class_name = o.__module__ + "." + o.__class__.__name__
            regex = re.compile(self.filter_class_regexp)
            if (regex.match(class_name)): valid_class_name = True
            
            if (not (valid_class and valid_class_name)):
                
                return ("val", "class " + class_name)
            
            else:
                
                if (o in self._visited_nodes): return ("rel", o)
                self._visited_nodes.append(o)
                
                tattributes = []        
                for attr in o.__dict__.keys():
                    value = o.__dict__[attr]
                    ao = self._add_object(ctx, value)
                    
                    if (ao[0] == "val"):
                        tattributes.append ("<b>" + str(attr) + "</b>: " + ao[1])
                    elif (ao[0] == "rel"):
                        tattributes.append ("<b>" + str(attr) + "</b>: rel " + str(id(ao[1])))
                        self._jsgraphit.add_connector(ctx, id(o), id(value))
                        
                title = o.__class__.__name__
                self._jsgraphit.add_node(ctx, id(o), title, "<br />\n".join(tattributes))
                return ("rel", o)
                    
    def process(self, ctx, m):
        
        self._add_object(ctx, m["obj"])
        
        yield m
        
        

class CubETLContextGraph(Node):
    
    def __init__(self):
        
        super(CubETLContextGraph, self).__init__()
        
        self._cubetl_object_graph = None
    
    def initialize(self, ctx):
        
        super(CubETLContextGraph, self).initialize(ctx)
        
        self._cubetl_object_graph = CubETLObjectGraph()
        ctx.comp.initialize(self._cubetl_object_graph)

    def finalize(self, ctx):
        ctx.comp.finalize(self._cubetl_object_graph)
        super(CubETLContextGraph, self).finalize(ctx)
    
    def process(self, ctx, m):
        
        objects = [ ]
        objects.append (ctx)
        objects.extend ([comp for comp in ctx.comp.components.keys() ])
        
        for o in objects:
            res = ctx.comp.process(self._cubetl_object_graph, {"obj": o })
            for m2 in res:
                pass
        
        print self._cubetl_object_graph._jsgraphit.render(ctx).encode("utf-8")
        
        yield
        
        