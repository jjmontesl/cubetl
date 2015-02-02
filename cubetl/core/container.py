import logging
import cubetl
import yaml
from copy import deepcopy
from cubetl.core import Component

# Get an instance of a logger
logger = logging.getLogger(__name__)


class YAMLRef(yaml.YAMLObject):

    #yaml_loader = Loader
    #yaml_dumper = Dumper

    yaml_tag = u'!ref'
    #yaml_flow_style = ...

    @classmethod
    def from_yaml(cls, loader, node):

        node_id = node.value
        #logger.debug("Loading reference: %s" % node.value)
        try:
            value = cubetl.container.get_component_by_id(node_id)
            #value = lambda: cubetl.container.get_component_by_id(node_id)
        except KeyError as e:
            raise Exception("Could not find referenced object '%s' at %s:%d" % (node_id, loader.name, loader.line))

        return value

    #@classmethod
    #def to_yaml(cls, dumper, data):
    #    return node


class Container(object):

    components = []

    def get_component_by_id(self, component_id):
        #logger.debug("Getting component: %s" % component_id)
        for comp in self.components:
            if (hasattr(comp, "id")):
                if (comp.id == component_id):
                    return comp

        raise KeyError("Component not found with id '%s'" % component_id)

    def add_component(self, component):
        if component == None:
            raise Exception('Tried to configure a null object')
        if not isinstance(component, Component):
            raise Exception('Tried to configure a non Component object: %s' % component)
        self.components.append(component)





