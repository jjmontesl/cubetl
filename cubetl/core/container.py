import logging
import cubetl
import yaml
from cubetl.core import Component
import os.path

# Get an instance of a logger
logger = logging.getLogger(__name__)


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

        # Search if it exists already
        if (hasattr(component, "id")):
            try:
                if self.get_component_by_id(component.id) != None:
                    raise Exception("Tried to define an already existing id: " % component.id)
            except:
                pass


        self.components.append(component)





