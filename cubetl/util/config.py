import logging
from os import listdir
from os.path import isfile, join
import itertools
import re
from cubetl.core import Node, Component
import time
from cubetl.text.functions import parsebool
import cubetl

# Get an instance of a logger
logger = logging.getLogger(__name__)

class PrintConfig(Node):

    def __init__(self):

        self.type = None

    def process(self, ctx, m):

        if (self.type == None):
            class_parent = object
        elif (self.type == "components"):
            class_parent = Component
        elif (self.type == "nodes"):
            class_parent = Node
        else:
            logger.error("Invalid PrintConfig object type '%s' (valid values are None, components, nodes)" % self.type)

        obj_list = cubetl.container.get_objects_by_type(class_parent).keys()
        #obj_list = cubetl.container.object_defs.keys()

        obj_list.sort()
        for e in obj_list:
            if (not e.endswith('<anonymous>')):
                print "  %s" % (e)

        yield m

