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

    type = None

    def process(self, ctx, m):

        obj_list = cubetl.container.components
        #obj_list = cubetl.container.object_defs.keys()

        obj_list.sort(key=lambda x: x.id)
        for e in obj_list:
            #if (not e.endswith('<anonymous>')):
            print("  %s (%s)" % (e.id, e.__class__.__name__))

        yield m

