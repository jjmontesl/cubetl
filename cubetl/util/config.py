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

        obj_list = ctx.components

        for e in obj_list:
            #if (not e.endswith('<anonymous>')):
            print("  %r" % (e))

        yield m

