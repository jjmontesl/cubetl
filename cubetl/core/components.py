
import cubetl
import logging

import sys
import traceback

from repoze.lru import LRUCache
from collections import namedtuple
from cubetl.core.exceptions import ETLConfigurationException

# Get an instance of a logger
logger = logging.getLogger(__name__)


class ComponentDescriptor():
    pass


class Components():

    def __init__(self, context):

        self.components = {}

        self.ctx = context

    def component_desc(self, comp):
        if (not comp in self.components):
            self.components[comp] = ComponentDescriptor()
            self.components[comp].comp = comp
            self.components[comp].initialized = False
            self.components[comp].finalized = False

        return self.components[comp]

    def is_initialized(self, comp):
        return self.component_desc(comp).initialized

    def is_finalized(self, comp):
        return self.component_desc(comp).finalized

    def initialize(self, comp):
        if (not self.is_initialized(comp)):
            logger.debug("Initializing %s" % (comp, ))
            self.component_desc(comp).initialized = True
            comp.initialize(self.ctx)
            try:
                pass
            except AttributeError as e:
                raise ETLConfigurationException("Tried to initialize invalid component (%s): %s" % (comp, e))


    def finalize(self, comp):

        # TODO: Count references and finalize in an adequate order!!!

        if (not self.is_initialized(comp)):
            logger.warn("Finalized a non initialized component: %s" % comp)
        if (not self.is_finalized(comp)):
            logger.debug("Finalizing %s" % comp)
            self.component_desc(comp).finalized = True
            comp.finalize(self.ctx)

    def process(self, comp, m):
        if (not self.is_initialized(comp)):
            raise Exception("Sent message to a non initialized component: %s" % comp)
        if (self.is_finalized(comp)):
            raise Exception("Message to a finalized component: %s" % comp)
        return comp.process(self.ctx, m)

    def cleanup(self):
        for comp_desc in self.components.values():
            if (not comp_desc.finalized):
                logger.warn("Unfinalized component %s" % comp_desc.comp)

