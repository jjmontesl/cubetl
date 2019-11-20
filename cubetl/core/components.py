# CubETL
# Copyright (c) 2013-2019 Jose Juan Montes

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import logging

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
        if comp not in self.components:
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
            logger.warning("Finalized a non initialized component: %s" % comp)
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
                logger.warning("Unfinalized component %s" % comp_desc.comp)

