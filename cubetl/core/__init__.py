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
import cubetl
from copy import deepcopy
from cubetl.core.exceptions import ETLConfigurationException
import inspect

# Get an instance of a logger
logger = logging.getLogger(__name__)


class Component(object):
    """
    Base class for all components.
    """

    def __init__(self):
        self.ctx = None
        self.urn = None
        self.description = None

    def initialize(self, ctx):
        if hasattr(self, '_initialized'):
            raise ETLConfigurationException("Component already initialized: %s" % self)
        self._initialized = True
        self.ctx = ctx

    def finalize(self, ctx):
        #self.ctx = None
        self._initialized = False

    def __str__(self, *args, **kwargs):
        return "%s(%s)" % (self.__class__.__name__, self.urn)

    def __repr__(self):
        args = []
        signature = inspect.signature(self.__init__)  # ArgSpec(args=['self', 'name'], varargs=None, keywords=None, defaults=None)
        #print(argspec)
        for key in signature.parameters:
            if key == "self": continue
            value = getattr(self, key) if hasattr(self, key) else None

            if value and isinstance(value, Component) and value.urn:
                value = "ctx.get('%s')" % value.urn
            elif callable(value):
                if value.__code__.co_name:
                    value = value.__code__.co_name
                else:
                    value = inspect.getsource(value)
            else:
                value = "%r" % (value)

            args.append((key, value))  # TODO: get default?
        '''
        if argspec.keywords:
            for key in argspec.keywords:
                print(argspec.keywords)
                print(argspec.defaults)
                if not hasattr(self, key): continue
                args.append((key, getattr(self, key)))  # TODO: get default?
        '''
        return "%s(%s)" % (self.__class__.__name__, ", ".join(["%s=%s" % (key, value) for key, value in args]))


class Node(Component):
    """
    Base class for all control flow nodes.

    These must implement a process(ctx, m) method that
    accepts and yield messages.
    """

    def process(self, ctx, m):

        yield m


class ContextProperties(Component):

    #def after_properties_set(self):

    def load_properties(self, ctx):

        for attr in self.__dict__:

            if (attr == "id"):
                continue

            value = getattr(self, attr)
            value = ctx.interpolate(value)

            if attr not in ctx.props:
                logger.debug("Setting context property %s = %s" % (attr, value))
                ctx.props[attr] = value
            else:
                logger.debug("Not setting context property %s as it is already defined with value %s" % (attr, ctx.props[attr]))

'''
class Mappings(Component):
    """
    Serves as a holder for mappings, which can be included from other mappings.

    This component tries to make mappings more reusable, by providing a way to reference
    them.
    """

    mappings = None

    def initialize(self, ctx):

        #raise Exception("Mappings initialize method cannot be called.")

        super(Mappings, self).initialize(ctx)
        Mappings.includes(ctx, self.mappings)

    def finalize(self, ctx):

        #raise Exception("Mappings finalize method cannot be called.")
        super(Mappings, self).finalize(ctx)


    @staticmethod
    def includes(ctx, mappings):

        mapping = True
        while mapping != None:
            pos = 0
            mapping = None
            for m in mappings:
                if (isinstance(m, Mappings)):
                    mapping = m
                    break
                else:
                    pos = pos + 1

            if (mapping):
                # It's critical to copy mappings
                ctx.comp.initialize(mapping)
                mappings[pos:pos + 1] = deepcopy(mapping.mappings)

'''

