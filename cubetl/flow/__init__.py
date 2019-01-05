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


import copy
import logging

from cubetl.core import Node
from cubetl.core.exceptions import ETLConfigurationException
from cubetl.script import Eval
from cubetl.text.functions import parsebool


# Get an instance of a logger
logger = logging.getLogger(__name__)


class Chain(Node):

    def __init__(self, steps, fork=False, condition=None):
        super().__init__()
        self.steps = steps or []
        self.fork = fork
        self.condition = condition

    def initialize(self, ctx):
        super().initialize(ctx)
        for p in self.steps:
            if p is None:
                raise ETLConfigurationException("Component %s steps contain a None reference." % self)
            ctx.comp.initialize(p)

    def finalize(self, ctx):
        for p in self.steps:
            ctx.comp.finalize(p)
        super().finalize(ctx)

    def _process(self, steps, ctx, m):

        if (len(steps) <= 0):
            yield m
            return

        if ctx.debug2:
            logger.debug("Processing step: %s" % (steps[0]))

        result_msgs = ctx.comp.process(steps[0], m)
        for m in result_msgs:
            result_msgs2 = self._process(steps[1:], ctx, m)
            for m2 in result_msgs2:
                yield m2

    def process(self, ctx, m):

        cond = True
        if self.condition:
            cond = parsebool(ctx.interpolate(m, self.condition))

        if cond:
            if (not self.fork):
                result_msgs = self._process(self.steps, ctx, m)
                for m in result_msgs:
                    yield m
            else:
                logger.debug("Forking flow (copying message).")
                m2 = ctx.copy_message(m)
                result_msgs = self._process(self.steps, ctx, m2)
                count = 0
                for mdis in result_msgs:
                    count = count + 1

                logger.debug("Forked flow end - discarded %d messages" % count)
                yield m
        else:
            yield m


class Filter(Node):

    def __init__(self, condition, message=None):
        super().__init__()
        self.condition = condition
        self.message = message

    def process(self, ctx, m):

        if (parsebool(ctx.interpolate(m, self.condition))):
            yield m
        else:
            if (self.message):
                logger.info(ctx.interpolate(m, self.message))
            elif (ctx.debug2):
                logger.debug("Filtering out message")
            return


class Skip(Node):

    def __init__(self, skip):
        super().__init__()
        self.skip = skip
        self._next_skip = 0

    def initialize(self, ctx):
        super().initialize(ctx)
        self.counter = 0
        self._next_skip = 0

    def process(self, ctx, m):

        self.counter += 1
        if self.counter < self._next_skip:
            # Skip message
            return

        self._next_skip = int(ctx.interpolate(m, self.skip))
        self.counter = 0

        yield m


class Limit(Node):

    def __init__(self, limit):
        super().__init__()
        self.limit = limit
        self.counter = 0

    def initialize(self, ctx):
        super().initialize(ctx)
        self.counter = 0

    def process(self, ctx, m):

        self.counter += 1
        limit = int(ctx.interpolate(m, self.limit))

        if self.counter > limit:
            # Skip message
            # TODO: We shall actually break the process flow, signalling backwards (return or yield some constant?)
            return

        yield m


class Multiplier(Node):
    """
    The multiplier node produces several messages for each incoming message received.

    The incoming message is copied before assigning the value to the attribute.

    | name | Name of the attribute that will be created in the message.
    | values | A list or comma-separated-string of values to be assigned.
    """

    name = None
    values = None

    def initialize(self, ctx):
        super(Multiplier, self).initialize(ctx)

        if (self.name == None):
            raise Exception("Iterator field 'name' not set in node %s" % (self))

        if (self.values == None):
            raise Exception("Iterator field 'values' not set in node %s" % (self))

    def process(self, ctx, m):

        pvalues = self.values
        if (isinstance(pvalues, str)):
            pvalues = ctx.interpolate(m, self.values)
        if (isinstance(pvalues, str)):
            pvalues = [ v.strip() for v in pvalues.split(",") ]
        for val in pvalues:
            # Copy message and set value
            if (ctx.debug2):
                logger.debug("Multiplying: %s = %s" % (self.name, val))
            m2 = ctx.copy_message(m)
            m2[self.name] = val
            yield m2


"""
class Iterator(Node):

    name = None
    values = None
    node = None

    def __init__(self):

        super(Iterator, self).__init__()

    def initialize(self, ctx):
        super(Iterator, self).initialize(ctx)
        ctx.comp.initialize(self.node)

    def finalize(self, ctx):
        ctx.comp.finalize(self.node)
        super(Iterator, self).finalize(ctx)

    def process(self, ctx, m):

        pvalues = self.values
        if (isinstance(pvalues, basestring)):
            pvalues = ctx.interpolate(m, self.values)
        if (isinstance(pvalues, basestring)):
            pvalues = [ v.strip() for v in pvalues.split(",") ]

        mes = m
        for val in pvalues:

            if (ctx.debug2):
                logger.debug("Iterating %s = %s" % (self.name, val))

            # Message is not copied as we are iterating over the same message
            mes[self.name] = val

            result_msgs = ctx.comp.process(self.node, mes)
            result_msgs = list(result_msgs)
            if len(result_msgs) != 1:
                logger.error("No message or more than one message obtained from Iterator node %s (%d messages received)" % (self, len(result_msgs)))
            mes = result_msgs[0]

        yield mes
"""


class Union(Node):

    steps = None

    def __init__(self):
        super(Union, self).__init__()
        self.steps = []

    def initialize(self, ctx):
        super(Union, self).initialize(ctx)
        for p in self.steps:
            ctx.comp.initialize(p)

    def finalize(self, ctx):
        for p in self.steps:
            ctx.comp.finalize(p)
        super(Union, self).finalize(ctx)

    def process(self, ctx, m):

        if (len(self.steps) <= 0):
            raise Exception("Union with no steps.")

        for step in self.steps:
            m2 = ctx.copy_message(m)
            result_msgs = ctx.comp.process(step, m2)
            for m3 in result_msgs:
                yield m3


