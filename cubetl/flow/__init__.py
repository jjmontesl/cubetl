import logging
from cubetl.core import Node
import copy
from cubetl.text.functions import parsebool
from cubetl.script import Eval

# Get an instance of a logger
logger = logging.getLogger(__name__)


class Chain(Node):

    fork = False
    steps = None

    def __init__(self):
        super(Chain, self).__init__()
        self.steps = []

    def initialize(self, ctx):
        super(Chain, self).initialize(ctx)
        for p in self.steps:
            ctx.comp.initialize(p)

    def finalize(self, ctx):
        for p in self.steps:
            ctx.comp.finalize(p)
        super(Chain, self).finalize(ctx)

    def _process(self, steps, ctx, m):

        if (len(steps) <= 0):
            yield m
            return

        if ctx.debug2:
            logger.debug ("Processing step: %s" % (steps[0]))

        result_msgs = ctx.comp.process(steps[0], m)
        for m in result_msgs:
            result_msgs2 = self._process(steps[1:], ctx, m)
            for m2 in result_msgs2:
                yield m2

    def process(self, ctx, m):

        if (not self.fork):
            result_msgs = self._process(self.steps, ctx, m)
            for m in result_msgs:
                yield m
        else:
            logger.debug("Forking flow")
            m2 = ctx.copy_message(m)
            result_msgs = self._process(self.steps, ctx, m2)
            count = 0
            for mdis in result_msgs:
                count = count + 1

            logger.debug("Forked flow end - discarded %d messages" % count)
            yield m


class Filter(Node):

    condition = None
    message = None

    def process(self, ctx, m):

        if (self.condition == None):
            raise Exception("Filter node with no condition.")

        if (parsebool(ctx.interpolate(m, self.condition))):
            yield m
        else:
            if (self.message):
                logger.info(ctx.interpolate(m, self.message))
            elif (ctx.debug2):
                logger.debug("Filtering out message")
            return


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
        if (isinstance(pvalues, basestring)):
            pvalues = ctx.interpolate(m, self.values)
        if (isinstance(pvalues, basestring)):
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


