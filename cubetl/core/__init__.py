import logging
import cubetl
from copy import deepcopy

# Get an instance of a logger
logger = logging.getLogger(__name__)


class Component(object):
    """
    Base class for all components.
    """

    def get_id(self):

        if (hasattr(self, "id")):
            return self.id

        return None

    def initialize(self, ctx):
        pass

    def finalize(self, ctx):
        pass

    def __str__(self, *args, **kwargs):

        cid = self.get_id()
        if (not cid and hasattr(self, "name")):
            cid = self.name

        if (not cid):
            cid = id(self)
        else:
            # TODO: Only on debug
            #cid = cid + "/" + str(id(self))
            pass

        return "%s(%s)" % (self.__class__.__name__, cid)

        #return object.__str__(self, *args, **kwargs)


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
            value = ctx.interpolate(None, value)

            if attr not in ctx.props:
                logger.debug("Setting context property %s = %s" % (attr, value))
                ctx.props[attr] = value
            else:
                logger.debug("Not setting context property %s as it is already defined with value %s" % (attr, ctx.props[attr]))


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

