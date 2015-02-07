import logging
from cubetl.core import Node, Component
#from cubetl.olap.sql import FactMapper

# Get an instance of a logger
logger = logging.getLogger(__name__)


class Dimension(Component):
    """A flat dimension.

    Note: This represents a Flat dimension (no hierarchies, only one level with attributes).
    """

    name = None
    label = None
    attributes = []
    role = None

    def __init__(self):
        super(Dimension, self).__init__()
        self.attributes = []

    def initialize(self, ctx):

        super(Dimension, self).initialize(ctx)

        # FIXME: Added because PyYAML didn't call init
        if self.attributes == []: self.attributes = []

        if (self.label == None):
            self.label = self.name

        if len(self.attributes) == 0 and type(self) is Dimension:
            # If attributes are not defined, this is a shortcut for simple dimensions
            attr = { "name": self.name, "label": self.label, "type": "String" }
            logger.debug("Automatically adding a default String attribute to dimension %s" % self)
            self.attributes.append(attr)
            #raise Exception("Dimension has no attributes: %s" % self)
            pass
        else:
            for attr in self.attributes:
                if (not "label" in attr):
                    if ((len(self.attributes) == 1) and (attr["name"] == self.name)):
                        attr["label"] = self.label
                    else:
                        if (not "name" in attr):
                            raise Exception("Attribute '%s' of %s has no 'name' attribute" % (attr, self))
                        attr["label"] = attr["name"]


    """
    def has_attribute(self, search):
        return (search in [attr["name"] for attr in self.attributes])

    def attribute(self, search):
        att = [attr for attr in self.attributes if attr["name"] == search]

        if (len(att) > 1):
            raise Exception("More than one attribute with name '%s' found in dimension %s" % (search, self.name))
        if (len(att) == 0):
            raise Exception("Could not find attribute '%s' in dimension %s" % (search, self.name))

        return att[0]
    """


class HierarchyDimension(Dimension):
    """A non-flat dimension, forming one or more hierarchies.

    References subdimensions (levels), usually forming hierarchies.
    """

    levels = None
    hierarchies = None

    def __init__(self):
        super(HierarchyDimension, self).__init__()
        self.levels = []
        self.hierarchies = []

    def initialize(self, ctx):
        super(HierarchyDimension, self).initialize(ctx)

        # FIXME: Added because PyYAML didn't call init
        if (self.levels == None): self.levels = []
        if (self.hierarchies == None): self.hierarchies = []

        if (len(self.attributes) > 0):
            raise Exception ("%s is a HierarchyDimension and cannot have attributes." % (self))

        for hie in self.hierarchies:
            if (isinstance(hie["levels"], basestring)):
                levels = []
                for lev_name in hie["levels"].split(","):
                    lev_name = lev_name.strip()
                    level = [lev for lev in self.levels if lev.name == lev_name]
                    if (len(level) != 1):
                        raise Exception ("Level %s defined in hierarchy %s is undefined." % (lev_name, hie))
                    levels.append(level[0])
                hie["levels"] = levels


class AliasDimension(Dimension):

    dimension = None
    role = None

    def initialize(self, ctx):

        #super(Dimension, self).initialize(ctx)

        ctx.comp.initialize(self.dimension)

        if (self.name == None):
            self.name = self.dimension.name
        if (self.role == None):
            self.role = self.dimension.role

        if (self.attributes):
            raise Exception("%s cannot define own attributes because it is an AliasDimension, and attributes are aliased from the referenced dimension." % self)

    def finalize(self, ctx):
        ctx.comp.finalize(self.dimension)
        #super(AliasDimension, self).finalize(ctx)

    """
    def has_attribute(self, search):
        return self.dimension.has_attribute(search)

    def attribute(self, search):
        return self.dimension.attribute(self, search)
    """

    def __getattr__(self, attr):
        if (attr in ["label", "name", "dimension", "role", "initialize", "finalize"]):
            return super(AliasDimension, self).__getattr__(attr)
        else:
            return getattr(self.dimension, attr)

    def __setattr__(self, attr, value):
        if (attr in ["label", "name", "dimension", "role", "initialize", "finalize"]):
            return super(AliasDimension, self).__setattr__(attr, value)
        else:
            return setattr(self.dimension, attr, value)


class Fact(Component):

    name = None
    label = None

    dimensions = None
    attributes = None
    measures = None

    def __init__(self):
        super(Fact, self).__init__()
        self.dimensions = []
        self.attributes = []
        self.measures = []

    def initialize(self, ctx):

        super(Fact, self).initialize(ctx)

        if (self.attributes == None):
            self.attributes = []

        if (self.label == None):
            self.label = self.name
        for attr in self.attributes:
            if (not "label" in attr):
                attr["label"] = attr["name"]

        for measure in self.measures:
            if (not "label" in measure):
                measure["label"] = measure["name"]


class FactDimension(Dimension):

    fact = None

    dimensions = None
    attributes = None
    measures = None

    def __init__(self):
        super(Fact, self).__init__()
        self.dimensions = []
        self.attributes = []
        self.measures = []

    def finalize(self, ctx):
        ctx.comp.finalize(self.fact)
        super(FactDimension, self).finalize(ctx)

    def initialize(self, ctx):

        super(FactDimension, self).initialize(ctx)
        ctx.comp.initialize(self.fact)

        if (len(self.attributes) > 0):
            raise Exception("Cannot define attributes for a FactDimension (it's defined by the linked fact)")

        self.dimensions = self.fact.dimensions
        self.measures = self.fact.measures
        self.attributes = self.fact.attributes


    """
    def attribute(self, search):
        att = [attr for attr in self.fact.attributes if attr["name"] == search]
        if (len(att) != 1):
            raise Exception("Could not find attribute %s in fact dimension %s" % (search, self.name))

        return att[0]
    """




class OlapMapper(Component):

    mappers = []
    include = []

    def initialize(self, ctx):

        super(OlapMapper, self).initialize(ctx)

        for incl in self.include:
            ctx.comp.initialize(incl)
        for mapper in self.mappers:
            # TODO: FIXME: if we do this, mappers shall be "prototype", in case there are several references
            mapper.olapmapper = self
            ctx.comp.initialize(mapper)

    def finalize(self, ctx):
        for incl in self.include:
            ctx.comp.finalize(incl)
        for mapper in self.mappers:
            ctx.comp.finalize(mapper)
        super(OlapMapper, self).finalize(ctx)


    def entity_mapper(self, entity, fail = True):
        """Returns the OlapMapper that handles a fact or dimension.

        Included mappers are processed after local ones, so mapping
        definitions for different entities can be overrided.
        """

        for mapper in self.mappers:
            #if (mapper.entity.name == entity.name):
            if (mapper.entity == entity) or (mapper.entity == entity):
                return mapper

        for inc in self.include:
            mapper = inc.entity_mapper(entity, False)
            if (mapper):
                return mapper

        if fail:
            raise Exception("No OLAP mapper found for: %s" % entity.name)

        return None


class Store(Node):

    def initialize(self, ctx):
        super(Store, self).initialize(ctx)
        ctx.comp.initialize(self.mapper)

    def finalize(self, ctx):
        ctx.comp.finalize(self.mapper)
        super(Store, self).finalize(ctx)

    def process(self, ctx, m):

        logger.debug ("Storing entity %s" % (self.entity.name))

        # Store
        # TODO: We shall not collect the ID here possibly
        fid = self.mapper.entity_mapper(self.entity).store(ctx, m)
        if (fid != None): m[self.entity.name + "_id"] = fid

        yield m

