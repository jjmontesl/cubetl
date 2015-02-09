
import cubetl
import logging
import os
import yaml
import pprint

# Get an instance of a logger
logger = logging.getLogger(__name__)


# Global:( reference to CubETL context
_ctx = None


class YAMLRef(yaml.YAMLObject):

    #yaml_loader = Loader
    #yaml_dumper = Dumper

    yaml_tag = u'!ref'
    #yaml_flow_style = ...

    @classmethod
    def from_yaml(cls, loader, node):

        node_id = node.value
        #logger.debug("Loading reference: %s" % node.value)
        try:
            value = cubetl.container.get_component_by_id(node_id)
            #value = lambda: cubetl.container.get_component_by_id(node_id)
        except KeyError as e:
            raise Exception("Could not find referenced object '%s' at %s:%d" % (node_id, loader.name, loader.line))

        return value

    #@classmethod
    #def to_yaml(cls, dumper, data):
    #    return node



class YAMLIncludeLoader(yaml.Loader):

    def __init__(self, stream):

        #self._root = os.path.split(stream.name)[0]

        super(YAMLIncludeLoader, self).__init__(stream)

    def include(self, node):

        #filename = os.path.join(self._root, self.construct_scalar(node))
        filename = self.construct_scalar(node)
        load_config(_ctx, filename)

YAMLIncludeLoader.add_constructor('!include', YAMLIncludeLoader.include)


def load_config(ctx, filename):

    global _ctx
    _ctx = ctx

    logger.debug("Loading config file %s" % filename)

    file_exp = ctx.interpolate(None, filename)
    stream = open(file_exp, 'r')

    comps = yaml.load_all(stream, YAMLIncludeLoader)
    for comp in comps:
        try:
            if comp != None:
                cubetl.container.add_component(comp)
        except Exception as e:
            raise Exception("Could not load config %s: %s" % (file_exp, e))

