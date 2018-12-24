import logging

from cubetl.core import Node, Component
from mako import template


# Get an instance of a logger
logger = logging.getLogger(__name__)


'''
class MakoTemplateRenderer(TemplateRenderer):

    template = None

    _mako_template = None

    def initialize(self, ctx):

        super(MakoTemplate, self).initialize(ctx)
        self._mako_template = template.Template(filename = self.template)

    def finalize(self, ctx):
        super(MakoTemplate, self).initialize(ctx)

    def render(self, ctx, **kwargs):
        result = self._mako_template.render_unicode(ctx = ctx, **kwargs)
        return result

'''
