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
import os

from cubetl.core import Node
from cubetl.template.jinja import JinjaTemplateRenderer
from cubetl.util import Print
import slugify


# Get an instance of a logger
logger = logging.getLogger(__name__)


class PrintConfig(Node):

    def __init__(self, path=None):
        super().__init__()
        self.path = path
        self.truncate_line = None

    def initialize(self, ctx):
        super().initialize(ctx)
        self._print = Print(truncate_line=None)
        ctx.comp.initialize(self._print)

    def finalize(self, ctx):
        ctx.comp.finalize(self._print)
        super().finalize(ctx)

    def write_config(self, ctx, m):
        text = ""
        for k, e in ctx.components.items():
            #k = slugify.slugify(k, separator="_")
            item = "  ctx.add('%s',\n          %r)" % (k, e)
            #print()
            text += item + "\n"
        return text

    def process(self, ctx, m):

        res = self.write_config(ctx, m)

        # Write to file if so configured
        if self.path:
            with open(self.path, "w") as f:
                f.write(res)

        result_msgs = ctx.comp.process(self._print, res)
        # Consume message
        for _ in result_msgs:
            pass

        yield m


class ListConfig(Node):

    def __init__(self):
        super().__init__()

    def list_config(self, ctx, m):
        text = "\n"
        text += "List of nodes in CubETL configuration:\n"
        for k, e in ctx.components.items():
            if not isinstance(e, Node):
                continue
            item = "  * %s  %s" % (k, e.description if hasattr(e, 'description') and e.description else "")
            text += item + "\n"
        return text

    def process(self, ctx, m):
        res = self.list_config(ctx, m)
        print(res)
        yield m


class CreateTemplateConfig(Node):

    def __init__(self, config_name="myproject", config_path=None):
        super().__init__()
        self.config_name = config_name
        self.config_path = config_path
        self._template_renderer = None

    def initialize(self, ctx):
        super().initialize(ctx)
        self._template_renderer = JinjaTemplateRenderer(template=None)

    def process(self, ctx, m):

        template_path = os.path.dirname(__file__) + "/config.py.template"
        #logger.debug("Reading cubes config template from: %s", template_path)
        template_text = open(template_path).read()
        self._template_renderer.template = template_text

        m['config_name'] = ctx.interpolate(m, self.config_name)
        config_text = self._template_renderer.render(ctx, {'m': m})
        m['config_text'] = config_text
        print(config_text)

        config_path = ctx.interpolate(None, self.config_path)
        if config_path:
            logger.info("Writing Cubes server config to: %s", config_path)
            with open(config_path, "w") as f:
                f.write(config_text)

        yield m

