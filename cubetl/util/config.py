import logging

from cubetl.core import Node
import slugify
from cubetl.util import Print


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

