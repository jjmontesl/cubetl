import logging

from cubetl.core import Node
import slugify


# Get an instance of a logger
logger = logging.getLogger(__name__)


class PrintConfig(Node):

    type = None



    def process(self, ctx, m):
        text = ""
        for k, e in ctx.components.items():
            #k = slugify.slugify(k, separator="_")
            item = "  ctx.add('%s',\n          %r)" % (k, e)
            #print()
            text += item + "\n"

        #print()
        m['data'] = text

        yield m

