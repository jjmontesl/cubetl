import logging
from os import listdir
from os.path import isfile, join
import itertools
import re
from cubetl.core import Node
import chardet
from BeautifulSoup import UnicodeDammit
import os
from cubetl.fs import FileReader
import urllib2
import StringIO
import gzip


# Get an instance of a logger
logger = logging.getLogger(__name__)


class DecompressReader(Node):

    data = '${ m["data"] }'
    name = "data"

    def initialize(self, ctx):

        super(DecompressReader, self).initialize(ctx)

    def process(self, ctx, m):
        # TODO: Implement

        data = ctx.interpolate(m, self.data)
        compressedFile = StringIO.StringIO(data)
        decompressedFile = gzip.GzipFile(fileobj=compressedFile)

        m[self.name] = decompressedFile.read()

        yield m


