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


# Get an instance of a logger
logger = logging.getLogger(__name__)


class HttpReader(FileReader):

    url = None

    user_agent = None

    def initialize(self, ctx):

        self.path = self.url

        super(HttpReader, self).initialize(ctx)

        if (self.url == None):
            raise Exception("Missing url attribute for %s" % self)

    def process(self, ctx, m):

        # Resolve path
        url = ctx.interpolate(m, self.url)

        logger.debug("Requesting URL: %s" % url)

        headers = {}
        if self.user_agent:
            headers['User-Agent'] = self.user_agent

        req = urllib2.Request(url, headers=headers)
        response = urllib2.urlopen(req)
        html = response.read()
        m[self.name] = html

        # Encoding
        """
        encoding = ctx.interpolate(m, self.encoding)
        m[self.name] = self._solve_encoding(encoding, m[self.name])
        m["_encoding"] = encoding
        """

        yield m

