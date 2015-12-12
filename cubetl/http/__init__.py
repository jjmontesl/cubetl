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
    attempts = 3

    def initialize(self, ctx):

        self.path = self.url

        super(HttpReader, self).initialize(ctx)

        if (self.url == None):
            raise Exception("Missing url attribute for %s" % self)

    def process(self, ctx, m):

        # Resolve path
        url = ctx.interpolate(m, self.url)

        logger.debug("Requesting URL: %r" % url)

        headers = {}
        if self.user_agent:
            headers['User-Agent'] = self.user_agent

        req = urllib2.Request(url, headers=headers)

        attempt_count = 0
        html = None
        while attempt_count < self.attempts:
            try:
                attempt_count += 1
                response = urllib2.urlopen(req)
                html = response.read()
            except Exception as e:
                logger.warn("Could not retrieve HTTP document (attempt %d/%d): %s " % (attempt_count, self.attempts, e))

        # Encoding
        """
        encoding = ctx.interpolate(m, self.encoding)
        m[self.name] = self._solve_encoding(encoding, m[self.name])
        m["_encoding"] = encoding
        """

        if html is not None:
            m[self.name] = html
            yield m
        else:
            logger.error("Coudl not retrieve HTTP document in %d attempts (blocking message)." % (self.attempts))


