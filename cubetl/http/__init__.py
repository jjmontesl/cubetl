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


import itertools
import logging
import os

from cubetl import APP_NAME_VERSION
from cubetl.core import Node
from cubetl.fs import FileReader
import chardet
import re


# Get an instance of a logger
logger = logging.getLogger(__name__)


class HttpReader(FileReader):
    """
    Makes an HTTP request and reads the body of a URL, returning the result as a message attribute.

    .. code-block:: javascript

        - !!python/object:cubetl.http.HttpReader
          url: "http://www.cubesviewer.com/"
          user_agent: "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:35.0) Gecko/20100101 Firefox/35.0"
    """

    url = None

    user_agent = APP_NAME_VERSION
    attempts = 3

    def initialize(self, ctx):

        self.path = self.url

        super(HttpReader, self).initialize(ctx)

        if (self.url == None):
            raise Exception("Missing url attribute for %s" % self)

    def process(self, ctx, m):

        # Resolve path
        url = ctx.interpolate(self.url, m)

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
        encoding = ctx.interpolate(self.encoding, m)
        m[self.name] = self._solve_encoding(encoding, m[self.name])
        m["_encoding"] = encoding
        """

        if html is not None:
            m[self.name] = html
            yield m
        else:
            logger.error("Could not retrieve HTTP document in %d attempts (blocking message)." % (self.attempts))


