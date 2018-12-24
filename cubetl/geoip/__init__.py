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

from cubetl.core import Node
from incf.countryutils import transformations
import GeoIP


# Get an instance of a logger
logger = logging.getLogger(__name__)


class GeoIPFromAddress(Node):
    """
    This CubETL node performs a GeoIP library search for a given IP address,
    adding country and continent information to the message.

    It adds the following fields:

    * geoip_country_code
    * geoip_country_name
    * cont_name
    """

    def __init__(self, data='${ m["ip"] }', prefix='geoip_'):
        """
        Creates a new GeoIPFromAddress node.

        :param expr data: The input IP address (interpolated).
        :param str prefix: The prefix for output fields (defaults to 'geoip_')
        :type expr: A literal value, a string expression (interpolated) or a lambda.

        #:return: the message id
        #:rtype: int
        #:raises ValueError: if the message_body exceeds 160 characters
        #:raises TypeError: if the message_body is not a basestring
        """

        super().__init__()
        self.data = data
        self.prefix = prefix

        self._extract_error = False

    def initialize(self, ctx):
        """
        See :func:`~cubetl.core.Node.initialize`
        """

        super().initialize(ctx)

        self._gi = GeoIP.new(GeoIP.GEOIP_MEMORY_CACHE)
        #self._gi = GeoIP.new(GeoIP.GEOIP_STANDARD)
        #self._gi = GeoIP.open("/usr/local/share/GeoIP/GeoIP.dat",GeoIP.GEOIP_STANDARD)
        #gi = GeoIP.open("/usr/local/share/GeoIP/GeoIPRegion.dat", GeoIP.GEOIP_STANDARD)

    def process(self, ctx, m):

        data = ctx.interpolate(m, self.data)

        m[self.prefix + "country_code"] = self._gi.country_code_by_addr(data)
        m[self.prefix + "country_name"] = self._gi.country_name_by_addr(data) #.decode("latin1")
        m[self.prefix + "cont_name"] = None

        if m[self.prefix + "country_code"]:
            try:
                m[self.prefix + "cont_name"] = transformations.ccn_to_ctn(transformations.cca2_to_ccn(m[self.prefix + "country_code"])) #.decode("latin1")
            except Exception as e:
                if not self._extract_error and not ctx.debug2:
                    self._extract_error = True
                    logger.warn("Could not extract continent name from country code '%s' (reported only once per run) Error: %s" % (m[self.prefix + "country_code"], e))

        #gir = self._gi.region_by_addr(data)
        #m["geo_region_code"] = gir['region']
        #m["geo_region_name"] = gir['region_name']

        yield m

