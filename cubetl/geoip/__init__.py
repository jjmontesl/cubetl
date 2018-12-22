
from cubetl.core import Node
from pygments import highlight
from pygments.formatters.terminal import TerminalFormatter
from pygments.lexers.web import JsonLexer
import json
import logging
import pprint
import simplejson
import sys
import re

import GeoIP
from incf.countryutils import transformations

# Get an instance of a logger
logger = logging.getLogger(__name__)


class GeoIPFromAddress(Node):
    """
    Splits text into lines.
    """

    def __init__(self, data='${ m["ip"] }', prefix='geoip'):
        super().__init__()
        self.data = data
        self.prefix = prefix

        self._extract_error = False

    def initialize(self, ctx):

        super().initialize(ctx)

        self._gi = GeoIP.new(GeoIP.GEOIP_MEMORY_CACHE)
        #self._gi = GeoIP.new(GeoIP.GEOIP_STANDARD)
        #self._gi = GeoIP.open("/usr/local/share/GeoIP/GeoIP.dat",GeoIP.GEOIP_STANDARD)
        #gi = GeoIP.open("/usr/local/share/GeoIP/GeoIPRegion.dat", GeoIP.GEOIP_STANDARD)

    def process(self, ctx, m):
        # TODO: Implement

        data = ctx.interpolate(m, self.data)

        m[self.prefix + "_country_code"] = self._gi.country_code_by_addr(data)
        m[self.prefix + "_country_name"] = self._gi.country_name_by_addr(data) #.decode("latin1")
        m[self.prefix + "_cont_name"] = None

        if m[self.prefix + "_country_code"]:
            try:
                m[self.prefix + "_cont_name"] = transformations.ccn_to_ctn(transformations.cca2_to_ccn(m[self.prefix + "_country_code"])) #.decode("latin1")
            except Exception as e:
                if not self._extract_error and not ctx.debug2:
                    self._extract_error = True
                    logger.warn("Could not extract continent name from country code '%s' (reported only once per run) Error: %s" % (m[self.prefix + "_country_code"], e))

        #gir = self._gi.region_by_addr(data)
        #m["geo_region_code"] = gir['region']
        #m["geo_region_name"] = gir['region_name']

        yield m

