import logging
from os import listdir
from os.path import isfile, join
import itertools
import re
from cubetl.core import Node
import chardet
from BeautifulSoup import UnicodeDammit
from cubetl.fs import FileReader
import csv



# Get an instance of a logger
logger = logging.getLogger(__name__)


class CsvReader(Node):

    data = '${ m["data"] }'

    header = None
    headers = None

    delimiter = ","
    row_delimiter = "\n"

    count = 0
    _linenumber = 0

    def _utf_8_encoder(self, unicode_csv_data):
        for line in unicode_csv_data:
            yield line.encode('utf-8')

    def process(self, ctx, m):

        logger.debug ("Processing CSV data at %s" % self)

        # Resolve data
        data = ctx.interpolate(m, self.data)

        header = None
        if (self.headers):
            if (isinstance(self.headers, basestring)):
                header = [h.strip() for h in self.headers.split(",")]
            else:
                header = self.headers

        #census_year = 0
        rows = iter (self._utf_8_encoder(data.split(self.row_delimiter)))

        reader = csv.reader(rows, delimiter = self.delimiter)
        for row in reader:

            self._linenumber = self._linenumber + 1

            if (header == None):
                header = [v.encode('utf-8') for v in row]
                logger.debug ("CSV header is: %s" % header)
                continue

            if (self._linenumber == 1) and (self.header): continue

            #arow = {}
            if (len(row) > 0):
                arow = ctx.copy_message(m)
                for header_index in range (0,  len(header)):
                    arow[(header[header_index])] = unicode(row[header_index], "utf-8")

                self.count = self.count + 1
                yield arow


class CsvFileReader (CsvReader):
    """
    This class is a shortcut to a FileReader and CsvReader
    """

    path = None
    filter_re = None

    encoding = "detect"
    encoding_errors = "strict" # strict, ignore, replace
    encoding_abort = True

    def initialize(self, ctx):

        super(CsvFileReader, self).initialize(ctx)

        self._fileReader = FileReader()
        self._fileReader.path = self.path
        if (self.encoding): self._fileReader.encoding = self.encoding

        ctx.comp.initialize(self._fileReader)

    def finalize(self, ctx):
        ctx.comp.finalize(self._fileReader)
        super(CsvFileReader, self).finalize(ctx)

    def process(self, ctx, m):

        logger.debug("Reading and processing CSV file at %s" % self)

        files_msgs = ctx.comp.process(self._fileReader, m)
        for mf in files_msgs:
            csv_rows = super(CsvFileReader, self).process(ctx, m)
            for csv_row in csv_rows:
                yield csv_row


