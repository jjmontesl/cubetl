import logging
from os import listdir
from os.path import isfile, join
import itertools
import re
from cubetl.core import Node
import chardet
from BeautifulSoup import UnicodeDammit
from cubetl.fs import FileReader, FileWriter
import csv
import StringIO



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

        self._linenumber = 0
        rows = iter (self._utf_8_encoder(data.split(self.row_delimiter)))

        reader = csv.reader(rows, delimiter = self.delimiter)
        for row in reader:


            if (header == None):
                header = [v.encode('utf-8') for v in row]
                logger.debug("CSV header is: %s" % header)
                continue

            if (self._linenumber == 0) and (self.header): continue

            self._linenumber = self._linenumber + 1

            #arow = {}
            if (len(row) > 0):
                arow = ctx.copy_message(m)
                for header_index in range(0, len(header)):
                    arow[(header[header_index])] = unicode(row[header_index], "utf-8")

                self.count = self.count + 1
                arow['_csv_count'] = self.count
                arow['_csv_linenumber'] = self._linenumber

                yield arow


class CsvFileReader (CsvReader):
    """
    This class is a shortcut to a FileReader and CsvReader
    """

    path = None

    encoding = "detect"
    encoding_errors = "strict" # strict, ignore, replace
    encoding_abort = True

    def initialize(self, ctx):

        super(CsvFileReader, self).initialize(ctx)

        self._fileReader = FileReader()
        self._fileReader.path = self.path
        if (self.encoding):
            self._fileReader.encoding = self.encoding

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


class CsvFileWriter(Node):

    # TODO: This class should possibly inherit from FileWriter

    data = '${ m }'
    headers = None
    write_headers = True
    path = "-"

    _row = 0

    delimiter = ","
    row_delimiter = "\n"

    overwrite = False
    encoding = None #"utf-8"

    _output = None
    _csvwriter = None

    columns = None

    def initialize(self, ctx):

        super(CsvFileWriter, self).initialize(ctx)

        self._fileWriter = FileWriter()
        self._fileWriter.path = self.path
        self._fileWriter.data = "${ m['_csvdata'] }"
        if (self.encoding):
            self._fileWriter.encoding = self.encoding
            self._fileWriter.overwrite = self.overwrite
            self._fileWriter.newline = False

        # Process columns
        for c in self.columns:
            if not "label" in c:
                c["label"] = c["name"]
            if not "value" in c:
                c["value"] = '${ m["' + c["name"] + '"] }'

        ctx.comp.initialize(self._fileWriter)

    def finalize(self, ctx):
        ctx.comp.finalize(self._fileWriter)
        super(CsvFileWriter, self).finalize(ctx)

    def _csv_row(self, ctx, row):

        if self.encoding:
            row = [(r.encode(self.encoding) if isinstance(r, basestring) else r) for r in row]

        self._csvwriter.writerow(row)
        result = self._output.getvalue()
        self._output.truncate(0)
        return result

    def process(self, ctx, m):

        if not self._csvwriter:
            self._output = StringIO.StringIO()
            self._csvwriter = csv.writer(self._output, delimiter=self.delimiter,
                              quotechar='"', quoting=csv.QUOTE_MINIMAL)

        if (self._row == 0):
            if (self.write_headers):
                row = [c["label"] for c in self.columns]
                m['_csvdata'] = self._csv_row(ctx, row)
                self._fileWriter.process(ctx, m)

        self._row = self._row + 1

        row = [ctx.interpolate(m, c["value"]) for c in self.columns]
        m['_csvdata'] = self._csv_row(ctx, row)
        self._fileWriter.process(ctx, m)
        del (m['_csvdata'])

        yield m



