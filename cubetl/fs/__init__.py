import logging
from os import listdir
from os.path import isfile, join
import itertools
import re
from cubetl.core import Node
import chardet
import os
from cubetl.core.exceptions import ETLConfigurationException
from bs4.dammit import UnicodeDammit

# Get an instance of a logger
logger = logging.getLogger(__name__)


class DirectoryList(Node):


    def __init__(self, path, filter_re=None):
        super().__init__()

        self.path = path
        self.filter_re = filter_re

        self.name = "path"

    def process(self, ctx, m):

        # Resolve path
        path = ctx.interpolate(m, self.path)

        # Check m is empty, etc

        filter_re = ctx.interpolate(m, self.filter_re)
        logger.info ("Listing directory %s (mask '%s')" % (path, filter_re))

        files = [ f for f in listdir(path) if isfile(join(path, f)) ]
        if (filter_re != None):
            regex = re.compile(filter_re)
            files = [m.group(0) for m in [regex.match(f) for f in files] if m]
        files = [join(path, f) for f in files]

        for f in files:
            m = { self.name: f }
            yield m


class FileReader(Node):
    """

    * encoding_errors can be one of "strict, ignore, replace"
    """

    def __init__(self, path, encoding="detect", encoding_errors="strict", encoding_abort=True):
        super().__init__()

        self.path = path

        self.encoding = encoding
        self.encoding_errors = encoding_errors # strict, ignore, replace
        self.encoding_abort = encoding_abort

        self.name = "data"

    def initialize(self, ctx):

        super().initialize(ctx)

        if self.path is None:
            raise ETLConfigurationException("Missing path attribute for %s" % self)

    def _solve_encoding(self, encoding, text):

        result = text
        if encoding:

            if (encoding in ["guess", "detect", "unicodedammit"]):
                dammit = UnicodeDammit(text)
                encoding = dammit.original_encoding  #originalEncoding
                logger.debug("Detected content encoding as %s (using 'unicodedammit' detection)" % encoding)
                result = str(dammit)

            else:
                if (encoding in ["chardet"]):
                    chardet_result = chardet.detect(text)
                    encoding = chardet_result['encoding']
                    logger.debug("Detected content encoding as %s (using 'chardet' detection)" % encoding)

                try:
                    result = text.decode(encoding, self.encoding_errors)
                except UnicodeDecodeError:
                    if (self.encoding_abort):
                        raise Exception("Error decoding unicode with encoding '%s' on data: %r" % (encoding, text))
                    logger.warning("Error decoding unicode with encoding '%s' on data: %r" % (encoding, text))
                    result = text.decode("latin-1")

        return result

    def process(self, ctx, m):

        # Resolve path
        msg_path = ctx.interpolate(m, self.path)

        logger.debug("Reading file %s (encoding=%s)" % (msg_path, self.encoding))
        with open(msg_path, "rb") as myfile:

            m[self.name] = myfile.read()

            # Encoding
            encoding = ctx.interpolate(m, self.encoding) if self.encoding else None
            m[self.name] = self._solve_encoding(encoding, m[self.name])
            m["_encoding"] = encoding

        yield m


class FileWriter(Node):
    """
    This class is encoding-agnostic.
    TODO: Create an EncodingFileWriter if needed.
    """

    path = None
    overwrite = False

    _open_records = 0
    _open_file = None
    _open_path = None

    data = '${ m["data"] }'
    newline = True

    def initialize(self, ctx):
        super(FileWriter, self).initialize(ctx)

    def finalize(self, ctx):

        self._close()
        super(FileWriter, self).finalize(ctx)

    def on_open(self):
        pass

    def on_close(self):
        pass

    def _close(self):
        if self._open_file:
            self.on_close()
            self._open_file.close()
            self._open_records = 0
            self._open_path = None

    def _close_reopen_file(self, ctx, m):

        path = ctx.interpolate(m, self.path)

        if (not self._open_file or path != self._open_path):

            self._close()

            # Check if file exists
            file_exists = os.path.isfile(path)
            if file_exists and not self.overwrite:
                raise Exception("Cannot open file '%s' for writing as it already exists (you may wish to use 'overwrite: True')" % path)

            if file_exists:
                logger.info("Opening (overwriting) file '%s'" % path)
            else:
                logger.info("Creating file '%s' for writing" % path)
            self._open_records = 0
            self._open_path = path
            self._open_file = open(path, "w")

            self.on_open()

    def process(self, ctx, m, value = None):

        self._close_reopen_file(ctx, m)
        self._open_records = self._open_records + 1

        if not value:
            value = ctx.interpolate(m, self.data)

        self._open_file.write(value + "\n" if self.newline else value)


class FileLineReader(FileReader):

    _line = 0

    def initialize(self, ctx):

        super(FileReader, self).initialize(ctx)

    def process(self, ctx, m):

        # Resolve path
        msg_path = ctx.interpolate(m, self.path)

        logger.debug("Reading file %s lines" % msg_path)
        with open(msg_path, "r") as myfile:

            for line in myfile:

                self._line = self._line + 1

                m2 = ctx.copy_message(m)
                m2[self.name] = line

                # Encoding
                encoding = ctx.interpolate(m2, self.encoding)
                m2[self.name] = self._solve_encoding(encoding, m2[self.name])
                m2["_encoding"] = encoding

                yield m2


class DirectoryFileReader(Node):
    """
    This class is a shortcut to a DirectoryLister and a FileReader
    """

    def __init__(self, path, filter_re=None, encoding="detect", encoding_errors="strict"):
        super().__init__()
        self.path = path
        self.filter_re = filter_re
        self.encoding = encoding
        self.encoding_errors = encoding_errors

    def initialize(self, ctx):

        super().initialize(ctx)

        self.directoryLister = DirectoryList(path=self.path,
                                             filter_re=self.filter_re)

        self.fileReader = FileReader(path="${ m['path'] }",
                                     encoding=self.encoding,
                                     encoding_errors=self.encoding_errors)

        ctx.comp.initialize(self.directoryLister)
        ctx.comp.initialize(self.fileReader)

    def finalize(self, ctx):
        ctx.comp.finalize(self.directoryLister)
        ctx.comp.finalize(self.fileReader)
        super().finalize(ctx)

    def process(self, ctx, m):

        files_msgs = ctx.comp.process(self.directoryLister, m)
        for mf in files_msgs:
            fr_msgs = ctx.comp.process(self.fileReader, mf)
            for mfr in fr_msgs:
                yield mfr


