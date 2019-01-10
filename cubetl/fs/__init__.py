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


from bs4.dammit import UnicodeDammit
from os import listdir
from os.path import isfile, join
import chardet
import itertools
import logging
import os
import re

from cubetl.core import Node
from cubetl.core.exceptions import ETLConfigurationException
import sys


# Get an instance of a logger
logger = logging.getLogger(__name__)


class DirectoryList(Node):
    """
    This produces a message for each file in a given path.
    Each message contains a property named `path` (by default) with the
    path to the file.

    The list is in arbitrary order.  It does not include the special
    entries '.' and '..' even if they are present in the directory.

    Note that the input message is ignored by default (and its
    properties lost), but you can choose to copy the input message
    using `copy=True`.
    """


    def __init__(self, path, filter_re=None, name="path", maxdepth=0, copy=False):
        super().__init__()

        self.path = path
        self.filter_re = filter_re
        self.copy = copy
        self.maxdepth = maxdepth

        self.name = name

    @staticmethod
    def walklevel(path, maxdepth=0):
        path = os.path.normpath(path)
        assert os.path.isdir(path)
        num_sep = path.rstrip(os.path.sep).count(os.path.sep)
        for root, dirs, files in os.walk(path):
            yield root, dirs, files
            num_sep_this = root.rstrip(os.path.sep).count(os.path.sep)
            if maxdepth is not None and num_sep + maxdepth <= num_sep_this:
                del dirs[:]

    def process(self, ctx, m):

        # Resolve path
        path = ctx.interpolate(m, self.path)

        # Check m is empty, etc

        filter_re = ctx.interpolate(m, self.filter_re)
        logger.info("Listing directory %s (mask '%s')" % (path, filter_re))

        files = ((root, f) for root, dirs, files in self.walklevel(path, self.maxdepth) for f in files)
        if filter_re:
            regex = re.compile(filter_re)
            files = (ma[0] for ma in (regex.match(f) for f in files) if ma)
        files = (str(join(f[0], f[1])) for f in files)

        for f in files:
            fields = {self.name: f}
            if self.copy:
                m2 = ctx.copy_message(m)
                m2.update(fields)
            else:
                m2 = fields
            yield m2


class FileInfo(Node):
    """
    This node adds data to the input message, altering it.
    """


    def __init__(self, path="${m['path']}", prefix=''):
        super().__init__()
        self.path = path
        self.prefix = ''

    def process(self, ctx, m):
        # Resolve path
        path = ctx.interpolate(m, self.path)
        stat = os.stat(path)

        m[self.prefix + 'size'] = stat.st_size
        m[self.prefix + 'mtime'] = stat.st_mtime

        # TODO: add other info: times, owners

        # TODO: resolve user names optionally

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
            m["_file_path"] = msg_path
            m["_encoding"] = encoding

        yield m


class FileWriter(Node):
    """
    This class is encoding-agnostic.
    TODO: Create an EncodingFileWriter if needed.
    """

    def __init__(self, path="-", append=False, overwrite=False, data='${ m["data"] }', newline="\n"):
        super().__init__()

        self.path = path
        self.overwrite = overwrite
        self.append = append

        self.data = data
        self.newline = newline

        self._open_records = 0
        self._open_file = None
        self._open_path = None

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

            if self._open_file != sys.stdout:
                self._close()

            # Check if file exists
            file_exists = os.path.isfile(path) if path != "-" else False
            if file_exists and not self.overwrite:
                raise Exception("Cannot open file '%s' for writing as it already exists (you may wish to use 'overwrite: True')" % path)

            if file_exists:
                logger.info("Opening (overwriting) file '%s'" % path)
            else:
                if path == "-":
                    logger.info("Writing to standard output (file: '-'):")
                else:
                    logger.info("Creating file '%s' for writing" % path)
            self._open_records = 0
            self._open_path = path
            self._open_file = open(path, "w") if path != "-" else sys.stdout

            self.on_open()

    def process(self, ctx, m, value = None):

        self._close_reopen_file(ctx, m)
        self._open_records = self._open_records + 1

        if not value:
            value = ctx.interpolate(m, self.data)

        self._open_file.write(value)
        if self.newline:
            self._open_file.write(self.newline)


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


