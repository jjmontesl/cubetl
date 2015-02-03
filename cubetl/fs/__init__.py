import logging
from os import listdir
from os.path import isfile, join
import itertools
import re
from cubetl.core import Node
import chardet
from BeautifulSoup import UnicodeDammit



# Get an instance of a logger
logger = logging.getLogger(__name__)


class DirectoryLister(Node):

    path = None
    filter_re = None

    name = "path"

    def process(self, ctx, m):

        # Resolve path
        path = ctx.interpolate(m, self.path)

        # Check m is empty, etc

        logger.info ("Listing directory %s (mask '%s')" % (path, self.filter_re))

        files = [ f for f in listdir(path) if isfile(join(path, f)) ]
        if (self.filter_re != None):
            regex = re.compile(self.filter_re)
            files = [m.group(0) for m in [regex.match(f) for f in files] if m]
        files = [join(path, f) for f in files]

        for f in files:
            m = { self.name: f }
            yield m


class FileReader(Node):

    path = None

    encoding = "detect"
    encoding_errors = "strict" # strict, ignore, replace
    encoding_abort = True

    name = "data"


    def initialize(self, ctx):

        super(FileReader, self).initialize(ctx)

        if (self.path == None):
            raise Exception("Missing path attribute for %s" % self)

    def _solve_encoding(self, encoding, text):

        result = text
        if encoding:

            if (encoding in ["guess", "detect", "unicodedammit"]):
                dammit = UnicodeDammit(text)
                encoding = dammit.originalEncoding
                logger.debug("Detected content encoding as %s (using 'unicodedammit' detection)" % encoding )
                result = dammit.unicode

            else:
                if (encoding in ["chardet"]):
                    chardet_result = chardet.detect(text)
                    encoding = chardet_result['encoding']
                    logger.debug("Detected content encoding as %s (using 'chardet' detection)" % encoding )

                try:
                    result = text.decode(encoding, self.encoding_errors)
                except UnicodeDecodeError:
                    if (self.encoding_abort):
                        raise Exception ("Error decoding unicode with encoding '%s' on data: %r" %  (encoding, text))
                    logger.warn("Error decoding unicode with encoding '%s' on data: %r" % (encoding, text))
                    result = text.decode("latin-1")

        return result

    def process(self, ctx, m):

        # Resolve path
        msg_path = ctx.interpolate(m, self.path)

        logger.debug("Reading file %s" % msg_path)
        with open(msg_path, "r") as myfile:

            m[self.name] = myfile.read()

            # Encoding
            encoding = ctx.interpolate(m, self.encoding)
            m[self.name] = self._solve_encoding(encoding, m[self.name])
            m["_encoding"] = encoding

        yield m


class FileLineReader(FileReader):


    def initialize(self, ctx):

        super(FileReader, self).initialize(ctx)

    def process(self, ctx, m):

        # Resolve path
        msg_path = ctx.interpolate(m, self.path)

        logger.debug ("Reading file %s lines" % msg_path)
        with open (msg_path, "r") as myfile:

            for line in myfile:

                m2 = ctx.copy_message(m)
                m2[self.name] = line

                # Encoding
                encoding = ctx.interpolate(m2, self.encoding)
                m2[self.name] = self._solve_encoding(encoding, m2[self.name])
                m2["_encoding"] = encoding

                yield m2


class DirectoryFileReader (Node):
    """
    This class is a shortcut to a DirectoryLister and a FileReader
    """

    path = None
    filter_re = None

    encoding = None

    def initialize(self, ctx):

        super(DirectoryFileReader, self).initialize(ctx)

        self.directoryLister = DirectoryLister()
        self.directoryLister.filter_re = self.filter_re
        self.directoryLister.path = self.path

        self.fileReader = FileReader()
        self.fileReader.path = "${ m['path'] }"
        if (self.encoding): self.fileReader.encoding = self.encoding

        ctx.comp.initialize(self.directoryLister)
        ctx.comp.initialize(self.fileReader)

    def finalize(self, ctx):
        ctx.comp.finalize(self.directoryLister)
        ctx.comp.finalize(self.fileReader)
        super(DirectoryFileReader, self).finalize(ctx)

    def process(self, ctx, m):

        files_msgs = ctx.comp.process(self.directoryLister, m)
        for mf in files_msgs:
            fr_msgs = ctx.comp.process(self.fileReader, mf)
            for mfr in fr_msgs:
                yield mfr


