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
   
    def __init__(self):
        
        self.path = None
        self.filter_re = None
        self.name = "filename" 
    
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
    
    
    
    def __init__(self):
        
        self.filename = None
        
        self.encoding = "detect"
        self.encoding_errors = "strict" # strict, ignore, replace
        self.encoding_abort = True
        
        self.name = "data"
    
    def initialize(self, ctx):
        
        super(FileReader, self).initialize(ctx)
        
        if (self.filename == None):
            raise Exception("Missing filename attribute for %s" % self)
        
    
    def process(self, ctx, m):
        
        # Resolve filename
        msg_filename = ctx.interpolate(m, self.filename)
        
        logger.debug ("Reading file %s" % msg_filename)
        with open (msg_filename, "r") as myfile:

            m[self.name] = myfile.read()

            # Encoding
            encoding = ctx.interpolate(m, self.encoding)
            
            if encoding:
            
                if (encoding in ["guess", "detect", "unicodedammit"]):
                    dammit = UnicodeDammit(m[self.name])
                    encoding = dammit.originalEncoding
                    m[self.name] = dammit.unicode
                    logger.debug("Detected content encoding as %s (using 'unicodedammit' detection)" % encoding )
                    
                else:
                    if (encoding in ["chardet"]):
                        chardet_result = chardet.detect(m[self.name])
                        encoding = chardet_result['encoding']
                        logger.debug("Detected content encoding as %s (using 'chardet' detection)" % encoding )  
                    
                    try:
                        m[self.name] = m[self.name].decode(encoding, self.encoding_errors)
                    except UnicodeDecodeError:
                        if (self.encoding_abort):
                            raise Exception ("Error decoding unicode with encoding '%s' on data: %r" %  (encoding, m[self.name]))
                        logger.warn("Error decoding unicode with encoding '%s' on data: %r" % (encoding, m[self.name]))
                        m[self.name] = m[self.name].decode("latin-1")
                    
                m["_encoding"] = encoding
                
        yield m
            

class DirectoryFileReader (Node):
    """
    This class is a shortcut to a DirectoryLister and a FileReader
    """
    
    def __init__(self):
        
        self.path = None
        self.filter_re = None 
        
        self.encoding = None
    
    def initialize(self, ctx):
        
        super(DirectoryFileReader, self).initialize(ctx)
        
        self.directoryLister = DirectoryLister()
        self.directoryLister.filter_re = self.filter_re
        self.directoryLister.path = self.path
        
        self.fileReader = FileReader()
        self.fileReader.filename = "${ m['filename'] }"
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
        

