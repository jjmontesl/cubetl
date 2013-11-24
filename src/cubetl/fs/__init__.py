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
            m = { "data": f }
            yield m 
    
class FileReader(Node):
    
    
    
    def __init__(self):
        
        self.filename = '${ m["data"] }'
        
        self.encoding = "detect"
        self.encoding_errors = "strict" # strict, ignore, replace
        self.encoding_abort = True
    
    def process(self, ctx, m):
        
        # Resolve filename
        msg_filename = ctx.interpolate(m, self.filename)
        
        logger.debug ("Reading file %s" % msg_filename)
        with open (msg_filename, "r") as myfile:

            m["_filename"] = msg_filename
            m["data"] = myfile.read()

            # Encoding
            encoding = ctx.interpolate(m, self.encoding)
            
            if encoding:
            
                if (encoding in ["guess", "detect", "unicodedammit"]):
                    dammit = UnicodeDammit(m["data"])
                    encoding = dammit.originalEncoding
                    m["data"] = dammit.unicode
                    logger.debug("Detected content encoding as %s (using 'unicodedammit' detection)" % encoding )
                    
                else:
                    if (encoding in ["chardet"]):
                        chardet_result = chardet.detect(m["data"])
                        encoding = chardet_result['encoding']
                        logger.debug("Detected content encoding as %s (using 'chardet' detection)" % encoding )  
                    
                    try:
                        m["data"] = m["data"].decode(encoding, self.encoding_errors)
                    except UnicodeDecodeError:
                        if (self.encoding_abort):
                            raise Exception ("Error decoding unicode with encoding '%s' on data: %r" %  (encoding, m["data"]))
                        logger.warn("Error decoding unicode with encoding '%s' on data: %r" % (encoding, m["data"]))
                        m["data"] = m["data"].decode("latin-1")
                    
                m["_encoding"] = encoding
                
                
                
        
        yield m
            

class DirectoryFileReader (Node):
    """
    This class is a shortcut to a DirectoryLister and a FileReader
    """
    
    def __init__(self):
        
        self.path = None
        self.filter_re = None 
        
        self.is_initialized = False
        self.encoding = None
    
    def process(self, ctx, m):
        
        if (not self.is_initialized):
            self.directoryLister = DirectoryLister()
            self.directoryLister.filter_re = self.filter_re
            self.directoryLister.path = self.path
            
            self.fileReader = FileReader()
            if (self.encoding): self.fileReader.encoding = self.encoding 
            
            self.is_initialized = True
        
        files_msgs = self.directoryLister.process(ctx, m)
        for mf in files_msgs:
            fr_msgs = self.fileReader.process(ctx, mf)
            for mfr in fr_msgs:
                yield mfr
        

