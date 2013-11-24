import logging
from os import listdir
from os.path import isfile, join
import itertools
import re
from cubetl.core import Node

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
    
    def process(self, ctx, m):
        
        # Resolve filename
        msg_filename = ctx.interpolate(m, self.filename)
        
        logger.debug ("Reading file %s" % msg_filename)
        with open (msg_filename, "r") as myfile:
            m["filename"] = msg_filename
            m["data"] = myfile.read()
        
        yield m
            

class DirectoryFileReader (Node):
    """
    This class is a shortcut to a DirectoryLister and a FileReader
    """
    
    def __init__(self):
        
        self.path = None
        self.filter_re = None 
        
        self.is_initialized = False
    
    def process(self, ctx, m):
        
        if (not self.is_initialized):
            self.directoryLister = DirectoryLister()
            self.directoryLister.filter_re = self.filter_re
            self.directoryLister.path = self.path
            
            self.fileReader = FileReader()
            
            self.is_initialized = True
        
        files_msgs = self.directoryLister.process(ctx, m)
        for mf in files_msgs:
            fr_msgs = self.fileReader.process(ctx, mf)
            for mfr in fr_msgs:
                yield mfr
        

