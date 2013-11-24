import logging

from dateutil import parser
from cubetl.core import Node


# Get an instance of a logger
logger = logging.getLogger(__name__)

class DateExtract(Node):
    
    def __init__(self):
        
        super(DateExtract, self).__init__()
        
        self.mappings = []
    
    def process(self, ctx, m):

        for mapping in self.mappings:

            if ("source" in mapping):
                datestring = m[mapping["source"]]
            else:
                datestring = m[mapping["name"]]
            
            datetime = parser.parse(datestring, fuzzy = True)
            m[mapping["name"]] = datetime
    
            logger.debug("Datetime %s parsed to %s" % (datestring, datetime))
        
        yield m
        
