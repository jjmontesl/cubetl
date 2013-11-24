import logging
from lxml import etree
from cubetl.core import Node

# Get an instance of a logger
logger = logging.getLogger(__name__)

class XmlParser(Node):
    
    def __init__(self):
        
        super(XmlParser, self).__init__()
    
    def process(self, ctx, m):
        
        #logger.debug("Parsing XML")
        
        m["xml"] = etree.fromstring(m["data"])
        
        yield m
            
        
class XPathExtract(Node):
    
    def __init__(self):
        
        super(XPathExtract, self).__init__()
    
        self.mappings = []
        self.xmlfield = "xml"
    
    def process(self, ctx, m):

        logger.debug ("XPathExtract (%s mappings)" % len(self.mappings))
        
        for mapping in self.mappings:
            
            if ("xpath" in mapping):
                
                m[mapping["name"]] = m[self.xmlfield].xpath(mapping["xpath"])
                #m[mapping["key"]] = etree.XPath("string()")( m["xml"].xpath(mapping["xpath"])[0] )
                #m[mapping["key"]] = etree.tostring(m["xml"].xpath(mapping["xpath"])[0], method="text", encoding=unicode)
            
            if ("eval" in mapping):
                m[mapping["name"]] = ctx.interpolate(m, mapping["eval"])

            if ("default" in mapping):
                if ((not mapping["name"] in m) or
                    (m[mapping["name"]] == None) or
                    (m[mapping["name"]].strip() == "")):
                    
                    m[mapping["name"]] = ctx.interpolate(m, mapping["default"])   
            
        yield m
        
