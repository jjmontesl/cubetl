import logging
from lxml import etree
from cubetl.core import Node

# Get an instance of a logger
logger = logging.getLogger(__name__)

class XmlParser(Node):
    
    def __init__(self):
        
        super(XmlParser, self).__init__()
        
        self.encoding = 'utf-8' #'${ m["encoding"] }'
    
    def process(self, ctx, m):
        
        #logger.debug("Parsing XML")
        
        parser = etree.XMLParser(ns_clean=True, recover=True, encoding=ctx.interpolate(m, self.encoding))
        m["xml"] = etree.fromstring(m["data"].encode(ctx.interpolate(m, self.encoding)), parser = parser)
        
        yield m
            
        
class XPathExtract(Node):
    
    def __init__(self):
        
        super(XPathExtract, self).__init__()
    
        self.mappings = []
        self.xml = "xml"
        
        self.encoding = 'utf-8' #'${ m["encoding"] }'
    
    def process(self, ctx, m):

        logger.debug ("XPathExtract (%s mappings)" % len(self.mappings))
        
        for mapping in self.mappings:
            
            if ("xpath" in mapping):
                
                m[mapping["name"]] = m[self.xml].xpath(mapping["xpath"])
                if (isinstance(m[mapping["name"]], str)):
                    m[mapping["name"]] = m[mapping["name"]].decode(ctx.interpolate(m, self.encoding))
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
        
