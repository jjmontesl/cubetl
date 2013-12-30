import logging
from cubetl.core import Node
from elementtidy.TidyHTMLTreeBuilder import TidyHTMLTreeBuilder as TB
from xml.etree import ElementTree
import lxml
from BeautifulSoup import BeautifulSoup

# Get an instance of a logger
logger = logging.getLogger(__name__)

class XmlParser(Node):
    
    def __init__(self):
        
        super(XmlParser, self).__init__()
        
        self.encoding = 'utf-8' #'${ m["encoding"] }'
    
    def process(self, ctx, m):
        
        #logger.debug("Parsing XML")
        
        parser = lxml.etree.XMLParser(ns_clean=True, recover=True, encoding=ctx.interpolate(m, self.encoding))
        m["xml"] = lxml.etree.fromstring(m["data"].encode(ctx.interpolate(m, self.encoding)), parser = parser)
        
        yield m
            

class XPathExtract(Node):
    
    def __init__(self):
        
        super(XPathExtract, self).__init__()
    
        self.eval = []
        self.xml = "xml"
        
        self.encoding = 'utf-8' #'${ m["encoding"] }'
        
    def initialize(self, ctx):
        super(XPathExtract, self).initialize(ctx)
        
        if (hasattr(self, "mappings")):
            raise Exception ("%s config contains a mappings element which is not allowed (use 'eval' element)" % self)
    
    def process(self, ctx, m):

        logger.debug ("XPathExtract (%s eval)" % len(self.eval))
        
        for eval in self.eval:
            
            if ("xpath" in eval):
                
                m[eval["name"]] = m[self.xml].xpath(eval["xpath"])
                if (isinstance(m[eval["name"]], str)):
                    m[eval["name"]] = m[eval["name"]].decode(ctx.interpolate(m, self.encoding))
                #m[mapping["key"]] = etree.XPath("string()")( m["xml"].xpath(mapping["xpath"])[0] )
                #m[mapping["key"]] = etree.tostring(m["xml"].xpath(mapping["xpath"])[0], method="text", encoding=unicode)
            
            if ("eval" in eval):
                raise Exception("Deprecated (invalid) option 'eval' in eval at %s" % self)
            
            if ("value" in eval):
                m[eval["name"]] = ctx.interpolate(m, eval["value"])

            if ("default" in eval):
                if ((not eval["name"] in m) or
                    (m[eval["name"]] == None) or
                    (m[eval["name"]].strip() == "")):
                    
                    m[eval["name"]] = ctx.interpolate(m, eval["default"])   
            
        yield m
        

class TidyHtmlParser(Node):
    
    def __init__(self):
        
        super(TidyHtmlParser, self).__init__()
        
        self.encoding = 'utf-8' #'${ m["encoding"] }'
        
    def process(self, ctx, m):
        
        #logger.debug("Parsing XML")
        
        tb = TB(encoding=self.encoding)
        tb.feed(m["data"].encode(self.encoding))
        m["tidy"] = tb.close()
        
        yield m
        
class BeautifulSoupParser(Node):
    
    def __init__(self):

        super(BeautifulSoupParser, self).__init__()

        #self.parser = "html.parser"
        #self.encoding = 'utf-8' #'${ m["encoding"] }'
        
    def process(self, ctx, m):
        
        #logger.debug("Parsing XML")
        m["soup"] = BeautifulSoup(m["data"]) #, self.parser)
        
        yield m

