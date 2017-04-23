import logging
from cubetl.core import Node
#from elementtidy.TidyHTMLTreeBuilder import TidyHTMLTreeBuilder as TB
import lxml
from lxml import etree
from xml.dom import pulldom
from xml.etree import ElementTree


# Get an instance of a logger
logger = logging.getLogger(__name__)


class XmlPullParser(Node):

    path = None
    tagname = None

    def process(self, ctx, m):

        path = ctx.interpolate(ctx, self.path)
        logger.debug("Reading XML in pull mode (splitting by tag '%s'): %s" % (self.tagname, path))


        with open(path, "r") as xmlfile:

            doc = pulldom.parse(xmlfile)
            for event, node in doc:
                if event == pulldom.START_ELEMENT and node.tagName == self.tagname:
                    doc.expandNode(node)

                    m2 = ctx.copy_message(m)
                    xmltext = node.toxml().encode('utf-8')
                    xmltext = "<root>" + xmltext + "</root>"
                    parser = etree.XMLParser(recover=True, encoding="utf-8")
                    xml = etree.fromstring(xmltext, parser=parser)

                    for elem in xml.iter():
                        if ":" in elem.tag:
                            elem.tag = ":".join(elem.tag.split(":")[1:])

                    m2['xml'] = xml

                    yield m2


class XmlParser(Node):

    encoding = 'utf-8'  #'${ m["encoding"] }'

    def process(self, ctx, m):

        #logger.debug("Parsing XML")
        parser = etree.XMLParser(ns_clean=True, recover=True, encoding=ctx.interpolate(m, self.encoding))
        m["xml"] = etree.fromstring(m["data"].encode(ctx.interpolate(m, self.encoding)), parser=parser)

        yield m


class XPathExtract(Node):

    eval = []
    xml = "xml"
    encoding = "utf-8" #'${ m["encoding"] }'

    def __init__(self):
        super(XPathExtract, self).__init__()
        self.eval = []

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


'''
class TidyHTMLParser(Node):

    encoding = 'utf-8' #'${ m["encoding"] }'

    def process(self, ctx, m):

        #logger.debug("Parsing XML")

        tb = TB(encoding=self.encoding)
        tb.feed(m["data"].encode(self.encoding))
        m["tidy"] = tb.close()

        yield m
'''


class BeautifulSoupParser(Node):

    def __init__(self):

        super(BeautifulSoupParser, self).__init__()

    def process(self, ctx, m):

        from BeautifulSoup import BeautifulSoup
        import beautifulsoupselect as soupselect
        # Monkeypatch BeautifulSoup
        BeautifulSoup.findSelect = soupselect.select

        #logger.debug("Parsing XML")
        m["soup"] = BeautifulSoup(m["data"]) #, self.parser)

        yield m

