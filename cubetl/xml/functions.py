import logging
import re
from dateutil import parser
from urlparse import urlparse as org_ulparse
from slugify import slugify
import HTMLParser
import mimetypes


# Get an instance of a logger
logger = logging.getLogger(__name__)


def scrap(soup, search, get="next"):

    result = None

    found = soup.findAll(text = re.compile(search))

    if found:

        if len(found) > 1:
            result = found
        else:

            if get == "next":
                result = " ".join([s.text for s in found[0].parent.findNextSiblings()])
            elif get == "self":
                result = str(found[0]) #.text
            else:
                raise ValueError("Invalid scrap get mode: %s" % (get))

    return result

