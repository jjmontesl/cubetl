
import logging
import re


# Get an instance of a logger
logger = logging.getLogger(__name__)


'''
# Removed: uncertain how to use next, self... improve
def scrap(soup, search, get="next"):

    result = None

    found = soup.find_all(string=re.compile(search))  # search
    if found:
        if len(found) > 1:
            result = found
        else:
            if get == "next":
                result = found[0].find_next_siblings()[0].text  # .parent
            elif get == "self":
                result = str(found[0]) #.text
            else:
                raise ValueError("Invalid scrap get mode: %s" % (get))

    return result
'''
