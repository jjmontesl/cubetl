# CubETL
# Copyright (c) 2013-2019 Jose Juan Montes

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


from dateutil import parser
from urllib import parse
import logging
import mimetypes
import re
import string

from slugify import slugify


# Get an instance of a logger
logger = logging.getLogger(__name__)


def slug(value):
    return slugify(value)


def slugu(value):
    return slug(value).replace("-", "_")


def labelify(value):
    return string.capwords(slugify(value).replace("-", " "))


def re_search(pattern, text, match = 0):
    m = re.search(pattern, text)
    return m.group(match)


def urlparse(value):
    return parse.urlparse(value)


#_html_parser = HTMLParser.HTMLParser()
def html_unescape(value):
    return _html_parser.unescape(value)


def mimetype_guess(url, strict = False):
    return mimetypes.guess_type(url, strict)[0]


def parsebool(value):

    if (isinstance(value, bool)): return value

    try:
        v = value.strip().lower()
        if (v == "true"):
            return True
        elif (v == "false"):
            return False
        else:
            raise Exception("Invalid boolean value '%s' (valid values are 'True' or 'False')" % value)
    except Exception as e:
        raise Exception("Invalid boolean value '%r' (valid values are 'True' or 'False')" % value)


def format_seconds_hms(sec):

    sec = int(sec)

    days = sec // 86400
    sec -= 86400 * days

    hrs = sec // 3600
    sec -= 3600 * hrs

    mins = sec // 60
    sec -= 60 * mins

    val = "%02d:%02d:%02d" % (hrs, mins, sec)
    if days:
        val = "%dd:%s" % (days, val)

    return val


def extract_date(value, dayfirst, fuzzy=True):

    if value is None:
        raise ValueError("Tried to extract date from null value.")
    datetime = parser.parse(value, dayfirst = dayfirst, fuzzy = fuzzy)
    return datetime


def extract_number(value):

    if value is None: return None
    if isinstance(value, int): return value
    if isinstance(value, float): return value

    text = value
    text = re.sub(r'\&\#[0-9A-Fa-f]+', '', text)
    text = re.sub(r' +', ' ', text)

    _pattern = r"""(?x)       # enable verbose mode (which ignores whitespace and comments)
        ^                     # start of the input
        [^\d+-\.]*            # prefixed junk
        (?P<number>           # capturing group for the whole number
            (?P<sign>[+-])?       # sign group (optional)
            (?P<integer_part>         # capturing group for the integer part
                \d{1,3}               # leading digits in an int with a thousands separator
                (?P<sep>              # capturing group for the thousands separator
                    [ ,.]                 # the allowed separator characters
                )
                \d{3}                 # exactly three digits after the separator
                (?:                   # non-capturing group
                    (?P=sep)              # the same separator again (a backreference)
                    \d{3}                 # exactly three more digits
                )*                    # repeated 0 or more times
            |                     # or
                \d+                   # simple integer (just digits with no separator)
            )?                    # integer part is optional, to allow numbers like ".5"
            (?P<decimal_part>     # capturing group for the decimal part of the number
                (?P<point>            # capturing group for the decimal point
                    (?(sep)               # conditional pattern, only tested if sep matched
                        (?!                   # a negative lookahead
                            (?P=sep)              # backreference to the separator
                        )
                    )
                    [.,']                  # the accepted decimal point characters
                )
                \d+                   # one or more digits after the decimal point
            )?                    # the whole decimal part is optional
        )
        [^\d]*                # suffixed junk
        $                     # end of the input
    """

    match = re.match(_pattern, text)
    if match is None or not (match.group("integer_part") or
                             match.group("decimal_part")):    # failed to match
        return None                      # consider raising an exception instead



    num_str = match.group("number")      # get all of the number, without the junk
    sep = match.group("sep")
    if sep:
        sep_count = num_str.count(sep)
        num_str = num_str.replace(sep, "")     # remove thousands separators
    else:
        sep_count = 0


    if match.group("decimal_part"):
        point = match.group("point")
        if point != ".":
            num_str = num_str.replace(point, ".")  # regularize the decimal point
        return float(num_str)
    else:
        # Special case for 1.500 (we want it to be parsed as float)
        if (sep and sep != ' ' and sep_count == 1 ):
            return float(match.group("number").replace(sep, "."))  # regularize the decimal point

    return int(num_str)


