import logging
import re

# Get an instance of a logger
logger = logging.getLogger(__name__)

from slugify import slugify

def slug(value):
    return unicode(slugify(unicode(value)))

def slugu(value):
    return slug(value).replace("-", "_")

def parsebool (value):
    
    if (isinstance(value, bool)): return value

    try:    
        v = value.strip().lower()
        if (v == "true"):
            return True
        elif (v == "false"):
            return False
        else:
            raise Exception("Invalid boolean value '%s' (valid values are 'True' or 'False')" % value)
    except Exception, e:
        raise Exception("Invalid boolean value '%r' (valid values are 'True' or 'False')" % value)
    

def extract_number(value):
    
    if (value == None): return None
    if (isinstance(value, int)): return value
    if (isinstance(value, float)): return value
        
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
            return float (match.group("number").replace(sep, ".")) # regularize the decimal point

    return int(num_str)   


