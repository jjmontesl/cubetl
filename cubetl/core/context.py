
import cubetl
import logging

from cubetl.text import functions
import sys
import traceback

from repoze.lru import LRUCache
from cubetl.core.components import Components
import copy
import inspect
from cubetl.core import Component
from inspect import isclass

# Get an instance of a logger
logger = logging.getLogger(__name__)

class Context():

    def __init__(self):

        self.args = {}

        self.debug = False
        self.debug2 = False

        self.quiet = False

        self.config_files = []

        self.start_node = None
        self.start_message = {}

        self.props = {}

        self._globals = {
                         "text": functions,
                         "cubetl": cubetl
                         }

        self._compiled = LRUCache(512)  # TODO: Configurable

        self.comp = Components(self)


    @staticmethod
    def _class_from_frame(fr):
        try:
            class_type = fr.f_locals['self'].__class__
        except KeyError:
            class_type = None

        return class_type

    def interpolate(self, m, value, data = {}):

        # TODO: Naive interpolation

        # TODO: We are enforcing unicode working around Python Spring seems to give strings, not unicode
        # This shall not be necessary and it's possibly bad practice
        pos = -1
        result = unicode(value)

        for dstart,dend in (('${|', '|}'), ('${', '}')):
            if (pos >= -1): pos = result.find(dstart)
            while (pos >= 0):
                pos_end = result.find(dend)
                expr = result[pos+len(dstart):pos_end].strip()

                compiled = self._compiled.get(expr)
                try:
                    if (not compiled):
                        compiled = compile(expr, '', 'eval')
                        self._compiled.put(expr, compiled)

                    c_locals = { "m": m, "ctx": self, "cubetl": cubetl }
                    c_locals.update(data)
                    res = eval (compiled, self._globals ,  c_locals)

                    if (self.debug2):
                        if (isinstance(res, basestring)):
                            logger.debug ('Evaluated: %s = %r' % (expr, res if (len(res) < 100) else res[:100] + ".."))
                        else:
                            logger.debug ('Evaluated: %s = %r' % (expr, res))

                except (Exception) as e:
                    exc_type, exc_value, exc_traceback = sys.exc_info()

                    caller_component = None
                    frame = inspect.currentframe()
                    for caller in inspect.getouterframes(frame):
                        fc = Context._class_from_frame(caller[0])
                        if (isclass(fc) and issubclass(fc, Component)):
                            caller_component = caller[0].f_locals['self']
                            break

                    #logger.error("Error evaluating expression %s on data: %s" % (expr, m))
                    raise Exception('Error evaluating expression "%s" called from %s:\n%s' % (expr, caller_component, ("".join(traceback.format_exception_only(exc_type, exc_value)))) )



                if ((pos>0) or (pos_end < len(result) - (len(dend)))):
                    result = result[0:pos] + unicode(res) + result[pos_end + (len(dend)):]
                    pos = result.find(dstart)
                else:
                    # Keep non-string types
                    result = res
                    pos = -2

        return result

    def copy_message (self, m):
        if m == None:
            return {}
        else:
            return copy.copy (m)

