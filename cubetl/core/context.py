
from inspect import isclass
from past.builtins import basestring
from repoze.lru import LRUCache
import cProfile
import copy
import datetime
import inspect
import logging
import os
import random
import re
import sys
import traceback
import urllib

from cubetl.core import Component
from cubetl.core.components import Components
from cubetl.core.exceptions import ETLException
from cubetl.text import functions
from cubetl.xml import functions as xmlfunctions
import cubetl
from collections import OrderedDict


# Get an instance of a logger
logger = logging.getLogger(__name__)


class Context():

    def __init__(self):

        self.cli = False
        self.args = {}

        self.debug = False
        self.debug2 = False

        self.quiet = False

        self.profile = False

        self.components = OrderedDict()

        self.start_item = {}

        self.props = {}
        self.properties = self.props

        self.var = {}

        self.working_dir = os.getcwd()
        self.library_path = os.path.dirname(os.path.realpath(__file__)) + "/../../library"

        self._globals = {
                         "text": functions,
                         "xml": xmlfunctions,
                         "cubetl": cubetl,
                         "datetime": datetime,
                         "re": re,
                         "sys": sys,
                         "urllib": urllib,
                         "random": random.Random()
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

    def get(self, uid):
        #logger.debug("Getting component: %s" % component_id)
        try:
            comp = self.components.get(uid, None)
        except KeyError as e:
            raise KeyError("Component not found with id '%s'" % uid)
        return comp

    def key(self, comp):
        for k, c in self.components.items():
            if c == comp:
                return k
        return None

    def find(self, type):
        result = []
        for comp in self.components.values():
            if isinstance(comp, type):
                result.append(comp)
        return result

    def add(self, urn, component):

        # FIXME: TODO: Allow anonymous components? these would be exported in-line with their parents.
        # This assumes that components are initialized completely (possibly better for config comprehension)
        # Also would serve as a hint for deep-swallow copying (anonymous components are always deep copied?)

        if urn is None:
            raise Exception('Tried to configure an object with no URN')
        if component is None:
            raise Exception('Tried to configure a null object')
        if not isinstance(component, Component):
            raise Exception('Tried to configure a non Component object: %s' % component)
        if self.get(urn) != None:
            raise Exception("Tried to define an already existing URN: %s" % urn)

        component.ctx = self
        component.urn = urn

        self.components[urn] = component
        return component

    def interpolate(self, m, value, data = {}):

        if value == None:
            return None

        if not isinstance(value, basestring):
            return value

        value = value.strip()

        pos = -1
        result = str(value)

        for dstart, dend in (('${|', '|}'), ('${', '}')):
            if (pos >= -1):
                pos = result.find(dstart)
            while (pos >= 0):
                pos_end = result.find(dend)
                expr = result[pos + len(dstart):pos_end].strip()

                compiled = self._compiled.get(expr)
                try:
                    if (not compiled):
                        compiled = compile(expr, '', 'eval')
                        self._compiled.put(expr, compiled)

                    c_locals = { "m": m, "ctx": self, "props": self.props, "var": self.var, "cubetl": cubetl }
                    c_locals.update(data)
                    res = eval(compiled, self._globals, c_locals)

                    if (self.debug2):
                        if (isinstance(res, basestring)):
                            logger.debug('Evaluated: %s = %r' % (expr, res if (len(res) < 100) else res[:100] + ".."))
                        else:
                            logger.debug('Evaluated: %s = %r' % (expr, res))

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
                    self._eval_error_message = m

                    logger.error('Error evaluating expression "%s" called from %s:\n%s' % (expr, caller_component, ("".join(traceback.format_exception_only(exc_type, exc_value)))))
                    raise

                if ((pos>0) or (pos_end < len(result) - (len(dend)))):
                    result = result[0:pos] + str(res) + result[pos_end + (len(dend)):]
                    pos = result.find(dstart)
                else:
                    # Keep non-string types
                    result = res
                    pos = -2

        return result

    def copy_message(self, m):
        if m == None:
            return {}
        else:
            return copy.copy(m)


    def _do_process(self, process, ctx):
        item = ctx.copy_message(ctx.start_item)
        msgs = ctx.comp.process(process, item)
        count = 0
        m = None
        for m in msgs:
            count = count + 1
        return (m, count)

    def process(self, start_node):

        ctx = self

        # Launch process
        if not start_node:
            logger.error("Start process '%s' not found in configuration" % ctx.start_node)
            if ctx.cli:
                sys.exit(1)
            else:
                raise Exception("Start process '%s' not found in configuration" % ctx.start_node)

        result = None
        processed = 0

        # Launch process and consume items
        try:
            logger.debug("Initializing components")
            ctx.comp.initialize(start_node)

            logger.info("Processing %s" % start_node)

            if ctx.profile:
                logger.warning("Profiling execution (WARNING this is SLOW) and saving results to: %s" % ctx.profile)
                cProfile.runctx("count = self._do_process(process, ctx)", globals(), locals(), ctx.profile)
            else:
                (result, processed) = self._do_process(start_node, ctx)

            logger.debug("%s items resulted from the process" % processed)

            logger.debug("Finalizing components")
            ctx.comp.finalize(start_node)

            ctx.comp.cleanup()

        except KeyboardInterrupt as e:
            logger.error("User interrupted")

        except Exception as e:
            '''
            exc_type, exc_value, exc_traceback = sys.exc_info()
            logger.fatal("Error during process: %s" % ", ".join((traceback.format_exception_only(exc_type, exc_value))))

            if hasattr(ctx, "eval_error_message"):
                pp = pprint.PrettyPrinter(indent=4, depth=2)
                print(pp.pformat(ctx._eval_error_message))

            traceback.print_exception(exc_type, exc_value, exc_traceback)
            '''

            raise

        return result


