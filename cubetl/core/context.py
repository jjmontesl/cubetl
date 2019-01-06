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


from inspect import isclass
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
from cubetl.core.exceptions import ETLException, ETLConfigurationException
from cubetl.text import functions
from cubetl.xml import functions as xmlfunctions
import cubetl
from collections import OrderedDict
import importlib


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
        self.start_nodes = []
        self.config_files = []

        self.props = {}
        self.properties = self.props

        self.var = {}

        self.working_dir = os.getcwd()
        self.library_path = os.path.dirname(os.path.realpath(__file__)) + "/../../library"

        self._globals = {"text": functions,
                         "xml": xmlfunctions,
                         "datetime": datetime,
                         "re": re,
                         "sys": sys,
                         "urllib": urllib,
                         "random": random.Random()}

        self._compiled = LRUCache(512)  # TODO: Configurable

        self.comp = Components(self)

    @staticmethod
    def _class_from_frame(fr):
        try:
            class_type = fr.f_locals['self'].__class__
        except KeyError:
            class_type = None

        return class_type

    def get(self, uid, fail=True):
        #logger.debug("Getting component: %s" % component_id)

        if uid is None:
            raise ETLException("Cannot retrieve component with id None.")

        comp = self.components.get(uid, None)

        if comp is None and fail:
            raise ETLException("Component not found with id '%s'" % uid)

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

    def add(self, urn, component, description=None):

        # FIXME: TODO: Allow anonymous components? these would be exported in-line with their parents.
        # This assumes that components are initialized completely (possibly better for config comprehension)
        # Also would serve as a hint for deep-swallow copying (anonymous components are always deep copied?)

        if urn is None:
            raise Exception('Tried to add an object with no URN')
        if component is None:
            raise Exception('Tried to add a null object')
        if not isinstance(component, Component):
            raise Exception('Tried to add a non Component object: %s' % component)
        if self.components.get(urn, None) is not None:
            raise Exception("Tried to add an already existing URN: %s" % urn)

        component.ctx = self
        component.urn = urn
        component.description = description

        self.components[urn] = component
        return component

    # TODO: Put value first
    def interpolate(self, m, value, data = {}):
        """
        Interpolates expressions `${ ... }` in a value.
        """

        if value == None:
            return None

        if callable(value):
            value = value(m)

        if not isinstance(value, str):
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
                        if (isinstance(res, str)):
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

                if (pos > 0) or (pos_end < len(result) - (len(dend))):
                    result = result[0:pos] + str(res) + result[pos_end + (len(dend)):]
                    pos = result.find(dstart)
                else:
                    # Keep type of non-string types
                    result = res
                    pos = -2

        return result

    def copy_message(self, m):
        # TODO: Create a copy-on-write message instead of actually copying (?)
        if m is None:
            return {}
        else:
            return copy.copy(m)

    def _do_process(self, process, ctx, multiple):
        # TODO: When using multiple, this should allow to yield,
        # TODO: Also, this method shall be called "consume" or something, and public
        item = ctx.copy_message(ctx.start_item)
        msgs = ctx.comp.process(process, item)
        count = 0
        result = [] if multiple else None
        for m in msgs:
            count = count + 1
            if multiple:
                result.append(m)
            else:
                result = m
        return (result, count)

    def run(self, start_node, multiple=False):

        ctx = self

        if isinstance(start_node, str):
            start_node_comp = ctx.get(start_node, fail=False)
        else:
            start_node_comp = start_node

        # Launch process
        if not start_node_comp:
            logger.error("Start process '%s' not found in configuration" % start_node)
            if ctx.cli:
                sys.exit(1)
            else:
                raise Exception("Start process '%s' not found in configuration" % start_node)

        result = None
        processed = 0

        # Launch process and consume items
        try:
            logger.debug("Initializing components")
            ctx.comp.initialize(start_node_comp)

            logger.info("Processing %s" % start_node_comp)

            if ctx.profile:
                logger.warning("Profiling execution (WARNING this is SLOW) and saving results to: %s" % ctx.profile)
                cProfile.runctx("(result, processed) = self._do_process(start_node_comp, ctx, multiple=multiple)", globals(), locals(), ctx.profile)
            else:
                (result, processed) = self._do_process(start_node_comp, ctx, multiple=multiple)

            logger.debug("%s items resulted from the process" % processed)

            logger.debug("Finalizing components")
            ctx.comp.finalize(start_node_comp)

            ctx.comp.cleanup()

        except KeyboardInterrupt as e:
            logger.error("User interrupted")
            sys.exit(1)

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

    def include(self, configfile):
        configfile = self.interpolate(None, configfile)
        logger.info("Including config file: %s", configfile)
        spec = importlib.util.spec_from_file_location("configmodule", configfile)
        configmodule = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(configmodule)
        except Exception as e:
            raise ETLConfigurationException("Config include file not found: %s" % (configfile))
        configmodule.cubetl_config(self)

