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
import inspect
import logging
import sys
import traceback

from cubetl.core import Node, ContextProperties
from cubetl.core.context import Context
import cubetl


# Get an instance of a logger
logger = logging.getLogger(__name__)


class Script(Node):

    script = None
    refs = {}

    def __init__(self):
        super(Script, self).__init__()
        self.refs = {}

    def process(self, ctx, m):

        # TODO: Cache code?

        try:
            e_locals = { "m": m, "ctx": ctx, "props": ctx.props, "var": ctx.var, "refs": self.refs, "logger": logger }
            e_globals = ctx._globals
            exec (self.code, e_locals, e_globals)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            logger.error("Error in script %s" % self)
            #print self.code
            #raise Exception('Error evaluating script at %s:\n%s' % (self, ("".join(traceback.format_exception_only(exc_type, exc_value)))) )
            raise

        yield m


class Function(Node):

    def __init__(self, function):
        super().__init__()
        self.function = function

    def process(self, ctx, m):

        # TODO: Cache code?
        self.function(ctx, m)
        yield m


class ContextScript(ContextProperties):

    #def after_properties_set(self):
    code = None

    def load_properties(self, ctx):

        try:
            e_locals = { "ctx": ctx, "props": ctx.props, "var": ctx.var, "logger": logger }
            e_globals = ctx._globals
            exec (self.code, e_locals, e_globals)
        except (Exception) as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            #logger.error("Error evaluating script")
            raise Exception('Error evaluating script at %s: %s' % (self, ("".join(traceback.format_exception_only(exc_type, exc_value)))) )
            #raise


class Eval(Node):
    """
    Note that evaluation is done via ctx.interpolate(), and so
    requires expressions to be delimited by ${}.
    """

    def __init__(self, eval=None):

        super().__init__()

        self.eval = eval or []

    def initialize(self, ctx):
        super(Eval, self).initialize(ctx)

        if (hasattr(self, "mappings")):
            raise Exception("%s config contains a mappings element which is not allowed (use 'eval' element)" % self)


    @staticmethod
    def process_evals(ctx, m, evals, data = {}):

        # Accept a single dict as eval (convert to list)
        if (isinstance(evals, dict)):
            evals = [ evals ]

        if (len(evals) == 0):
            if (m != data):
                m.update(data)

        else:

            for evalitem in evals:

                if isinstance(evalitem, dict):
                    m.update(evalitem)
                    return

                elif ("value" in evalitem):
                    try:
                        m[evalitem["name"]] = ctx.interpolate(evalitem["value"], m, data)
                    except Exception as e:
                        if "except" in evalitem:
                            m[evalitem["name"]] = ctx.interpolate(evalitem["except"], m, data)
                        else:
                            raise
                else:
                    print(evalitem)
                    if evalitem["name"] in data:
                        m[evalitem["name"]] = data[evalitem["name"]]
                    else:
                        if (not "default" in evalitem):
                            logging.warn("Mapping with no value and no default: %s" % evalitem)
                        #m[evalitem["name"]] = None

                if ("default" in evalitem):
                    if (not m[evalitem["name"]]):
                        m[evalitem["name"]] = ctx.interpolate(evalitem["default"], data)


    def process(self, ctx, m):

        Eval.process_evals(ctx, m, self.eval)

        yield m


class Delete(Node):
    """
    """

    def __init__(self, fields):

        super().__init__()

        self.fields = fields

    def process(self, ctx, m):
        for field in self.fields:
            try:
                m.pop(field)
            except:
                pass

        yield m

