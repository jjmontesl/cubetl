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


import logging
import os
import time

from cubetl.core import Node
from cubetl.text.functions import parsebool


# Get an instance of a logger
logger = logging.getLogger(__name__)


class Log(Node):

    LEVEL_DEBUG = logging.DEBUG
    LEVEL_INFO = logging.INFO
    LEVEL_WARN = logging.WARN
    LEVEL_WARNING = logging.WARN
    LEVEL_ERROR = logging.ERROR
    LEVEL_FATAL = logging.FATAL
    LEVEL_CRITICAL = logging.CRITICAL

    def __init__(self, message, condition=None, level=LEVEL_INFO, once=False):
        super().__init__()

        self.condition = condition
        self.message = message
        self.level = level
        self.once = once

        self.count = 0

    def process(self, ctx, m):

        self.count = self.count + 1

        dolog = True
        if (self.condition):
            dolog = parsebool(ctx.interpolate(self.condition, m))

        if dolog and (not self.once or self.count == 1):
            logger.log(self.level, ctx.interpolate(self.message, m))

        yield m


class LogPerformance(Node):
    """
    """

    interval = 10

    _count = 0
    _startTime = time.time()
    _lastTime = time.time()
    _lastCount = 0

    def __init__(self, name="Performance"):
        super().__init__()
        self.name = name
        self._startTime = time.time()
        self._lastTime = time.time()

    def memory_usage_psutil(self):
        # return the memory usage in MB

        import psutil
        process = psutil.Process(os.getpid())
        if hasattr(process, 'get_memory_info'):
            mem = process.get_memory_info()[0] / float(2 ** 20)
        elif hasattr(process, 'memory_info'):
            mem = process.memory_info()[0] / float(2 ** 20)
        else:
            mem = 0
        return mem

    def finalize(self, ctx):

        super(LogPerformance, self).finalize(ctx)

        current = time.time()
        logger.debug("Context expression cache usage - size: %d evictions: %d hits/misses: %d/%d" %
                    (ctx._compiled.size, ctx._compiled.evictions, ctx._compiled.hits, ctx._compiled.misses))
        logger.info("%s - Total time: %d  Total messages: %d  Global rate: %.3f msg/s" % (
                    self.name,
                    current - self._startTime,
                    self._count,
                    float(self._count) / (current - self._startTime)
                    ))

    def loginfo(self, ctx):
        import psutil
        current = time.time()
        logger.info("%s - Time: %d Mem: %.3f MB Messages: %d (+%d) Rate global: %.3f msg/s Rate current: %.3f msg/s" % (
             self.name,
             current - self._startTime,
             self.memory_usage_psutil(),
             self._count,
             self._count - self._lastCount,
             float(self._count) / (current - self._startTime),
             float(self._count - self._lastCount) / (current - self._lastTime)
             ))

    def process(self, ctx, m):

        if (self._count == 0):
            _startTime = time.time()
            _lastTime = time.time()

        self._count = self._count + 1

        current = time.time()
        if (current - self._lastTime > self.interval):
            self.loginfo(ctx)
            self._lastTime = current
            self._lastCount = self._count

        yield m

