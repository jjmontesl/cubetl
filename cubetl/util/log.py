import logging
from os import listdir
from os.path import isfile, join
import itertools
import re
from cubetl.core import Node
import time
from cubetl.text.functions import parsebool

import psutil
import os

# Get an instance of a logger
logger = logging.getLogger(__name__)


class Log(Node):

    condition = None
    message = "MESSAGE NOT CONFIGURED - Check Log node configuration"
    level = logging.INFO

    count = 0

    def process(self, ctx, m):

        self.count = self.count + 1

        dolog = True
        if (self.condition):
            dolog = parsebool(ctx.interpolate(m, self.condition))

        if dolog:
            logger.log(self.level, ctx.interpolate(m, self.message))

        yield m


class LogPerformance(Node):
    """
    """

    interval = 10

    _count = 0
    _startTime = time.time()
    _lastTime = time.time()
    _lastCount = 0

    def __init__(self):
        self._startTime = time.time()
        self._lastTime = time.time()

    def memory_usage_psutil(self):
        # return the memory usage in MB
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
        logger.info("Total time: %d  Total messages: %d  Global rate: %.3f msg/s" % (
                    current - self._startTime,
                    self._count,
                    float(self._count) / (current - self._startTime)
                    ))

    def loginfo(self, ctx):
        current = time.time()
        logger.info("Time: %d Mem: %.3f MB Messages: %d (+%d) Rate global: %.3f msg/s Rate current: %.3f msg/s" % (
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

