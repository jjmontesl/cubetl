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

from cubetl.core import Node, ConsumerNode
from multiprocessing import Process, Queue, Value
import queue

# Get an instance of a logger
logger = logging.getLogger(__name__)


class MultiProcess(ConsumerNode):
    """
    """

    def __init__(self, node, processes=3):
        super().__init__()
        self.num_processes = processes
        self.node = node

        self._queue_input = None
        #self._queue_input_batches = None
        self._queue_output = None
        self._processes = None
        self._finish_process = False

    def initialize(self, ctx):
        super().initialize(ctx)
        # TODO: Do N deep copies of the node (meanwhile use only thread-safe nodes)?
        ctx.comp.initialize(self.node)

        self._queue_input = Queue(maxsize=32)
        #self._queue_input_batches = Queue(maxsize=128)
        self._queue_output = Queue()

        # Start threads
        self._processes = []
        self._finish_process = Value("B", 0)
        logger.info("Starting threads for node: %s", self)
        for i in range(self.num_processes):
            process = Process(target=self.multiprocess_worker, args=(ctx, ))
            process.start()
            self._processes.append(process)
        #thread.join()

        #thread = Thread(target=self.threaded_consumer, args=(ctx, ))
        #thread.start()
#
    def finalize(self, ctx):
        logger.info("%s finalizing %d subprocesses.", self, len(self._processes))
        self._finish_process.value = 1
        for p in self._processes:
            p.join(3.0)  # p.join(2.0)
            p.terminate()
            p.join()

        ctx.comp.finalize(self.node)
        super().finalize(ctx)

    '''
    def process(self, ctx, m):
        """
        """
        pass
        # As with any node, we just need to block until there is one output message to yield
        # How to consume input in parallel? we need to be called concurrently
        # we actually need to extract messages from the previous chain quicker.
        # OR: reconsider message flow routing.
    '''

    def multiprocess_worker(self, ctx):
        while self._finish_process.value == 0:
            try:
                input_msg = self._queue_input.get(timeout=1.0)
            except queue.Empty:
                input_msg = None

            if not input_msg:
                continue

            result_msgs = ctx.consume(self.node, [input_msg])
            for m in result_msgs:
                if self._finish_process.value == 1:
                    return

                retry = True
                while retry and self._finish_process.value == 0:
                    try:
                        self._queue_output.put(m, timeout=2.0)
                        retry = False
                    except queue.Full as e:
                        retry = True


    def consume(self, ctx, msgs):

        def _consume_iter(msgs):

            while self._finish_process.value == 0:
                msg_in = next(msgs)
                self._queue_input.put(msg_in)

                process_out = True
                while process_out and self._finish_process.value == 0:
                    try:
                        msg_out = self._queue_output.get_nowait()
                    except queue.Empty:
                        process_out = False
                        msg_out = None
                    if msg_out:
                        yield msg_out

        return _consume_iter(msgs)




