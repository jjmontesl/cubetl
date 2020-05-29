# CubETL
# Copyright (c) 2013-2019 CubETL Contributors

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

import osmium

from cubetl.core import Component, Node
from multiprocessing import Queue
from threading import Thread


# Get an instance of a logger
logger = logging.getLogger(__name__)


class OsmiumHandler(osmium.SimpleHandler):

    def __init__(self, filename, queue):
        osmium.SimpleHandler.__init__(self)
        self._filename = filename
        self._queue = queue
        self._buffer_size = 1500

        self._buffer = []

    def process_object(self, o):

        if o:
            d = {
                'id': int(o.positive_id()),
                'uid': o.uid,
                'type': o.__class__.__name__,
                'user': o.user,
                'changeset': o.changeset,
                'deleted': o.deleted,
                'location': [float(o.location.lon), float(o.location.lat)] if hasattr(o, 'location') else None,
                'timestamp': o.timestamp,
                'version': o.version,
                'visible': o.visible,
                'tags': dict(o.tags)
            }

            self._buffer.append(d)

        if o is None or len(self._buffer) > self._buffer_size:
            self._queue.put(self._buffer)
            self._buffer = []

        if o is None:
            self._queue.put(None)

    def thread_run(self):
        logger.info("Processing OSM file: %s", self._filename)
        self.apply_file(self._filename)
        logger.debug("Finishing processing OSM file: %s", self._filename)
        self.process_object(None)  # This signal the consumer that the node is finished

    def node(self, n):
        #n.tags
        self.process_object(n)
    def way(self, w):
        self.process_object(w)
    def relation(self, r):
        self.process_object(r)


class OsmiumNode(Node):

    def __init__(self, filename):
        super().__init__()
        self.filename = filename
        self._queue = Queue(maxsize=5000)

    def process(self, ctx, m):

        filename = ctx.interpolate(self.filename, m)
        logger.info("Retriving info from Osmium file: %s", filename)
        self._osmium_handler = OsmiumHandler(filename, self._queue)
        self._osmium_thread = Thread(target=self._osmium_handler.thread_run)
        self._osmium_thread.start()

        finished = False
        while not finished:
            buffer = self._queue.get()

            if buffer is None:
                finished = True
            else:

                for o in buffer:

                    if o['deleted']: continue

                    #m['osm'] = o
                    m = {}
                    m['id'] = o['id']
                    #m['uid'] = o['uid']
                    m['type'] = o['type']
                    #m['user'] = o['user']
                    #m['changeset'] = o['user']
                    #m['deleted'] = o['deleted']
                    m['location'] = o['location']
                    m['timestamp'] = o['timestamp']
                    #m['version'] = o['version']
                    #m['visible'] = o['visible']

                    m['name'] = o['tags'].get('name', None)
                    m['description'] = o['tags'].get('description', None)
                    m['notes'] = o['tags'].get('notes', None)

                    m['tagkeys'] = ', '.join(o['tags'].keys())

                    #m.update(o['tags'])

                    if o['tags'] and (m['name'] or m['description']):
                        yield m


