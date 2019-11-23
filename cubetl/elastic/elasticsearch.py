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

from cubetl.core import Component, Node
from elasticsearch import Elasticsearch
import datetime

# Get an instance of a logger
logger = logging.getLogger(__name__)


class ElasticsearchConnection(Component):

    def __init__(self, url):
        super().__init__()
        self.url = url or "http://localhost:9200"
        #self.username = username  # admin
        #self.password = password  # admin

        self._conn = None
        self._uid = None
        self._connected = False

    def conn(self):
        if not self._connected:
            self.connect()
        return self._conn

    def connect(self):
        logger.info("Connecting to Elasticsearch instance [url=%s]", self.url)
        self._conn = Elasticsearch(self.url)  # , self.username, self.password)
        self._connected = True
        logger.debug("Connected to Elasticsearch instance")

    def index(self, index, doc_type, data_id, data):
        #doc = {
        #    'author': 'kimchy',
        #    'text': 'Elasticsearch: cool. bonsai cool.',
        #    'timestamp': datetime.datetime.now(),
        #}
        res = self.conn().index(index=index, doc_type=doc_type, id=data_id, body=data)
        #print(res['result'])  # created, updated

    def get(self, index, doc_type, data_id):

        res = self.conn().get(index=index, doc_type=doc_type, id=data_id)
        print(res['_source'])

    def search(self, index, query):
        logger.debug("Elasticsearch search [index=%s]", index)
        res = self.conn().search(index=index, body={'size': 10000})  # {"query": {"match_all": {}}})
        #logger.info("Got %d Hits:" % res['hits']['total'])
        for hit in res['hits']['hits']:
            #print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])
            yield hit

    def index_delete(self, index):
        self.conn().indices.delete(index=index, ignore=[400, 404])

    def index_create(self, index):
        self.conn().indices.create(index=index, ignore=[400])

    def index_refresh(self, index):
        self.conn().indices.refresh(index=index)


class Index(Node):

    def __init__(self, es, index, doc_type, data_id):
        super().__init__()
        self.es = es
        self.index = index
        self.doc_type = doc_type
        self.data_id = data_id

    def process(self, ctx, m):

        index = ctx.interpolate(self.index, m)
        doc_type = ctx.interpolate(self.doc_type, m)
        data_id = ctx.interpolate(self.data_id, m)
        self.es.index(index, doc_type, data_id, m)

        yield m


class Search(Node):

    def __init__(self, es, index, query):
        super().__init__()
        self.es = es
        self.index = index
        self.query = query

    def process(self, ctx, m):
        index = ctx.interpolate(self.index, m)
        res = self.es.search(index=index, query=None)
        for item in res:
            m2 = ctx.copy_message(m)
            m2.update(item)
            yield m2

