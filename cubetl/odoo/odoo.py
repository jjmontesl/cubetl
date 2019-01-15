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


from xmlrpc import client
import logging

from cubetl.core import Component, Node


# Get an instance of a logger
logger = logging.getLogger(__name__)


class OdooConnection(Component):

    def __init__(self, url, database, username, password):
        self.url = url or "http://localhost:8069/"
        self.database = database  # odoo
        self.username = username  # admin
        self.password = password  # admin

        self._conn = None
        self._uid = None
        self._connected = False

    def _get_conn(self):
        if not self._connected:
            self.connect()
        return self._conn

    def connect(self):

        # Get the uid
        logger.info("Connecting to Odoo instance [url=%s, database=%s, username=%s]", self.url, self.database, self.username)
        sock_common = client.ServerProxy(self.url + '/xmlrpc/common', allow_none=True)
        self._uid = sock_common.login(self.database, self.username, self.password)
        self._conn = client.ServerProxy(self.url + '/xmlrpc/object', allow_none=True)

        self._connected = True

        logger.info("Connected to Odoo instance")

        return self._conn


    def execute(self, objname, method, *args):

        return self._get_conn().execute(self.database, self._uid, self.password, objname, method, *args)

    def workflow(self, objname, method, *args):

        return self._get_conn().exec_workflow(self.database, self._uid, self.password, objname, method, *args)


class Execute(Node):

    def __init__(self, conn, objname, method, args):
        self.conn = conn
        self.objname = objname
        self.method = method
        self.args = args


class List(Node):

    def __init__(self, conn, objname):
        self.conn = conn
        self.objname = objname
