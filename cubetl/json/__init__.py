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


from os.path import isfile, join
from past.builtins import basestring
import StringIO
import itertools
import json
import logging
import re

from cubetl.core import Node
from cubetl.fs import FileReader, FileWriter


# Get an instance of a logger
logger = logging.getLogger(__name__)


class JsonReader(Node):
    """
    Processes JSON data, returning it as a Python dictionary inside the attribute specified by
    `name`. If no name is defined, JSON object attributes are flattened on the message (if the
    result is a JSON dictionary).

    If the result is a list, it will be iterated generating one message for each
    item. This can be avoided using `iterate: False`, in this case a `name` is required.

    .. code-block:: javascript

        - !!python/object:cubetl.json.JsonReader
          data: ${ m["data"] }
          name: null
          iterate: True

    """

    name = None
    data = '${ m["data"] }'
    iterate = True  # only if is an array

    def process(self, ctx, m):

        logger.debug("Processing JSON data at %s" % self)

        # Resolve data
        data = ctx.interpolate(self.data, m)

        result = json.loads(data)
        if isinstance(result, list) and self.iterate:
            for item in result:
                m2 = ctx.copy_message(m)
                if self.name:
                    m2[self.name] = item
                else:
                    if not isinstance(item, dict):
                        raise Exception("Cannot merge a non dictionary value (%s) with current message in %s (use 'name' to assign the object to a message property)" % (str(item), self))
                    m2.extend(item)
                yield m2
        else:
            if self.name:
                m[self.name] = result
            else:
                if not isinstance(item, dict):
                    raise Exception("Cannot merge a non dictionary value (%s) with current message in %s (use 'name' to assign the object to a message property)" % (str(item), self))
                m.extend(item)
            yield m


class JsonFileReader (JsonReader):
    """
    This class is a shortcut to a FileReader and JsonReader
    """

    # TODO: This and CSVFileReader should possibly extend FileReader (in streaming mode, if appropriate)

    data = '${ m["_jsondata"] }'
    path = None

    def initialize(self, ctx):

        super(JsonFileReader, self).initialize(ctx)

        self._fileReader = FileReader()
        self._fileReader.path = self.path
        self._fileReader.name = "_jsondata"
        self._fileReader.encoding = None
        self.data = '${ m["_jsondata"] }'
        ctx.comp.initialize(self._fileReader)

    def finalize(self, ctx):
        ctx.comp.finalize(self._fileReader)
        super(JsonFileReader, self).finalize(ctx)

    def process(self, ctx, m):

        logger.debug("Reading and processing JSON file %s at %s" % (self.path, self))

        file_msg = ctx.comp.process(self._fileReader, m)
        for mf in file_msg:
            m2 = ctx.copy_message(m)
            json_rows = super(JsonFileReader, self).process(ctx, m2)
            for json_row in json_rows:
                del(json_row['_jsondata'])
                yield json_row


class JsonFileWriter(FileWriter):

    data = '${ m }'
    fields = None
    multiple = True
    sort_keys = True
    indent = 4
    _count = 0

    def initialize(self, ctx):
        super(JsonFileWriter, self).initialize(ctx)
        self.newline = False

    def finalize(self, ctx):
        super(JsonFileWriter, self).finalize(ctx)

    def _csv_row(self, ctx, row):

        if self.encoding:
            row = [(r.encode(self.encoding) if isinstance(r, basestring) else r) for r in row]

        self._csvwriter.writerow(row)
        result = self._output.getvalue()
        self._output.truncate(0)
        return result

    def on_open(self):
        if (self.multiple):
            self._open_file.write("[")

    def on_close(self):
        if (self.multiple):
            self._open_file.write("]")

    def process(self, ctx, m):

        self._count = self._count + 1

        data = ctx.interpolate(self.data, m)

        if self.fields:
            o = {}
            for f in self.fields:
                o[f] = data[f]
        else:
            o = data

        value = json.dumps(o, sort_keys = self.sort_keys, indent = self.indent) # ensure_ascii, check_circular, allow_nan, cls, separators, encoding, default, )
        if (self.multiple and self._count > 1):
            value = ", " + value
        super(JsonFileWriter, self).process(ctx, m, value)

        yield m



