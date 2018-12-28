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


import importlib.util

import getopt
import logging
import os
import pprint
import sys
import traceback

from cubetl import APP_NAME_VERSION, util, flow
from cubetl.core import ContextProperties
from cubetl.core.context import Context
import cubetl
from cubetl.util import config, log
from cubetl.olap import sqlschema
from cubetl.sql import schemaimport
from cubetl.sql.sql import Connection
from cubetl.cubes import cubes10


# Get an instance of a logger
logger = logging.getLogger(__name__)


class Bootstrap:
    """
    This class takes care of CubETL command line tool bootstrapping: configure logging,
    processing arguments and triggering the ETL process entry node.
    """

    def configure_logging(self, ctx):

        # In absence of file config
        default_level = logging.INFO if ctx.debug is False else logging.DEBUG
        if not ctx.debug:
            logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=default_level)
        else:
            #logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=default_level)
            logging.basicConfig(format='%(asctime)s - %(module)s - %(levelname)s - %(message)s', level=default_level)

        # With file config:
        #logging.config.fileConfig('logging.conf')

    def usage(self):
        print("cubetl [-dd] [-q] [-h] [-r filename] [-p property=value] [-m attribute=value] [config.py ...] <start-node>")
        print("")
        print("    -p   set a context property")
        print("    -m   set an attribute for the start item")
        print("    -d   debug mode (can be used twice for extra debug)")
        print("    -q   quiet mode (bypass print nodes)")
        print("    -r   profile execution writing results to filename")
        print("    -l   list config nodes ('cubetl.config.list' as start-node)")
        print("    -h   show this help and exit")
        print("    -v   print version and exit")
        print("")
        print("  Builtin entry points: ")
        print("      cubetl.config.print  Print configuration.")
        print("      cubetl.config.list   List configured components.")
        print("")

    def _split_keyvalue(self, text):
        """Return key=value pair, or key=None if format is incorrect
        """
        if (text.find('=') < 0):
            return (None, text)
        else:
            return (text[ : text.find('=')], text[text.find('=') + 1 : ])

    def parse_args(self, ctx):

        try:
            opts, arguments = getopt.gnu_getopt(ctx.argv, "p:m:r:dqhvl", [ "help", "version"])
        except getopt.GetoptError as err:
            print(str(err))
            self.usage()
            sys.exit(2)

        list_nodes = False
        for o, a in opts:
            if o in ("-h", "--help"):
                self.usage()
                sys.exit(0)
            if o in ("-v", "--version"):
                print(APP_NAME_VERSION)
                sys.exit(0)
            if o == "-d":
                if (ctx.debug):
                    ctx.debug2 = True
                else:
                    ctx.debug = True
            elif o == "-q":
                ctx.quiet = True
            elif o == "-r":
                ctx.profile = a
            elif o == "-l":
                list_nodes = True
            elif o == "-p":
                (key, value) = self._split_keyvalue(a)
                if (key == None):
                    print("Invalid property key=value definition (%s)" % (value))
                    self.usage()
                    sys.exit(2)
                logger.debug("Setting context property from command line: %s = %s" % (key, value))
                ctx.props[key] = value
            elif o == "-m":
                (key, value) = self._split_keyvalue(a)
                if (key == None):
                    print("Invalid attribute key=value definition (%s)" % (value))
                    self.usage()
                    sys.exit(2)
                logger.debug("Setting item attribute from command line: %s = %s" % (key, value))
                ctx.start_item[key] = value

        for argument in arguments:
            if (argument.endswith('.py')):
                ctx.config_files.append(argument)
            else:
                ctx.start_nodes.append(argument)

        if list_nodes:
            ctx.start_nodes.append("cubetl.config.list")

        if not ctx.start_nodes:
            print("One starting node must be specified, but none found.")
            self.usage()
            sys.exit(2)


    def init(self, argv=None, cli=False, debug=False):

        # Set up context
        ctx = Context()
        ctx.argv = argv
        ctx.cli = cli

        if not ctx.cli:
            ctx.argv = []
            ctx.start_nodes = ["_dummy"]

        # Set library dir
        # FIXME: Fix this so it works with setup.py/others installatiob
        base_dir = os.path.abspath(os.path.dirname(os.path.abspath(__file__)) + "/../../")
        ctx.props['dir_lib'] = base_dir + "/library"

        ctx.debug = debug

        # Parse arguments
        self.parse_args(ctx)

        # Init logging
        self.configure_logging(ctx)
        logger = logging.getLogger(__name__)
        logger.info("Starting %s" % cubetl.APP_NAME_VERSION)
        logger.debug("Debug logging level enabled")

        # TODO: Character encoding considerations? warnings?

        # Load default config
        self.default_config(ctx)

        return ctx

    def start(self, argv):

        # Initialize context
        ctx = self.init(argv, cli=True)

        # Read config
        for configfile in ctx.config_files:
            ctx.include(configfile)

        # Run
        start_nodes = []
        for start_node_name in ctx.start_nodes:
            try:
                start_node = ctx.get(start_node_name)
                start_nodes.append(start_node)
            except:
                logger.error("Start node '%s' not found in config." % start_node_name)
                print("Start node '%s' not found in config." % start_node_name)
                sys.exit(1)

        for start_node in start_nodes:
            ctx.run(start_node)

    def default_config(self, ctx):

        ctx.add('cubetl.config.print', config.PrintConfig(),
                description="Prints current CubETL configuration.")
        ctx.add('cubetl.config.list', config.ListConfig(),
                description="List available CubETL nodes (same as: cubetl -l).")
        #ctx.add('cubetl.config.new', config.CreateTemplateConfig())
        ctx.add('cubetl.util.print', util.PrettyPrint(),
                description="Prints the current message.")

        ctx.add('cubetl.sql.db2sql',
                schemaimport.DBToSQL(connection=Connection(url="${ ctx.props['db2sql.db_url'] }")),
                description="Generate SQL schema from existing database.")

        ctx.add('cubetl.olap.sql2olap', sqlschema.SQLToOLAP(),
                description="Generate OLAP schema from SQL schema.")
        #ctx.add('cubetl.olap.mappings', sqlschema.PrintMappings(),
        #        description="Show all OLAP mappings in the defined schema.")

        ctx.add('cubetl.cubes.olap2cubes',
                cubes10.Cubes10ModelWriter(olapmapper="${ ctx.get('sql2olap.olapmapper') }",
                                           model_path="${ ctx.props.get('olap2cubes.cubes_model', None) }",
                                           config_path="${ ctx.props.get('olap2cubes.cubes_config', None) }"),
                description="Generate OLAP schema from SQL schema.")
