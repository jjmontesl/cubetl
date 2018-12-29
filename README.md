CubETL
======

CubETL is a framework and related tools for data ETL (Extract, Transform and Load),
based in Python.

CubETL provides a mechanism to run data items through a processing pipeline. It takes care
of initializing only the components used by the process, manages the data flow across
the process graph, logging and cleanup.

It provides out-of-the-box components that handle common data formats
and it also includes SQL and OLAP modules that understand SQL and OLAP schemas
and map data across them. This allows CubETL to insert OLAP facts across multiple tables
in a single store operation, while automatically performing (and caching) the appropriate lookups.

CubETL can also analyze an existing relational database and generate an OLAP schema, and
the other way around: generate an SQL schema from an OLAP schema. It can also produce
a Python Cubes server model. All together it allows for a quick analytical inspection
of an arbitrary database (see the examples below).

Features:

* Consumes and produces CSV, XML, JSON...
* SQL support (querying, inserting/updating, schema creation, schema loading)
* OLAP support:
  * Star-schema generation and data loading
  * SQL-to-OLAP schema generator
  * Cubes OLAP Server model export
* Support for text templating, GeoIP, network queries
* Insert / upsert for memory and SQL tables and OLAP entities.
* Extensible
* Caching

See the complete [CubETL component list]().

**Note**: This project is in alpha stage and is tested in few environments. You will hit issues:
please use the issue tracker for bugs, questions, suggestions and contributions.


Download / Install
------------------

While CubETL is in development, no *pip* packages are provided:

In your target environment (requires Python 3.5+):

    git clone https://github.com/jjmontesl/cubetl.git
    cd cubetl
    python3 -m venv env
    . env/bin/activate
    python setup.py install  # or: python setup.py develop

Test:

    cubetl -h

Usage
-----

Cubetl provides a command line tool, `cubetl`:

    cubetl [-dd] [-q] [-h] [-p property=value] [-m attribute=value] [config.py ...] <start-node>

        -p   set a context property
        -m   set an attribute for the start item
        -d   debug mode (can be used twice for extra debug)
        -q   quiet mode (bypass print nodes)
        -l   list config nodes ('cubetl.config.list' as start-node)
        -h   show this help and exit
        -v   print version and exit


You can also use CubETL directly from Python code.


Examples
========


Visualizing a SQL database
--------------------------

CubETL can inspect a SQL database and generate a CubETL OLAP schema and
SQL mappings for it. Such schema can then be visualized using CubesViewer:

    # For this example you need these dependencies:
    pip install cubes cubesviewer-utils  # and cubetl

    # Inspect database and generate a cubes model and config
    cubetl cubetl.sql.db2sql cubetl.olap.sql2olap cubetl.cubes.olap2cubes \
        -p db2sql.db_url=sqlite:///mydb.sqlite3 \
        -p olap2cubes.cubes_model=mydb.cubes-model.json \
        -p olap2cubes.cubes_config=mydb.cubes-config.ini

    # Run cubes server (in background with &)
    slicer serve mydb.cubes-config.ini &

    # Runs a local cubesviewer HTTP server and opens a browser
    cvutils cv

This will open a browser pointing to a local CubesViewer instance pointing to the
previously launched Cubes server. Alternatively, you can download CubesViewer and
load the HTML application locally.

The CubETL project contains an example database that you can use to test this (see the
[Generate OLAP schema from SQL database and visualize](https://github.com/jjmontesl/cubetl/tree/master/examples/sql2olap)
example below).

You can control the schema generation process using with options. Check the documentation
below for further information.


Creating a new ETL process config
---------------------------------

Create a directory for your ETL process and run:

    cubetl cubetl.config.new -p config.name=myprocess

This will create a new file `myprocess.py`, which you can use as a template
for your new ETL process.

The created example config includes an entry node called *myprocess.process*,
which simply prints a message to console. You can test your ETL process using:

    cubetl myprocess.py myprocess.process

See the example ETL below fore more examples, the documentation section
for information about how to define ETL processes.


Example ETL processes
---------------------

Example ETL processes included with the project:

  * [Simple CubETL process (local directory list)](https://github.com/jjmontesl/cubetl/tree/master/examples/various)
  * [Generate OLAP schema from SQL database and visualize in CubesViewer](https://github.com/jjmontesl/cubetl/tree/master/examples/sql2olap)
  * OLAP schema definition, SQL generation and random data load (fictional web shop)
  * [Apache web server log file parsing and SQL loading in OLAP star-schema](https://github.com/jjmontesl/cubetl/tree/master/examples/loganalyzer)
  * [PCAxis to SQL OLAP star-schema](https://github.com/jjmontesl/cubetl/tree/master/examples/pcaxis)
  * Querying a SQL database and exporting to CSV
  * Wikipedia huge XML load
  * Importing SDMX data
  * HTML scraping

To run these examples you'll need the *examples* directory of the *cubetl* project, which
is not included in the PyPI *pip* download. You can get them by cloning the cubetl
project repository (`git clone https://github.com/jjmontesl/cubetl.git`) or
by downloading the packaged project.


Running from Python
-------------------

In order to configure and/or run a process from client code, use:

    import cubetl

    # Create Cubetl context
    ctx = cubetl.cubetl()

    # Add components or include a configuration file...
    ctx.add('your_app.node_name', ...)

    # Launch process
    result = ctx.run("your_app.node_id")

See the examples/python to see a full working example.


Documentation
=============

* Quick Start (cubetl and cubeutil)

* Usage
  * Introduction and basics (components and nodes)
  * Creating and running ETL processes
  * Running CubETL
  * Running from Python
  * Configuration files
  * Process flow
  * Expressions (message, context, ternary operator...)

* Component Reference


Support
=======

If you have questions, problems or suggestions, please use:

* Report bugs: https://github.com/jjmontesl/cubetl/issues

If you are using or trying CubETL, please tweet #cubetl.

Source
======

Github source repository: https://github.com/jjmontesl/cubetl

Authors
=======

CubETL is written and maintained by Jose Juan Montes.

See AUTHORS file for more information.

License
=======

CubETL is published under MIT license. For full license see the LICENSE file.

Other sources:

* Country list from: http://www.geonames.org (CC-A 3.0)

