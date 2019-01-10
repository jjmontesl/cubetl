CubETL
======

CubETL is a tool for data ETL (Extract, Transform and Load).

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
  * SDMX and PC-Axis read support
* Support for text templating, GeoIP, network queries.
* Insert / upsert for memory and SQL tables and OLAP entities.
* Extensible
* Caching

**Note**: This project is in alpha stage and is tested in few environments. Documentation
is incomplete. You will hit issues. Please use the issue tracker for bugs, questions,
suggestions and contributions.


Download / Install
------------------

While CubETL is in alpha no *pip* packages are provided, and it should be installed
using `python setup.py develop`. Using a virtualenv is recommended.

    git clone https://github.com/jjmontesl/cubetl.git
    cd cubetl

    # Using a virtualenv is usually recommended:
    python3 -m venv env
    . env/bin/activate

    # Install dependencies
    sudo apt-get install python3-dev libxml2-dev libxslt1-dev zlib1g-dev

    # Install CubETL
    python setup.py develop

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

Each CubETL configuration can contain one or more *process nodes*. You must specify
the list of configuration files (.py files), followed by the process nodes you
want to run.

Note that, when running a complete ETL process, you need to remove print nodes
or to use the `-q` command line option to remove prints to standard output, which
will otherwise heavily slowdown the process.

You can also use CubETL directly from Python code.


Examples
========


Visualizing a SQL database
--------------------------

CubETL can inspect a SQL database and generate a CubETL OLAP schema and
SQL mappings for it. Such schema can then be visualized using CubesViewer:

    # Inspect database and generate a cubes model and config
    cubetl cubetl.sql.db2sql cubetl.olap.sql2olap cubetl.cubes.olap2cubes \
        -p db2sql.db_url=sqlite:///mydb.sqlite3 \
        -p olap2cubes.cubes_model=mydb.cubes-model.json \
        -p olap2cubes.cubes_config=mydb.cubes-config.ini

    # Run cubes server (in background with &)
    pip install https://github.com/jjmontesl/cubes/archive/alias-issue.zip click flask --upgrade
    slicer serve mydb.cubes-config.ini &

    # Run a local cubesviewer HTTP server (also opens a browser)
    # NOTE: not yet available, please download and use CubesViewer manually!
    pip install cubesviewer-utils
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

Create a new directory for your ETL process and inside it run:

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
  * [Apache web server log file parsing and SQL loading in OLAP star-schema](https://github.com/jjmontesl/cubetl/tree/master/examples/loganalyzer)
  * [SDMX schema and data import](https://github.com/jjmontesl/cubetl/tree/master/examples/sdmx)
  * [PCAxis to SQL OLAP star-schema](https://github.com/jjmontesl/cubetl/tree/master/examples/pcaxis)
  * OLAP schema definition and fake data loading (fictional web shop)
  * Querying a SQL database and exporting to CSV
  * Wikipedia huge XML load
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

See the `examples/python` directory for a full working example.


Documentation
=============

* [Quickstart](https://github.com/jjmontesl/cubetl/blob/master/doc/guide/cubetl-quickstart.md)
* Configuration (includes, expressions, library)
* Sequential formats (text files, CSV, JSON...)
* Tables
* SQL
* OLAP
* Custom components and processing nodes

* Library
  * [Datetime](https://github.com/jjmontesl/cubetl/blob/master/library/datetime.py)
  * [Geo](https://github.com/jjmontesl/cubetl/blob/master/library/geo.py)
  * [Net](https://github.com/jjmontesl/cubetl/blob/master/library/net.py)
  * [HTTP](https://github.com/jjmontesl/cubetl/blob/master/library/http.py)
  * [Person](https://github.com/jjmontesl/cubetl/blob/master/library/person.py)

* [Components](https://github.com/jjmontesl/cubetl/tree/master/cubetl)

* Examples (see "Example ETL processes" above)


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
