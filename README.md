CubETL
======

CubETL is a framework and related tools for data ETL (Extract, Transform and Load),
based in Python.

CubETL provides a mechanism to run data items through a processing pipeline. It takes care
of initializing only the components used by the process, managing the data flow across
the process graph, logging, performance metrics and cleaning up.

It provides out-of-the-box components that can deal with common data formats,
and it also includes SQL and OLAP modules that can handle SQL and OLAP schemas
and map data across them. This allows to insert OLAP facts across multiple tables
in a single store operation, doing (and caching) the appropriate lookups.

CubETL can also analyze an existing relational database and generate an OLAP schema, and
the other way around: generate an SQL schema from an OLAP schema. It can also produce
a Python Cubes server model. All together it allows for a quick analytical inspection
of an arbitrary database (see the examples below).

Features:

* Consumes and produces CSV, XML, JSON...
* SQL support (querying, inserting/updating)
* OLAP support:
  * Star-schema generation and data loading
  * SQL-to-OLAP schema generator
  * Cubes OLAP Server model export
* Support for text templating, GeoIP, network queries
* Insert / upsert for memory and SQL tables and OLAP entities.
* Extensible
* Caching

See the complete [CubETL feature list]().

**Note**: I built CubETL for my data transformation needs, and shared it once I thought
it could be useful. The project is very young and tested in few environments.
You may hit issues: please use the issue tracker for bugs, questions and suggestions!


Download / Install
------------------

In your target environment (requires Python 3.5+ and pip3):

    pip install cubetl

Note: as with most tools, it is recommended to use a virtualenv *first*:

    # Create virtualenv (run first time only)
    python3 -m venv env
    # Activate virtualenv
    . env/bin/activate



Usage
-----

Cubetl provides two command line tools:

* `cubetl` runs cvETL ETL processes.
* `cubeutil` is a helper tool for some typical operations.

You can also use CubETL directly from Python code.

See the Documentation section below for further information.


Examples
========


Visualizing a SQL database
--------------------------

CubETL can inspect a SQL database and generate a CubETL OLAP schema and
SQL mappings for it. Such schema can then be visualized using CubesViewer:

    # Inspect database and generate a Cubes model and config
    cubext sql2olap --cubes-model mydb.model.json --cubes-slicer mydb.slicer.ini sqlite:///mydb.sqlite3
    cubetl cubetl.sql2olap \
        -p cubes_model=mydb.cubes-model.json \
        -p cubes_config=mydb.cubes-config.ini \
        -p db_url=sqlite:///mydb.sqlite3

    # Run cubes server
    slicer serve mydb.slicer.ini &

    # Run cubesviewer
    cubetl cubetl.cv

This will open a browser pointing to a local CubesViewer instance pointing to the previously
launched Cubes server.

You can control the schema generation passing options. Check the documentation for more information.


Creating a new ETL process config
---------------------------------

Create a directory for your ETL process and run:

    cubetl cubetl.config.new -i myprocess.py

This will create a new file `myprocess.py`, which you can use as a template
for your new ETL process.

The created example config includes an entry node called *myprocess.process*,
which simply prints a message to console. You can test your ETL process using:

    cubetl myprocess.py myprocess.process

See the example ETL below fore more examples, the documentation section
for information about how to define ETL processes.


Example ETL processes
---------------------

Check the following example ETL processes

  * Simple CubETL process (local directory list)
  * PCAxis to SQL OLAP star-schema (Spanish census)
  * OLAP schema definition, SQL generation and random data load (fictional web shop)
  * Apache web server log file parsing and SQL loading in OLAP star-schema
  * Wikipedia huge XML load to SQL star schema
  * Querying and exporting to CSV

To run these examples you'll need the *examples* directory of the *cubetl* project, which
is not included in the PyPI *pip* download. You can get them by cloning the cubetl
project repository (`git clone https://github.com/jjmontesl/cubetl.git`) or
by downloading the packaged project.


Running from Python
-------------------

In order to configure and/or run a process from client code, use:

    from cubetl.core.bootstrap import Bootstrap

    # Create Cubetl context
    bootstrap = Bootstrap()
    ctx = bootstrap.init()
    ctx.debug = True

    # Extra configuration

    # Add components ...
    comp = ...
    cubetl.container.add_component(comp)

    # Launch process
    ctx.start_node = "your_app.node_id"
    result = bootstrap.run(ctx)


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

CubETL is published under MIT license.

For full license see the LICENSE file.

Other sources:

* Country list from: http://www.geonames.org (CC-A 3.0)

