CubETL
======

CubETL is a framework and related tools for data ETL (Extract, Transform and Load),
based in Python.

CubETL provides a mechanism to run data items through a processing pipeline. It takes care
of initializing only the components that are in use, managing the data flow across
the process graph, logging, performance metrics and cleaning up.

It provides several nodes out of the box that can deal with many common formats,
and it also includes SQL and OLAP modules that can handle SQL and OLAP schemas
and map data across them. This allows to include OLAP facts across multiple tables
in a single store operation, performing the appropriate lookups (and caching).

CubETL can also analyze an existing relational database and generate an OLAP schema, and
the other way around: generate an SQL schema from an OLAP schema. It can also produce
a Python Cubes server model. All together allows for a quick analytical inspection of an
arbitrary database (see the `cubeutil` tool examples below).

Features:

* Consumes and produces CSV, XML, JSON...
* SQL support (querying, inserting/updating)
* OLAP support:
  * Star-schema generation and data loading
  * SQL-to-OLAP schema generator
  * Cubes OLAP Server model export
* Support for text templating, GeoIP, network queries
* Extensible
* Caching

See the complete [CubETL feature list]().


Download / Install
------------------

In your target environment:

    pip install cubetl

As with most tools, it is recommended to use a virtualenv:

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
    # Run cubes server
    slicer serve cup.slicer.ini &
    # Run cubesviewer
    cubext cv

This will open a browser pointing to a local CubesViewer instance pointing to the previously
launched Cubes server.

You can control the schema generation passing options. Check the documentation for more information.


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
  * Introduction
  * Running CubETL
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

