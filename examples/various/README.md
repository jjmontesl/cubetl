# Simple examples

## Directory listing

This is process that simply lists files in the CubETL library directory.

The config file is `directorylist.py`. First see the entry points using
`cubetl directorylist.py -l`:

    $ cubetl directorylist.py -l
    2018-12-30 02:10:01,935 - INFO - Starting CubETL 1.1.0-beta
    2018-12-30 02:10:01,936 - INFO - Including config file: directorylist.py
    2018-12-30 02:10:01,958 - INFO - Processing ListConfig(cubetl.config.list)

    List of nodes in CubETL configuration:
      * cubetl.config.print  Prints current CubETL configuration.
      * cubetl.config.list  List available CubETL nodes (same as: cubetl -l).
      * cubetl.config.new  Creates a cubetl blank configuration file from a template.
      * cubetl.util.print  Prints the current message.
      * cubetl.sql.db2sql  Generate SQL schema from existing database.
      * cubetl.olap.sql2olap  Generate OLAP schema from SQL schema.
      * cubetl.cubes.olap2cubes  Generate OLAP schema from SQL schema.
      * directorylist.process

The entry point is the last one called `directorylist.process`,
call `cubetl` to run it:

    cubetl directorylist.py directorylist.process

This will produce a list of items (path entries), each of which will be printed to the output.
Each item includes 4 fields: the file path, an identifier, the separate filename
and a random number.

Try running with `-d`. Logging is now more verbose. You may wish to use this option
when debugging your own ETL processes:

    cubetl directorylist.py directorylist.process -d

When running in production, you will often want to use the `-q` command line option,
which disables nodes that print to terminal. Printing messages can be handy during development
but it is a major performance issue. If you try it, you'll see no data output:

    cubetl directorylist.py directorylist.process -q

(Of course, this does not apply if your process is writing to the standard
output as part of its normal function.)

**How it works**

The `directorylist.py` file contains a CubETL configuration. It defines a simple
ETL composed of a few nodes:

* A node that lists files in a directory and generates a data message for each.
* A node that manipulates some fields in each data message.
* A node that prints the data message.

CubETL processes are graphs that define the flow of data messages across nodes.

**Further information**

Inspect the CubETL process definition (`directorylist.py`),



