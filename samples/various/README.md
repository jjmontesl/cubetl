# Simple examples

## Directory listing

This is a simple process that

    cubetl directorylist.py -l

The entry point is called `directorylist.process`, call `cubetl` to run it.

    cubetl directorylist.py directorylist.process

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



