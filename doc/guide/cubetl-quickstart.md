# CubETL - Introduction

CubETL allows you to define processes that retrieve data from different sources,
manipulate it, and writes it in the same or different formats. This is usually known
as an ETL process (for Extract, Transform and Load).

It does so by handling data as separate items (eg. rows in a database, lines in a text file).

In CubETL, data items are called "**messages**" (sometimes simply "items"). Messages are a
simple key-value structures which hold any data your process needs to handle.

## CubETL processes

A process is defined by a chain of processing **nodes** that can take a message message and
transform it, or produce any number of derived messages. These are then processed by the
next step of the chain.

For example, a simple process that lists files in a directory writes them to a CSV file
would look like:

![DirectoryList-to-CSV process](https://raw.githubusercontent.com/jjmontesl/cubetl/master/doc/img/diagrams/directorylist-to-csv.plantuml.svg "DirectoryList-to-CSV process")


## CubETL context and components

CubETL processes are configured inside a **context**, which holds the configuration for
one or more processes.

A CubETL context keeps named references to **components** and **nodes**. Components represent resources and
nodes generate and process *messages*.

| Component Examples | Node Examples |
|--------------------|---------------|
| sql.Connection     | sql.Query     |
| sql.SQLTable       | util.Print    |
| olap.Dimension     | csv.CsvWriter |
| ...                | flow.Chain    |
|                    | ...           |

CubETL provides tools and classes that handle common ETL needs, including common facilities
like logging and caching and dedicated modules for RegExp, XML, CSV, JSON, SQL, and OLAP
schemas and data stores.

Within this framework, you can define your own transformation processes.


## Creating an ETL process

(Note: The example in this section can be found in the `examples/various/directorycsv.py` file).

In order to define the ETL process, choose a directory for your project and
create a `directorycsv.py` file with the following content:

    from cubetl import text, flow, fs, script, util

    def cubetl_config(ctx):
        # Your CubETL components configuration goes here

The `cubetl_config` must exist and accept a `ctx` argument. When CubETL loads configuration files,
it calls this method in order to setup the process configuration.

In this example, we will list files in a directory and write their path and size to
a CSV file.

We'll define a processing chain for the process. We'll create a `flow.Chain` node
and add it to the context with the name `directorylist.process`:

    def cubetl_config(ctx):
        ctx.add('directorycsv.process', flow.Chain(steps=[]))

## Running

This processing chain does nothing, but we can test this already. Run:

    cubetl directorylist.py directorylist.process

...and you should see the following output:

    $ cubetl directorycsv.py directorycsv.process
    2019-01-10 00:56:50,183 - INFO - Starting CubETL 1.x
    2019-01-10 00:56:50,183 - INFO - Including config file: directorycsv.py
    2019-01-10 00:56:50,224 - INFO - Processing Chain(directorycsv.process)

## Defining the process

In this example, we'll use a node that generates a message for each file in a
given directory. There's a CubETL component for that: `cubetl.fs.DirectoryList`.
Let's add it to the list of steps of our chain.

    ctx.add('directorycsv.process', flow.Chain(steps=[

        # Generates a message for each file in the given directory
        fs.DirectoryList(path="/"),

        # Print the message
        util.Print()

        ]))

Observe that the `cubetl.fs.DirectoryList` class takes a `path` argument that
defines which directory to list. In this example we are using `"/"` in order to
list files in the root directory.

Note that we have also added a `cubetl.util.Print` node, which formats and prints
a message to the terminal. If you now run the process you'll see:

    $ cubetl directorycsv.py directorycsv.process
    2019-01-10 01:22:20,486 - INFO - Starting CubETL 1.1.0-beta
    2019-01-10 01:22:20,486 - INFO - Including config file: directorycsv.py
    2019-01-10 01:22:20,536 - INFO - Processing Chain(directorycsv.process)
    2019-01-10 01:22:20,537 - INFO - Listing directory / (mask 'None')
    {'path': '/vmlinuz.old'}
    {'path': '/initrd.img.old'}
    {'path': '/initrd.img'}
    {'path': '/core'}
    {'path': '/vmlinuz'}


## The process flow

When the process is started, the initial *flow.Chain* node receives an initial, empty message.

The *Chain* node is a core component of CubETL: it processes messages in cascade (depth-first)
through each of the nodes in the *steps* list.

In the example above, the first step of the chain is the *DirectoryList* node, so it receives
the initial (empty) message. This node then emits a message for each file found in the specified
directory. Each of those messages is then printed by the *Print* node. Each message contains
a `path` attribute.


## Adding information

The `cubetl.fs` package includes a node that retrieves filesystem information for a file.
Let's add it right after the *DirectoryList* node:

    fs.DirectoryList(path="/"),
    fs.FileInfo(path=lambda m: m['path']),

The *FileInfo* node has a path argument that defines the target file. Here
we use a lambda expression that gets the path from the current message.
This is the method for referring to message attributes in node configuration.

If you run the process, you'll see messages now contain attributes `mtime` and `size`:

    $ cubetl directorycsv.py directorycsv.process
    2019-01-11 18:08:22,572 - INFO - Starting CubETL 1.1.0-beta
    2019-01-11 18:08:22,572 - INFO - Including config file: directorycsv.py
    2019-01-11 18:08:22,621 - INFO - Processing Chain(directorycsv.process)
    2019-01-11 18:08:22,621 - INFO - Listing directory / (mask 'None')
    {'mtime': 1540395102.0, 'path': '/vmlinuz.old', 'size': 7168736}
    {'mtime': 1544897440.0, 'path': '/initrd.img.old', 'size': 39999769}
    {'mtime': 1547148472.0, 'path': '/initrd.img', 'size': 40002122}
    {'mtime': 1543952702.0, 'path': '/core', 'size': 10305536}
    {'mtime': 1544018919.0, 'path': '/vmlinuz', 'size': 7171392}


## Writing to a CSV

Writing messages to screen in JSON format may be useful for development, but let's now transform
data into CSV format so it could, for example, be read with a spreadsheet application.

CubETL provides components to read and write CSV formats. Add the following node
to the end of the process steps list:

        # Generates CSV header and rows and writes them
        csv.CsvFileWriter(),

This component generates CSV header and rows and writes them. You could define a list of columns
to be written, but by default *CsvFileWriter* will write a column for each of the message attributes.

If you don't specify a file path, the *FileWriter* will write to standard output (it's often
convenient to do this, and simply redirect the process output to a file from the shell). You can
always provide a file path using the `path` argument.

This will interleave *Print* and *CsvFileWriter* output! (test it). You could comment the *Print* node,
but there's a command line argument to bypass *Print* nodes: `cubetl -q`.

If you now run the process with `-q` you can see the CSV output:

    $ cubetl directorycsv.py directorycsv.process -q
    2019-01-11 21:54:11,593 - INFO - Starting CubETL 1.1.0-beta
    2019-01-11 21:54:11,593 - INFO - Including config file: directorycsv.py
    2019-01-11 21:54:11,648 - INFO - Processing Chain(directorycsv.process)
    2019-01-11 21:54:11,648 - INFO - Listing directory / (mask 'None')
    2019-01-11 21:54:11,689 - INFO - Writing to standard output (file: '-'):
    mtime,path,size
    1538093710.231213,/dead.letter,16117
    1540395102.0,/vmlinuz.old,7168736
    1544897440.1933208,/initrd.img.old,39999769
    1547148472.3457868,/initrd.img,40002122
    1543952702.1512303,/core,10305536
    1544018919.0,/vmlinuz,7171392


## Choosing which directory to list

You can define **context properties** using the `-p` command line argument. This is useful to
pass variables to your ETL process (eg. connection strings, filenames...).

You can then use these properties in expressions. For example, let's take the directory
to list from a property:

    fs.DirectoryList(path=lambda ctx: ctx.props.get("path", "/")),

This resolves the path of the directory to list using a lambda expression. Expressions are
understood by many of CubETL components and allow you to define configuration in terms of
context and message values.

Context properties are a held by a dictionary available through `ctx.props`. Here we
use the `.get()` method to retrieve a key if it exists, or otherwise return a default
value `"/"`.

We can now run our process for a different directory using:

    cubetl directorycsv.py directorycsv.process -p path=.

In addition, you can also set **attributes on the initial message** using the `-m` command line
option:

    cubetl directorycsv.py directorycsv.process -m list=harddrive -m 'date=${f.dt.datetime.now()}'


## Printing messages is slow!

During development it is useful to use `cubetl.util.Print` nodes in order to print
messages and inspect them.

Serializing and highlighting message data is usually a slow operation.
Your message processing throughput will usually be much
higher than what can be printed to screen, and printing may slow the process down
to an unacceptable performance.

Instead of removing Print nodes from the configuration, you can skip them by using
the `-q` command line argument of CubETL.

    cubetl yourconfig.py start_node -q


## Debugging

You can increase logging verbosity by using the `-d` command line argument:

    cubetl yourconfig.py start_node -d


## New configuration template

In order to create a new configuration, you can use the builtin `cubetl.config.new`
processing node:

    cubetl cubetl.config.new -p config.name=myproject

This will create a `myproject.py` file with a default CubETL configuration which you
can use to start defining your own process.

## Further information

* [Documentation index](https://github.com/jjmontesl/cubetl/blob/master/doc/guide)
