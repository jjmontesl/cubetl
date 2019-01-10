# CubETL - Introduction

CubETL allows you to define processes that retrieve data from different sources,
manipulate it, and writes it in the same or different formats. This is usually known
as an ETL process (for Extract, Transform and Load).

It does so by handling data as separate items (eg. rows in a database, lines in a text file).

In CubETL, data items are called "**messages**" (or sometimes, "items"). Messages are a
simple key-value structure which can hold any data your process needs to handle.

CubETL provides tools and classes that handle common ETL needs, including common facilities
like logging and caching and dedicated modules for RegExp, XML, CSV, JSON, SQL, and OLAP
schemas and data stores.

Within that framework, you can define your own transformation processes.

(The example in this section can be found in the `examples/various/directorycsv.py` file).


## CubETL processes

A process is defined by a chain of processing **nodes** that can take a message message and
transform it, or produce any number of derived messages. These are then processed by the
next step of the chain.

For example, a simple process that lists files in a directory writes them to a CSV file
would look like:

![DirectoryList-to-CSV process](https://raw.github.com/jjmontesl/cubetl/master/doc/img/uml/directorylist-to-csv.plantuml.svg "DirectoryList-to-CSV process")


## CubETL context and components

CubETL processes are configured inside a **context**, which holds the configuration for
one or more processes.

A CubETL context keeps named references to **components**. Components represent resources and
*nodes* (which are also components) is where *message* generation and processing takes place.

| Component Examples | Node Examples |
|--------------------|---------------|
| sql.Connection     | sql.Query     |
| sql.SQLTable       | util.Print    |
| olap.Dimension     | csv.CsvWriter |
| ...                | flow.Chain    |
|                    | ...           |


## Creating an ETL process

In order to define the ETL process, choose a directory for your project and
create a `directorycsv.py` file with the following content:

    from cubetl import text, flow, fs, script, util

    def cubetl_config(ctx):
        # Your CubETL components configuration goes here

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
through each of the nodes in its *steps* list.

In the example above, the first step of the chain is the *DirectoryList* node, which receives
the initial (empty) message. This node <TODO>

<TODO>


## Writing to a CSV


## Adding information




## Choosing which directory to list

You can add properties to the context using the `-p` command line argument. This is useful to
pass variables to your ETL process (eg. connection strings, filenames...).

<TODO>

In addition, you can also set attributes on the initial message using the `-m` command line
option.


## Printing messages is slow!

During development it is useful to use `cubetl.util.Print` nodes in order to print
messages and inspect them.

Serializing and highlighting message data is usually a slow operation. Unless your
data processing is particularly slow, your processing throughput will usually be much
higher than what can be printed to screen, and printing may slowdown the process
to an unusable throughput.

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

* [Documentation index](index.md)
