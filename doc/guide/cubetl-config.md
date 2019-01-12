# CubETL - Config

Note: this section assumes you are familiar with CubETL terms (see the
Quickstart guide for further information).

CubETL is configured by defining *components* that will live in the ETL
*context*. These components are defined via one ore more configuration files.


## Configuration files

CubETL configuration files are standard Python source files that implement a function
named `cubetl_config(ctx)` that takes a single argument (`ctx`):

    def cubetl_config(ctx):
        # Your CubETL components configuration goes here
        ...

## Including other configuration files

If your context is complex (lots of tables, columns, mappings, OLAP entities...), this
file can get large.

Separating your entity definitions (like SQL tables and OLAP entities) from your
process definition (like transformations and mappings) you can better reuse your
logical entities and schemas across several processes.

From your configuration function, you can include other CubETL configuration files
using the `ctx.include(...)` method:

    def cubetl_config(ctx):

        ctx.include('${ ctx.library_path }/datetime.py')
        ctx.include("myentities.py")

Config *files are only included once*, the first time they are referenced.


## Adding components

Components are added to the *context* using the `ctx.add(...)` method.

    def cubetl_config(ctx):

        ctx.add('genre.table', sql.SQLTable(
            name="genre",
            connection=ctx.get('myprocess.connection'),
        ))

You need to provide a name for the component.

**Anonymous components**

When configuring a component, if you don't need to reference it from several other
components, you can create and add children components directly without naming and
adding those to the context.

    ctx.add('myprocess.process', flow.Chain(steps=[

        config.Print(),

        sql.Transaction(connection=ctx.get('myprocess.sql.connection')),

        fs.FileLineReader(path='file.txt', encoding=None),

        ...

In the above example, the *Print*, *Transaction* and *FileLineReader* nodes are defined
and listed without a name, directly inside the `steps` attribute of the *Chain* node
(which is then named and added to the context). This is also valid as long as you don't
need to reference them by name.


## Referencing other components

Component sometimes needs to reference other components in the context.

This can be done by directly defining the component in-place (as shown in the previous section)
or by refencing the object by name.

In order to reference an object in the context, you can use `ctx.get(...)`. This method
takes the name of the component as argument and returns it from the context:

    sql.Transaction(connection=ctx.get('myprocess.sql.connection'))


## Config expressions and lambdas

When configuring components, it is often needed to configure component behavior
depending on the current context or message values.

For example, suppose a process needs to read several CSV files and process all rows.
The *CSVFileReader* component reads a CSV file from the filesystem, but we need to
specify the path to the CSV file, which comes frmo a previous *DirectoryList* component,
and it's stored in the `path` attribute of the message. In this case we can use
a config expression `path="${ m['path'] }"` which will be evaluated for each message:

    def cubetl_config(ctx):

        ctx.add('myprocess.process', flow.Chain(steps=[

            fs.DirectoryList(path=".", filter_re=".*.csv"),

            fs.CsvFileReader(path="${ m['path'] }"),

            ...


**Value interpolation**

Value interpolation is performed on strings that contain an expression delimited
by `${ ... }`. Anything between `{` and `}` is treated as a Python expression.

This allows you to interpolate context and message data into strings, for example:

    log.Log(message="Processing CSV file: ${ m['path'] }"),

When a string is comprised *entirely* of an interpolated expression, the result value
is taken directly as the result of the interpolation even when this result is not
a string. The following example shows how to assign an *object reference* through an
interpolated string (here, the message `my_store_table` contains a reference to
a cubetl.sql.SQLTable, and the interpolated value returns that reference instead of
a string):

    sql.StoreRow(sqltable="${ m['my_store_table'] }")

Note that including whitespace will cause the expression to be treated as a string.
In this case, you can also use a *lambda expression* (see below).

Interpolated expressions are executed in a local scope where the following
variables are available:

  - `ctx`: a reference to the process context
  - `m`: a reference to the current message (if it applies)
  - `f`: contains references to all helper function modules
  - `props`: a reference to context properties (same as `ctx.props`)
  - each of the registered helper function modules by name (`text`, `xml`, `dt` , `re`, `sys`...)

Interpolated expressions are precompiled and cached in order to avoid impacting performance, but
when string substitution is not needed, lambda expressions should be favoured.

**Lambda expressions**

In addition to interpolated strings, you can also use a lambda function for any
config value that supports expressions:

    fs.CsvFileReader(path=lambda m: m['path']),

Alike *interpolated expressions*, the function will be executed whenever the
path value is needed. CubETL lambda functions can receive a `ctx` argument, a
`m` argument (with the current message), or both (in that order). CubETL will
call it with the appropriate arguments.

**Expressions support**

Note that not all components and configuration parameters accept *config expressions*.
Check each component's docs to see what configuration values they accept and whether
they accept value interpolation (please use the issue tracker if you are missing
expression support in a given component).


## Common Functions

Some CubETL modules provide helper functions meant to ease common data processing
operations, like extracting numbers from text strings, regular expression matching,
xml xpath searches...

These functions are available through their module name in interpolated expressions,
for example, here we show an expression using `text.extract_number` used in
a filter condition:

    flow.Filter(condition="${ text.extract_number(m['year']) > 2015 }"),

From Python code or lambda expressions, you can access those modules through the
`ctx.f` context property, as shown here:

    m['referer_domain'] = ctx.f.text.urlparse(m['referer']).hostname
    m['referer_path'] = ctx.f.text.urlparse(m['referer']).path

Check each module documentation for more information about published helper functions.

TODO: Add extensible function registration (by modules) and config (by user) and a "list of functions" (util.config.PrintFunctions)


## CubETL types and component library

CubETL includes a library of common types that can be used directly by ETL processes,
or as a basis for custom types.

CubETL allows configuration reuse through the `include` function. It is recommended
that you put your organisation entities and schemas in well-organised configuration
files. You can then reuse them from different ETL processes.

CubETL library includes OLAP schemas, regular expressions, and default data for
several topics: date and time, HTTP topics,

Check the [Library index](https://github.com/jjmontesl/cubetl/blob/master/doc/guide) for
further information.


## Further information

* [Documentation index](https://github.com/jjmontesl/cubetl/blob/master/doc/guide)
