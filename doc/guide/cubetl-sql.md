# CubETL - Working with SQL datasources

CubETL provides components that interact with SQL databases. SQL can be used both
as a data source or as a data store sink.

In order to do this, CubETL uses components that represent the relational schema:

  - *sql.Connection*: represents a connection to a SQL database
  - *sql.SQLTable*: represents a database table
  - *sql.SQLColumn*: represents a table column
  - *sql.SQLColumnFK*: represents a table column that is a foreign key

You can define your own schema via configuration or you can allow CubETL to inspect
an existing database and generate the schema configuration for you. There are also
other CubETL modules that generate SQL schemas (like the *cubetl.olap.sqlchema.OLAPToSQL*
class does), so in many cases you may not need to define your CubETL SQL schema manually.

In addition to the SQL components, there are a number of nodes that interact with SQL
components during data processing, among them:

  - *sql.Transaction*: represents a database transaction for the process chain
  - *sql.Query*: runs a SQL query and generates a message per result row
  - *sql.QueryLookup*: lookups a row by attributes (columns) in a table
  - *sql.StoreRow*: stores (inserts or upserts) a row


## Importing schema from an existing database

This section uses the chinook database for the examples (see `examples/sql`).

When you have an already existing database, you can have CubETL inspect its schema
using the *cubetl.sql.schemaimport.DBToSQL* component. This component can
be used both as a runtime node and as a configuration-time component.

(By the way, there's a command line shortcut for this. If all you want to do is to
inspect an existing database and quickly visualize it see the `examples/sql`
directory README).

You can use the component at configuration time like this:

    def cubetl_config(ctx):

        ctx.add('example.sql.connection',
                sql.Connection(url='sqlite:///Chinook_Sqlite.sqlite'))

        schemaimport.DBToSQL.db2sql(ctx, connection, options, debug, prefix)

The *Connection* component defines the database connection to be used. It requires
an `url` parameter, which is a SQLAlchemy DB connection URL. This can be used to access
many different database systems.

The *DBToSQL.db2sql* call will add to your context all the entities (SQL tables and columns) found in the
database schema.

You could already test this using `cubetl <file> -l` or `cubetl <file> cubetl.config.print`:

    $ cubetl sqlexample.py cubetl.config.print

When using `cubetl.config.print` the program provides a dump of the context configuration
in Python code. You could use this a basis for manual SQL schema configuration, where you can
easily replace labels or further alter the SQL schema to suit your needs (NOTE: this configuration
dump is not meant to be used as-is, but instead to be reviewed and copy-pasted as needed).


## Manually defining an schema

When you wish to manually define the SQL schema, you need to create *sql.SQLTable* and
*sql.SQLColumn* objects that represent the databse tables and columns.

For example, we'll add a new connection and a custom table:

    # Add output database and schema
    ctx.add('example.sql.connection_out',
            sql.Connection(url='sqlite:///chinook-aggregated.sqlite3'))

    ctx.add('example.agg.table', SQLTable(
        name='example_aggregates',
        label='Album Sales',
        connection=ctx.get('example.sql.connection_out'),
        columns=[
            SQLColumn(name='album_id', type='Integer', pk=True, label='AlbumId'),
            SQLColumn(name='album_title', type='String', label='Title'),
            SQLColumn(name='total_sales', type='Float', label='Sales')]))


## Running queries

You can run SQL queries using the *cubetl.sql.Query* node. This node generates a message
for each row that results from the SQL query.

Here, a *Chain* node is added with two steps, a SQL query and a print node (which allows
observing the resulting messages). The *Query* node takes a *Connection* and a SQL query
string.

    # Process
    ctx.add('example.process', flow.Chain(steps=[

        # Query album sales
        sql.Query(connection=ctx.get('example.sql.connection'),
                  query="""
                      select Album.AlbumId as album_id,
                             Album.Title as album_title,
                             sum(InvoiceLine.UnitPrice * InvoiceLine.Quantity) as total_sales,
                             sum(InvoiceLine.Quantity) as total_count
                      from InvoiceLine
                           join Track on InvoiceLine.TrackId = Track.TrackId
                           join Album on Track.AlbumId = Album.AlbumId
                      group by Album.AlbumId
                  """),

        util.Print(),

    ]))

Note that you don't need a SQL schema if are only running SQL queries through a connection. A schema is needed
in order to use table functions (table insert / upsert / lookups), database introspection or OLAP SQL mappings.

If you run this process over the example database, you'll each message resulting from the query:

    {'album_title': 'For Those About To Rock We Salute You', 'total_count': 10, 'total_sales': 9.9, 'album_id': 1}
    {'album_title': 'Balls to the Wall', 'total_count': 2, 'total_sales': 1.98, 'album_id': 2}
    {'album_title': 'Restless and Wild', 'total_count': 3, 'total_sales': 2.9699999999999998, 'album_id': 3}
    {'album_title': 'Let There Be Rock', 'total_count': 6, 'total_sales': 5.94, 'album_id': 4}
    {'album_title': 'Big Ones', 'total_count': 10, 'total_sales': 9.9, 'album_id': 5}
    ...


## Inserting rows

In this example, we insert the results of the previous query into our new database table created above.

    sql.StoreRow(sqltable=ctx.get('example.agg.table'),

When running the process, CubETL will create the database table if it doesn't exist. It will then store
each row that resulted from the aggregated query into the new table.

This works because the *StoreRow* node will try to map each table column to the message attribute with the
same name. If in your scenario names don't match, you neeed to define column mappings in the *StoreRow* node.

If you run this process now, you'll realize it's *very slow*. This is because of autocommit. You should
always try to wrap your SQL inserts in database transactions. In order to achieve that, add a
*sql.Transaction* node *before* the SQL Query (as the first node in the chain). Also, when you
work with more than one database, make sure you correctly target the connection you are writing to:

    # Process
    ctx.add('example.process', flow.Chain(steps=[
        sql.Transaction(connection=ctx.get('example.sql.connection_out')),
        ...

This must be done _before_ the query is run because the *Transaction* node starts a new transaction
with every incoming message. We want a single transaction, not one per row, so the *Transaction*
node must be the first one in the chain so it receives only the initial message.

If you now run the example process (`sqlexample.py`) with `-q`, you should see the summary of the process:

    $ cubetl sqlexample.py example.process -q
    2019-01-13 02:56:51,300 - INFO - Processing Chain(example.process)
    2019-01-13 02:56:51,300 - INFO - Starting database transaction
    2019-01-13 02:56:51,552 - INFO - Commiting database transaction
    2019-01-13 02:56:51,596 - INFO - SQLTable Totals  ins/upd/sel: 0/304/304
    2019-01-13 02:56:51,597 - INFO - SQLTable example_aggregates ins/upd/sel:      0/   304/304
    2019-01-13 02:56:51,597 - INFO - Performance - Total time: 0  Total messages: 304  Global rate: 938.138 msg/s


## Doing (cached) table lookups

*This section is yet to be written. Please help!*


## Further information

* [Documentation index](https://github.com/jjmontesl/cubetl/blob/master/doc/guide)
