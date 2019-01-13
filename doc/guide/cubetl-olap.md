# CubETL - Using OLAP components

CubETL provides OLAP components that help manipulating OLAP schemas and data.

OLAP stands for OnLine Analytical Processing. It's an approach to database mining
that differs from the traditional relational usage. OLAP databases are designed and
optimized to execute analytical queries fast, but not to allow modifications or
to keep data normalized.

By analytical queries we refer to queries that aggregate quantitative homogeneous data
while being able to split the result arbitrarily across any of the multiple attributes or
analytical axis (often called dimensions). This is the kind of approach that analytic
and visualization tools like Google Analytics or Tableu provide, for example.

For example, you could define a dataset for "sales" and use it to explore measurements
(like sale value, quantities, delivery time) by any available attribute (date, day of week,
customer, customer address, employee...).

## OLAP Entities: facts and dimensions

CubETL uses *OLAP components* to handle OLAP schema definition. The main components are:

  - *olap.Fact*: represents each dataset type (or *cube*).
  - *olap.Dimension*: represents an analytical axis (or *dimension*).
  - *olap.HierarchyDimension*: represents a dimension with multiple hierarchical levels.

These entities, besides the usual `name` and `label`, can have several
attributes. Each attribute must be of one of the following types:

  - *olap.Attribute*: represents an attribute of the fact or dimension.
  - *olap.DimensionAttribute*: represents an analytical dimension of the entity.
  - *olap.Measure*: represents an aggregated measure of the entity.

**Nested dimensions and facts**

In CubETL, the dimensions of a Fact are referenced using *DimensionAttribute* attributes.

*DimensionAttribute* objects require a reference to another *OlapEntity* (it can be a
*Dimension* or a *Fact*). Note that CubETL allows nested dimensions, and allows using
facts as dimensions.

You can reference the same dimension or fact more than once from another entity, using
*DimensionAttribute* objects with different names. This is often used to reference
a common dimension (eg `cubetl.datetime.date`) with several aliases (like
`order_date` and `invoice_date`).

This is an excerpt of an example OLAP schema configuration:

    ctx.add('ine.autonomy', olap.Dimension(
        name='autonomy',
        label='Autonomy',
        attributes=[olap.Attribute('autonomy', type='String')]))

    ctx.add('ine.province', olap.Dimension(
        name='province',
        label='Province',
        attributes=[olap.Attribute('province', type='String')]))

    ctx.add('ine.autonomyprovince', olap.HierarchyDimension(
        name='autonomyprovince',
        label='Province',
        attributes=[DimensionAttribute(ctx.get('ine.autonomy')),
                    DimensionAttribute(ctx.get('ine.province'))]))

    ctx.add('ine.nationality', olap.Dimension(
        name='nationality',
        label='Nationality',
        attributes=[olap.Attribute('nationality', type='String')]))

    ctx.add('ine.census', olap.Fact(
        name='census',
        label='Census',
        attributes=[DimensionAttribute(ctx.get('cubetl.datetime.datemonthly'), label="Sampling Date"),
                    DimensionAttribute(ctx.get('ine.autonomyprovince')),
                    DimensionAttribute(ctx.get('ine.nationality')),
                    DimensionAttribute(ctx.get('cubetl.person.gender')),
                    DimensionAttribute(ctx.get('cubetl.person.age_range')),
                    Measure(name='census', type='Integer', label="Population")]))

You can find a complete example in the [Apache web server log file parsing and SQL loading in OLAP star-schema](https://github.com/jjmontesl/cubetl/tree/master/examples/loganalyzer),
and in the [PCAxis to SQL OLAP star-schema](https://github.com/jjmontesl/cubetl/tree/master/examples/pcaxis)
example.

## OLAP Mappers

Once you have an OLAP schema for your data model, you need to map it to some storage backend in order
to be able to store and query data from it. In CubETL the relation between OLAP entities and backends
is done through an *OlapMapper*.

CubETL currently supports SQL backends.


## OLAP SQL Mappings

**Defining SQL mappings**

TODO: This section is not yet written. Help needed! Please get in touch if you wish to collaborate.

**Listing OLAP schema and mappings**

Printing OLAP schema and mappings is of great help to assist in debugging and understanding
the model. You can print a summary of OLAP configuration using the *cubetl.olap.PrintMappings*
node, or the builtin refence `cubetl.olap.mappings`, like in this example:

    cubetl myproject.py cubetl.olap.mappings


## Loading OLAP data

Because CubETL knows the relation between entities and how to map them to SQL tables,
it can load facts in a single store operation.

Since dimension items are often repeated and spread across tables, they commonly require
a lookup operation before being saved to their backing table or tables. CubETL walks
all of the fact attributes and nested dimensions recursively, performs (and caches)
the appropriate lookups on each table, and when appropriate creates new records for
each of the new dimension values. During the process it collects all the primary
keys needed to fill in each of the parent table foreign keys, and ultimately inserts
a record into the appropriate fact table.

You can load fact data using the *olap.Store* node:

        olap.Store(entity=ctx.get('myproject.myfact'),
                   mapper=ctx.get('olap2sql.olapmapper')),

Remember you can list the OLAP schema and mappings (see above). It shows
which message attribute will be mapped to each of the fact or dimension attributes,
and which dimension attribute is mapped to each SQL column.

## Auto generating SQL schema from OLAP configuration

While defining and curating your OLAP schema catalog is usually recommended, on the other hand
defining your own SQL star schema is not always a requirement and you can rely, at least
initially, on CubETL automatic OLAP-to-SQL mappings.

You can generate SQL mappings for your OLAP schema using:

    sqlschema.OLAPToSQL.olap2sql(ctx, connection=ctx.get('myproject.connection'))

This will inspect OLAP entities in the current context and generate SQLTable and SQLColumn
objects. A table is created for each dimension and fact, as well as columns for attributes and
measures, and primary keys added.

**Printing the generated configuration**

Remember that you can dump the current context configuration using the `cubetl.util.config.PrintConfig`
node or the `cubetl.config.print` builtin node in order to study or copy-paste sections from
the automatically generated schema and mappings.

**From command line**

CubETL provides command line support for these operations. See the
[Generate OLAP schema from SQL database and visualize in CubesViewer](https://github.com/jjmontesl/cubetl/tree/master/examples/sql2olap) example.


## Serving data with Databrewery Cubes

Once your OLAP schema is defined and SQL mappings configured, CubETL knows everything
needed to query the database in an analytic way.

CubETL can generate a *Databrewery Cubes Server* model and configuration. Cubes is a
SQL based OLAP server with support for filtering and drilldown operations.

You can generate a Cubes model and config using the *Cubes10ModelWriter* node:

        # Generate a Cubes model
        cubes10.Cubes10ModelWriter(olapmapper=ctx.get('olap2sql.olapmapper'),
                                   model_path="myproject.cubes-model.json",
                                   config_path="myproject.cubes-config.ini"),

You can then use Cubes to serve the database:

    slicer serve myproject.cubes-config.ini

You can also use [CubesViewer](http://www.cubesviewer.com) to query and visualize the data
(running `cvutils cv` and connecting to `http://localhost:8085` with your browser).


## Querying OLAP data from CubETL

Querying OLAP data from CubETL is planned but not yet implemented. Help is needed!
Please get in touch if you can collaborate with this project.

(See `cubetl.olap.query` module).


## Further information

* [Documentation index](https://github.com/jjmontesl/cubetl/blob/master/doc/guide)
