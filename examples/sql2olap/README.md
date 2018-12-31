# Create and serve an OLAP schema from an existing DB using Cubes

CubETL provides some entry points for inspecting an existing SQL database,
generating an OLAP model, and exporting a Cubes configuration.

All together, this can be used to quickly inspect an existing database using CubesViewer.

## The Chinook database

This example uses the well-known *Chinook test database*. It is provided for many
database systems, this directory includes a copy of the SQLite version.

You can find more information about this database at:
https://github.com/lerocha/chinook-database/

## Generate an OLAP schema and Cubes config

Let's run an initial default import process:

    # Inspect database and generate a cubes model and config
    cubetl cubetl.sql.db2sql cubetl.olap.sql2olap cubetl.cubes.olap2cubes \
        -p db2sql.db_url=sqlite:///Chinook_Sqlite.sqlite \
        -p olap2cubes.cubes_model=chinook.cubes-model.json \
        -p olap2cubes.cubes_config=chinook.cubes-config.ini

This runs three process nodes sequentially:

* **cubetl.sql.db2sql**: This connects to the target database and imports the SQL schema
  into CubETL model (using `cubetl.sql.SQLTable` and `cubetl.sql.SQLColumn`).
* **cubetl.olap.sql2olap**: This walks the cubetl.sql.SQLTable objects in the context
  and generates `cubetl.olap.*` entities for the SQL schema.
* **cubetl.cubes.olap2cubes**: This processes the OLAP schema defined in the previous step
  and generates a *Cubes OLAP Server* model and configuration file.

Also note that parameters are passed using `-p property=value`.

## Visualizing the dataset

Now you can serve the mapped database using Cubes, and browse it using CubesViewer.

For this example you need to have Cubes Server and CubesViewer packages installed.

    # Run cubes server (in background with &, or in other terminal)
    pip install cubes[all] click flask
    slicer serve mydb.cubes-config.ini &

    # Run a local cubesviewer HTTP server (also opens a browser)
    # NOTE: not yet available, please download and use CubesViewer manually!
    pip install cubesviewer-utils
    cvutils cv

## Options

The *sql2olap* utility generates an OLAP schema using simple guessing. Numbers are
considered observations and strings are always considered dimensions, even if they are
details.

Often, we want to control this process. The SQLToOLAP class accepts several options
that can be used to fine-tune how the schema generator works.

**Changing how fields are imported**

If you observe the generated schema you'll notice some columns would be more appropriate
if treated differently.

For example, click on "Cubes > Album" and then on the "Facts View" icon.
Artist dimension and Artist Name dimensions are repeated. This stems from the *SQLToOLAP* component
treating the *Artist.Name* column a *dimension*, when it is simply an *attribute* of an album.

Let's add an option to treat *Artist.Name* as an *attribute*, and also for *Album.Title*:

    cubetl cubetl.sql.db2sql cubetl.olap.sql2olap cubetl.cubes.olap2cubes \
            -p db2sql.db_url=sqlite:///Chinook_Sqlite.sqlite \
            -p olap2cubes.cubes_model=chinook.cubes-model.json \
            -p olap2cubes.cubes_config=chinook.cubes-config.ini \
            -p sql2olap.*.table.Artist.col.Name.type=attribute \
            -p sql2olap.*.table.Album.col.Title.type=attribute

*Note*: you need to restart your Cubes server and your CubesViewer instance to
account for the new schema changes!

Those two fields are now treated as attributes and the schema for those two cubes
is more usable. For a full list of options, see the documentation of the *SQLToOLAP* component.

**Use a shell script for this**

When running this from command line, it is recommended to create a shell script
that runs it. That way you can keep track of the additional settings for the schema importers,
as they can become quite lengthy. This directory includes the `sql2olap.sh` shell script,
which is the final example of this. You can run it using:

    bash sql2olap.sh


**Complete command**

An example of the complete command would look like:

    cubetl cubetl.sql.db2sql cubetl.olap.sql2olap cubetl.cubes.olap2cubes \
        -p db2sql.db_url=sqlite:///Chinook_Sqlite.sqlite \
        -p olap2cubes.cubes_model=chinook.cubes-model.json \
        -p olap2cubes.cubes_config=chinook.cubes-config.ini \
        -p sql2olap.*.table.Artist.col.Name.type=attribute \
        -p sql2olap.*.table.Album.col.Title.type=attribute \
        -p sql2olap.*.table.Customer.col.FirstName.type=attribute \
        -p sql2olap.*.table.Customer.col.LastName.type=attribute \
        -p sql2olap.*.table.Customer.col.Address.type=attribute \
        -p sql2olap.*.table.Customer.col.PostalCode.type=attribute \
        -p sql2olap.*.table.Customer.col.Phone.type=attribute \
        -p sql2olap.*.table.Customer.col.Fax.type=ignore \
        -p sql2olap.*.table.Customer.col.Email.type=attribute \
        -p sql2olap.*.table.Employee.col.FirstName.type=attribute \
        -p sql2olap.*.table.Employee.col.LastName.type=attribute \
        -p sql2olap.*.table.Employee.col.Address.type=attribute \
        -p sql2olap.*.table.Employee.col.PostalCode.type=attribute \
        -p sql2olap.*.table.Employee.col.Phone.type=attribute \
        -p sql2olap.*.table.Employee.col.Fax.type=ignore \
        -p sql2olap.*.table.Employee.col.Email.type=attribute \
        -p sql2olap.*.table.Employee.col.BirthDate.type=attribute \
        -p sql2olap.*.table.Genre.col.Name.type=attribute \
        -p sql2olap.*.table.MediaType.col.Name.type=attribute \
        -p sql2olap.*.table.Playlist.col.Name.type=attribute \
        -p sql2olap.*.table.Invoice.col.BillingAddress.type=attribute \
        -p sql2olap.*.table.Invoice.col.BillingPostalCode.type=attribute \

## Tips

This process is designed for quick inspection of existing databases.

Beyond this, you'd normally wish to define your own model or to further extend
the one that was automatically generated by CubETL. For this, defining your own
CubETL config works best, this way you have then greater flexibility for
configuring your model and processes.

You can print the current CubETL configuration by adding `cubetl.config.print`
as the last target of your process. This allows you to copy the definitions of
automatically generated components (by the sql2olap process above)
to your own process config.


## Data

Example database "Chinook" from: https://github.com/lerocha/chinook-database/
