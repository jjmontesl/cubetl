# SDMX

This sample defines an OLAP structure from SDMX data. This example uses
an international trade statistics dataset.

## Guide

The dataset contains data for each province, nationality, age group, gender and
date.

We first want to run the CubETL process to generate the database.
Start by listing the available nodes:

    cubetl ine_census.py -l

The entry point is called `ine.process.census`, call `cubetl` to run it.
Note that this process make take several minutes:

    cubetl ine_census.py ine.process.census -q

The `-q` option makes the process run in quiet mode, so process data items will
not be printed. The process includes a node that will print progress
information every 10 seconds.

This has generated a `ine.sqlite3` database in the current directory.
Note that the process has also generated a Cubes model and configuration file
(`ine.model.json` and `ine.slicer.ini`).

**Serving the data**

We now have a SQLite database. Run Cubes in order to serve
analytical queries for this database:

    slicer serve ine.slicer.ini  &

Note the `&` argument, which makes the process run in background. You could instead
use a separate terminal window if you wish.

Now run CubesViewer to inspect the dataset:

    # Note: not yet working, please download and use CubesViewer manually!
    cubext cv

This will start a local HTTP service running on port 8085 for CubesViewer studio.
It will also open a browser pointing to the application.

You can now inspect the *Census* cube using CubesViewer.

**Views to try**

Try the following views (on CubesViewer, click "Tools > Import from JSON").

Population chart of 2018:

    {}

Population nationality evolution:

    {}

[SCREENSHOTS]


## Further information

Inspect the CubETL process definition (ine_census.py),

## Data

This example SDMX data comes from the European Statistics Agency (Eurostat).


