# PC-Axis

This example imports an OLAP structure from a PC-Axis statistics file. The example
contains census data with several axis of observation.

## Guide

The dataset contains data for each province, nationality, age group, gender and
date.

We first want to run the CubETL process to generate the database.
Start by listing the available nodes:

    cubetl ine_census.py -l

The entry point is called `ine.process`, call `cubetl` to run it.
Note that this process make take several minutes:

    cubetl ine_census.py ine.process -q

The `-q` option makes the process run in quiet mode, so process data items will
not be printed. The process includes a node that will print progress
information every 10 seconds (the process takes a few minutes to process
the 600k cells in the original file).

This has generated a `ine.sqlite3` database in the current directory.
Note that the process has also generated a Cubes model and configuration file
(`ine.cubes-model.json` and `ine.cubes-config.ini`).

**Serving the data**

We now have a SQLite database. Run Cubes in order to serve
analytical queries for this database:

    # Run cubes server (in background with &, or in other terminal)
    pip install https://github.com/DataBrewery/cubes/archive/master.zip click flask --upgrade
    slicer serve ine.cubes-config.ini  &

Note the `&` argument, which makes the process run in background. You could instead
use a separate terminal window if you wish.

Now run CubesViewer to inspect the dataset:

    # NOTE: not yet available, please download and use CubesViewer manually!
    cvutils cv

This will start a local HTTP service running on port 8085 for CubesViewer studio.
It will also open a browser pointing to the application.

You can now inspect the *Census* cube using CubesViewer.

**Views to try**

In CubesViewer Studio, click "Tools > Import from JSON", and paste the following JSON:

    {
    "charttype":"bars-horizontal",
    "chartoptions": {"showLegend":true,"mirrorSerie2":true},
    "mode":"chart",
    "drilldown":["gender"],
    "cuts":[{"dimension":"datemonthly@monthly","value":"2018,3","invert":null}],
    "cubename":"census",
    "name":"2018 Population Chart",
    "xaxis":"age_range",
    "yaxis":"census_sum",
    "chart-barsvertical-stacked":true,
    "chart-disabledseries":{"key":"gender","disabled":{"Hombres":false,"Mujeres":false}}
    }

![CubesViewer Screenshot](https://raw.github.com/jjmontesl/cubetl/master/doc/img/screenshots/cv-ine-population.png "CubesViewer Screenshot")

## Further information

Inspect the CubETL process definition (ine_census.py),

## Data

This example data comes from the Spanish National Statistics Institute (INE):

* Población residente por fecha, sexo, grupo de edad y nacionalidad (agrupación de países) (2002) -
  [link](http://www.ine.es/dynt3/inebase/es/index.htm?padre=1894&capsel=1895)
* Población residente por fecha, sexo y grupo de edad (1971) -
  [link](http://www.ine.es/dynt3/inebase/index.htm?padre=1949&capsel=1953)



