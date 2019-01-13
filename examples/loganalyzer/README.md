# Log analysis example

This example defines an OLAP datastore for HTTP request data like the data that
can be obtained from a web server log file.

Each fact in this OLAP schema is an HTTP request. Dimensions are HTTP attributes
like the origin, the user agent, the request path, client country...

A sample apache2 log file is included for the process.

## Dependencies

For this example you need to have the following libraries available:

    pip install GeoIP user_agents

## Running

As usual, inspect the list of available targets in this config using `-l`:

    cubetl loganalyzer.py -l

The target we look for is `loganalyzer.process`. Run it:

    cubetl loganalyzer.py loganalyzer.process

It will start processing the sample Apache2 log file and print each processed
item to the terminal.

Note that **printing items slows down your process**. In order to run the
entire process, interrupt it *(CTRL-C)* and run it again using the `-q` option,
which silences print nodes:

    cubetl loganalyzer.py loganalyzer.process -q

(The process takes a few minutes.)

## Serving the data

We now have a SQLite database. Run Cubes in order to serve
analytical queries for this database:

    pip install https://github.com/DataBrewery/cubes/archive/master.zip click flask --upgrade
    slicer serve loganalyzer.cubes-config.ini  &

Note the `&` argument, which makes the process run in background. You could instead
use a separate terminal window if you wish.

Now run CubesViewer to inspect the dataset:

    # Note: not yet available, please download and use CubesViewer manually!
    cvutils cv

This will start a local HTTP service running on port 8085 for CubesViewer studio.
It will also open a browser pointing to the application.

You can now inspect the *HTTP Requests* cube using CubesViewer.

## Further information

Inspect the CubETL process definition (ine_census.py),

