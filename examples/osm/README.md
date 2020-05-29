# OSM to Elastic ETL Pipeline

This example defies a simple ETL process that reads an OpenStreetMap export
file and loads an Elasticsearch index with the data.

The pipeline loads node names and locations. Ways and relations
are simply loaded as names. Tags are not interpreted.


## Dependencies

For this example you need the pyosmium package and the corresponding system libraries
installed. Osmium is the library that deals with OpenStreetMap formats.

    pip install osmium

You also need a running Elasticsearch stack, and a Kibana instance in
order to inspect the data. This directory includes example 'docker-compose.yaml'
config and a 'start-docker-elasticsearch.sh' shell script that can be used
to start a number of docker containers running the appropriate services.

You also need an OpenStreetMap PBF data file of the region of interest.


## Running

Make sure your Elasticsearch instance is runnign and accessible through
http://localhost:9200 (see dependencies above).

As usual, inspect the list of available targets in this config using `-l`:

    cubetl osmiumelastic.py -l

The target we look for is `osmiumelastic.process`. Run it:

    cubetl osmiumelastic.py osmiumelastic.process -q

It will start processing data from the OpenStreetMap export file.

Note that **printing items slows down your process**. The '-q' option
above silences print nodes in order to avoid a performance penalty.

(Note that the process may take several hours process, depending on
the size of the input file.)


## Test

You can run a simple query using the `osmiumelastic.search` target:

    cubetl osmiumelastic.py osmiumelastic.search -m q=Parking


## Inspecting

You can use Elasticsearch Kibana to inspect your index.


## Note on hardware usage / possible memory leak

Running this process for entire Spain allocated over 8GB of RAM. Looks like a possible memory
leak and needs further investigation. Please bear this in mind during your tests.

Also, while running the Elasticsearch cluster on the same host, the machine used 17GB of RAM overall.
The process for 6 million objects (nodes, ways and relations) took about 1 hour on an 8-core i7 with SSD storage.

    INFO - Performance - Time: 3249 Mem: 8782.730 MB Messages: 2697886 (+26553) Rate global: 830.271 msg/s Rate current: 2629.977 msg/s


## Further information

Inspect the CubETL process definition (osmiumelastic.py),


