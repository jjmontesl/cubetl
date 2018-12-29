#!/bin/bash

# Inspect database and generate OLAP schema, and Cubes model and config
cubetl cubetl.sql.db2sql cubetl.olap.sql2olap cubetl.cubes.olap2cubes \
        -p db2sql.db_url=sqlite:///Chinook_Sqlite.sqlite \
        -p olap2cubes.cubes_model=chinook.cubes-model.json \
        -p olap2cubes.cubes_config=chinook.cubes-config.ini
        
        

