#!/bin/bash

# Inspect database and generate OLAP schema, and Cubes model and config
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
        $*

