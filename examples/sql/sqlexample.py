# CubETL
# Copyright (c) 2013-2019 Jose Juan Montes

# This is a CubETL example
# See: https://github.com/jjmontesl/cubetl


import datetime

from cubetl import text, flow, fs, script, olap, pcaxis, table, util
from cubetl.cubes import cubes10
from cubetl.olap import sqlschema
from cubetl.olap.sql import TableMapper
from cubetl.sql import sql, schemaimport
from cubetl.table import cache
from cubetl.util import log
from cubetl.sdmx import sdmx
from cubetl.sql.sql import SQLTable, SQLColumn


def cubetl_config(ctx):

    # Input database connection
    ctx.add('example.sql.connection',
            sql.Connection(url='sqlite:///Chinook_Sqlite.sqlite'))

    # Read database schema
    schemaimport.DBToSQL.db2sql(ctx, ctx.get("example.sql.connection"))


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

    # Process
    ctx.add('example.process', flow.Chain(steps=[

        sql.Transaction(connection=ctx.get('example.sql.connection_out')),

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

        sql.StoreRow(sqltable=ctx.get('example.agg.table'), store_mode=sql.SQLTable.STORE_MODE_UPSERT),

        log.LogPerformance(),

    ]))

