# CubETL
# Copyright (c) 2013-2019 Jose Juan Montes

# This is a CubETL example
# See: https://github.com/jjmontesl/cubetl


import datetime

from cubetl import text, flow, fs, script, olap, pcaxis, table
from cubetl.cubes import cubes10
from cubetl.olap import sqlschema
from cubetl.olap.sql import TableMapper
from cubetl.sql import sql
from cubetl.table import cache
from cubetl.util import log
from cubetl.sdmx import sdmx


def cubetl_config(ctx):

    ctx.add('estat.sql.connection',
            sql.Connection(url='sqlite:///estat.sqlite3'))

    # Load SDMX schema and transform it to CubETL OLAP entities
    sdmx.SDMXToOLAP.sdmx2olap(ctx,
                              path_dsd='data/eip_ext1.dsd.xml',
                              fact_name='estat_eip',
                              fact_label='Eurostat / Entrepreneurship Indicator Programme')

    # Generate a SQL schema from the OLAP schema
    sqlschema.OLAPToSQL.olap2sql(ctx, connection=ctx.get('estat.sql.connection'))  # store_mode='insert'
    ctx.get('olap2sql.olapmapper').entity_mapper(ctx.get('smdx2olap.fact.estat_eip')).store_mode = TableMapper.STORE_MODE_INSERT


    # Define the data load process
    ctx.add('estat.process.eip', flow.Chain(steps=[

        #ctx.get('cubetl.config.print'),

        # Generate a Cubes model
        cubes10.Cubes10ModelWriter(olapmapper=ctx.get('olap2sql.olapmapper'),
                                   model_path="estat.cubes-model.json",
                                   config_path="estat.cubes-config.ini"),

        sql.Transaction(connection=ctx.get('estat.sql.connection')),

        flow.Chain(fork=True, steps=[

            sdmx.SDMXFileReader(path_dsd='data/eip_ext1.dsd.xml',
                                path_sdmx='data/eip_ext1.sdmx.xml'),

            ctx.get('cubetl.util.print'),

            olap.Store(entity=ctx.get('smdx2olap.fact.estat_eip'),
                       mapper=ctx.get('olap2sql.olapmapper')),

            log.LogPerformance(),

            ]),

        ]))

