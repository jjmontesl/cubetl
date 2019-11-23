# CubETL
# Copyright (c) 2013-2019 CubETL Contributors

# This is a CubETL example
# See: https://github.com/jjmontesl/cubetl


import datetime

from cubetl import text, flow, fs, script, olap, pcaxis, table, util
from cubetl.cubes import cubes10
from cubetl.olap import sqlschema, DimensionAttribute, Measure
from cubetl.olap.sql import TableMapper
from cubetl.sql import sql
from cubetl.odoo import odoo
from cubetl.table import cache
from cubetl.util import log
from cubetl.text import functions
from cubetl.elastic import elasticsearch


def cubetl_config(ctx):

    ctx.props['file_path'] = ctx.props.get('file_path', '../loganalyzer/access.log')

    ctx.include('${ ctx.library_path }/datetime.py')
    ctx.include('${ ctx.library_path }/http.py')

    #ctx.include('${ ctx.library_path }/datetime.py')

    ctx.add('es.connection',
            elasticsearch.ElasticsearchConnection(url='http://localhost:9200'))

    # Define the data load process
    ctx.add('es.process', flow.Chain(steps=[

        fs.FileLineReader(path='${ ctx.props["file_path"] }', encoding=None),

        ctx.get('cubetl.http.parse.apache_combined'),

        util.PrettyPrint(depth=4),

        elasticsearch.Index(es=ctx.get("es.connection"),
                            index="test-index",
                            doc_type="logline",
                            data_id=lambda m: m['data']),

        log.LogPerformance(),
    ]))

    ctx.add('es.search', flow.Chain(steps=[
        elasticsearch.Search(es=ctx.get("es.connection"),
                             index="test-index",
                             query=None),
        util.PrettyPrint(depth=4),
        log.LogPerformance(),
    ]))


