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


def cubetl_config(ctx):

    ctx.include('${ ctx.library_path }/datetime.py')

    #ctx.include('${ ctx.library_path }/datetime.py')

    ctx.add('odoo.sql.connection',
            sql.Connection(url='sqlite:///odoo.sqlite3'))

    '''
    ctx.add('ine.autonomy', olap.Dimension(
        name='autonomy',
        label='Autonomy',
        attributes=[olap.Attribute('autonomy', type='String')]))

    ctx.add('ine.province', olap.Dimension(
        name='province',
        label='Province',
        attributes=[olap.Attribute('province', type='String')]))

    ctx.add('ine.autonomyprovince', olap.HierarchyDimension(
        name='autonomyprovince',
        label='Province',
        attributes=[DimensionAttribute(ctx.get('ine.autonomy')),
                    DimensionAttribute(ctx.get('ine.province'))]))

    ctx.add('ine.nationality', olap.Dimension(
        name='nationality',
        label='Nationality',
        attributes=[olap.Attribute('nationality', type='String')]))

    ctx.add('ine.census', olap.Fact(
        name='census',
        label='Census',
        #must_slice=ctx.get('cubetl.datetime.datemonthly'),  # study when and how dimensions can be aggregated, this cube requires slicing by date or results are invalid
        #natural_key=
        #notes='',
        attributes=[DimensionAttribute(ctx.get('cubetl.datetime.datemonthly')),
                    DimensionAttribute(ctx.get('ine.autonomyprovince')),
                    DimensionAttribute(ctx.get('ine.nationality')),
                    Measure(name='census', type='Integer', label="Population")]))  # TODO: Should not present avg/max/min
    '''


    # Generate a SQL star schema and mappings automatically
    #sqlschema.OLAPToSQL.olap2sql(ctx, connection=ctx.get('ine.sql.connection'))
    #ctx.get('olap2sql.olapmapper').entity_mapper(ctx.get('ine.census')).store_mode = TableMapper.STORE_MODE_INSERT

    ctx.add("odoo.conn",
            odoo.OdooConnection(url="http://127.0.0.1:8069", database="test", username="admin", password="admin"))

    # Define the data load process
    ctx.add('odoo.process', flow.Chain(steps=[

        #ctx.get('cubetl.config.print'),

        # Generate a Cubes model
        #cubes10.Cubes10ModelWriter(olapmapper=ctx.get('olap2sql.olapmapper'),
        #                           model_path="ine.cubes-model.json",
        #                           config_path="ine.cubes-config.ini"),

        sql.Transaction(connection=ctx.get('odoo.sql.connection')),

        #odoo.Execute(),
        #odoo.Dump('account.move'),

        script.Function(test),
        script.Function(test2),

        flow.Chain(fork=True, steps=[

            #script.Delete(['data', 'pcaxis']),

            #script.Function(process_data),

            #flow.Filter(condition="${ m['date'].year < 2002 }"),

            #cache.CachedTableLookup(
            #    table=ctx.get("ine.autonomy_province.table"),
            #    lookup={'province': lambda m: m['province_name']}),

            #ctx.get('cubetl.util.print'),
            util.PrettyPrint(depth=4),
            #util.Print(),

            #olap.Store(entity=ctx.get('ine.census'),
            #           mapper=ctx.get('olap2sql.olapmapper')),

            log.LogPerformance(),

            ]),

        ]))


def test(ctx, m):

    conn = ctx.get("odoo.conn")
    partner_ids = conn.execute('res.partner', 'search', [])  #, [ ('customer', '=', True ) ])

    partner_fields = ['name', 'active', 'ref', 'lang', 'vat', 'customer', 'parent_id',
                      'reseller_id', 'country', 'property_account_position', 'phone', 'property_product_pricelist',
                      'website', 'category_id', 'user_id', 'category_id']
    for partner_id in partner_ids:
        partner = conn.execute('res.partner', 'read', partner_id, partner_fields)  # []
        #print(partner)
    m["partner"] = partner[0]


def test2(ctx, m):

    conn = ctx.get("odoo.conn")
    invoice_ids = conn.execute('account.invoice', 'search', [])  #, [ ('customer', '=', True ) ])

    for invoice_id in invoice_ids:
        invoice = conn.execute('account.invoice', 'read', invoice_id, [])
        print(invoice)

    #m.update(invoice[0])
    m["invoice"] = invoice[0]


def process_data(ctx, m):

    # For date dimension
    m['year'] = m['date'].year
    m['quarter'] = int((m["date"].month - 1) / 3) + 1
    m['month'] = m['date'].month


