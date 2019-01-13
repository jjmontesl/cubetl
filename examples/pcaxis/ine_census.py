# CubETL
# Copyright (c) 2013-2019 Jose Juan Montes

# This is a CubETL example
# See: https://github.com/jjmontesl/cubetl


import datetime

from cubetl import text, flow, fs, script, olap, pcaxis, table
from cubetl.cubes import cubes10
from cubetl.olap import sqlschema, DimensionAttribute, Measure
from cubetl.olap.sql import TableMapper
from cubetl.sql import sql
from cubetl.table import cache
from cubetl.util import log
from cubetl.text import functions


def cubetl_config(ctx):

    ctx.include('${ ctx.library_path }/datetime.py')
    ctx.include('${ ctx.library_path }/person.py')

    ctx.add('ine.sql.connection',
            sql.Connection(url='sqlite:///ine.sqlite3'))

    ctx.add('ine.autonomy_province.table', table.CSVMemoryTable(
        data='''
            province,autonomy
            Albacete,Castilla la Mancha
            Alicante/Alacant,Comunidad Valenciana
            Almería,Andalucía
            Araba/Álava,País Vasco
            Asturias,Asturias
            Ávila,Castilla y León
            Badajoz,Extremadura
            "Balears, Illes",Comunidad Balear
            Barcelona,Cataluña
            Bizkaia,País Vasco
            Burgos,Castilla y León
            Cáceres,Extremadura
            Cádiz,Andalucía
            Cantabria,Cantabria
            Castellón/Castelló,Comunidad Valenciana
            Ciudad Real,Castilla la Mancha
            Córdoba,Andalucía
            "Coruña, A",Galicia
            Cuenca,Castilla la Mancha
            Gipuzkoa,País Vasco
            Girona,Cataluña
            Granada,Andalucía
            Guadalajara,Castilla la Mancha
            Huelva,Andalucía
            Huesca,Aragón
            Jaén,Andalucía
            León,Castilla y León
            Lleida,Cataluña
            Lugo,Galicia
            Madrid,Madrid
            Málaga,Andalucía
            Murcia,Murcia
            Navarra,Aragón
            Ourense,Galicia
            Palencia,Castilla y León
            "Palmas, Las",Canarias
            Pontevedra,Galicia
            "Rioja, La",Rioja
            Salamanca,Castilla y León
            Santa Cruz de Tenerife,Canarias
            Segovia,Castilla y León
            Sevilla,Andalucía
            Soria,Castilla y León
            Tarragona,Cataluña
            Teruel,Aragón
            Toledo,Castilla la Mancha
            Valencia/València,Comunidad Valenciana
            Valladolid,Castilla y León
            Zamora,Castilla y León
            Zaragoza,Aragón
            Ceuta,Ciudades Autónomas
            Melilla,Ciudades Autónomas
        '''))

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
                    DimensionAttribute(ctx.get('cubetl.person.gender')),
                    DimensionAttribute(ctx.get('cubetl.person.age_range')),
                    Measure(name='census', type='Integer', label="Population")]))  # TODO: Should not present avg/max/min


    # Generate a SQL star schema and mappings automatically
    sqlschema.OLAPToSQL.olap2sql(ctx, connection=ctx.get('ine.sql.connection'))
    ctx.get('olap2sql.olapmapper').entity_mapper(ctx.get('ine.census')).store_mode = TableMapper.STORE_MODE_INSERT


    # Define the data load process
    ctx.add('ine.process', flow.Chain(steps=[

        #ctx.get('cubetl.config.print'),

        # Generate a Cubes model
        cubes10.Cubes10ModelWriter(olapmapper=ctx.get('olap2sql.olapmapper'),
                                   model_path="ine.cubes-model.json",
                                   config_path="ine.cubes-config.ini"),

        sql.Transaction(connection=ctx.get('ine.sql.connection')),

        fs.FileReader(path='census-2002.px', encoding=None),
        pcaxis.PCAxisParser(),

        flow.Chain(fork=True, steps=[

            pcaxis.PCAxisIterator(),
            script.Delete(['data', 'pcaxis']),

            flow.Filter(condition="${ m['Sexo'] != 'Ambos sexos' }"),
            flow.Filter(condition="${ m['Grupo quinquenal de edad'] != 'Total' }"),
            #flow.Filter(condition="${ m['Grupo de edad'] != 'Total' }"),
            flow.Filter(condition="${ m['Nacionalidad'] != 'Total' }"),
            flow.Filter(condition="${ m['Provincias'] != 'Total Nacional' }"),

            #flow.Skip(skip="${ random.randint(1, 1000) }"),
            #flow.Limit(limit=5000),

            script.Function(process_data),

            #flow.Filter(condition="${ m['date'].year < 2002 }"),

            cache.CachedTableLookup(
                table=ctx.get("ine.autonomy_province.table"),
                lookup={'province': lambda m: m['province_name']}),

            ctx.get('cubetl.util.print'),

            olap.Store(entity=ctx.get('ine.census'),
                       mapper=ctx.get('olap2sql.olapmapper')),


            log.LogPerformance(),

            ]),

        ]))


def process_data(ctx, m):

    m['date'] = datetime.datetime(int(m['Periodo'].split(" ")[-1]), 7 if 'julio' in m['Periodo'] else 1, 1)
    m['nationality'] = m.get('Nacionalidad', 'Unknown')
    m['age_range'] = m.get('Grupo quinquenal de edad', None) or m.get('Grupo de edad')
    m['age_num'] = functions.re_search('(\d+)', m['age_range'])
    m['census'] = m['value']

    # For autonomy dimension
    m['province_name'] = " ".join(m['Provincias'].split(' ')[1:])
    m['province_id'] = m['Provincias'].split(' ')[0]

    # For date dimension
    m['year'] = m['date'].year
    m['quarter'] = int((m["date"].month - 1) / 3) + 1
    m['month'] = m['date'].month

    # For gender dimension
    m['gender'] = m['Sexo']
    m['color'] = None
    m['icon'] = None

    del(m['Provincias'])
    del(m['Nacionalidad'])
    del(m['Periodo'])
    del(m['Grupo quinquenal de edad'])
    #del(m['Grupo de edad'])
    del(m['Sexo'])
    del(m['value'])

