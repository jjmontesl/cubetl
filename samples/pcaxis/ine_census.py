

import random
from cubetl import text, flow, fs, script, olap, pcaxis, table
from cubetl.util import log
from cubetl.table import cache
from cubetl.sql import sql
from cubetl.olap import sqlschema
import datetime
from cubetl.cubes import cubes10
from cubetl.olap.sql import TableMapper


def cubetl_config(ctx):

    ctx.include('${ ctx.library_path }/cubetl_datetime.py')
    ctx.include('${ ctx.library_path }/cubetl_person.py')

    ctx.add('ine.sql.connection',
            sql.Connection(url='sqlite:///cubetl-examples.sqlite3'))

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
        levels=[ctx.get('ine.autonomy'),
                ctx.get('ine.province')]))

    ctx.add('ine.nationality', olap.Dimension(
        name='nationality',
        label='Nationality',
        attributes=[olap.Attribute('nationality', type='String')]))

    ctx.add('ine.census', olap.Fact(
        name='census',
        label='Census',
        #natural_key=
        #notes='',
        dimensions=[#olap.Attribute('sample_date', label="Date", entity=ctx.get('cubetl.datetime.datemonthly')),
                    ctx.get('cubetl.datetime.datemonthly'),
                    ctx.get('ine.autonomyprovince'),
                    ctx.get('ine.nationality'),
                    ctx.get('cubetl.person.gender'),
                    ctx.get('cubetl.person.age_range')],
        measures=[olap.Measure(name='census', type='Integer', label="Population")]))

    # Generate a SQL star schema and mappings automatically
    sqlschema.OlapSQLSchema.generate_star_schema_mapper(ctx,
                                                        connection=ctx.get('ine.sql.connection'))
    ctx.get('olap2sql.olapmapper').entity_mapper(ctx.get('ine.census')).store_mode = TableMapper.STORE_MODE_INSERT


    '''
    !!python/object:cubetl.olap.OlapMapper
    id: dp.ine.census.olapmapper
    #include:
    mappers:
    - !!python/object:cubetl.olap.sql.CompoundHierarchyDimensionMapper
      entity: !ref cubetl.datetime.datemonthly
      table: datemonthly
      connection: !ref dp.sql.connection
      eval:
      - name: _cubetl_datetime_date
        value: ${ m['date'] }
      mappings:
      - !ref cubetl.datetime.mappings
    - !!python/object:cubetl.olap.sql.DimensionMapper
      entity: !ref dp.ine.autonomy
      table: ine_autonomy
      connection: !ref dp.sql.connection
      lookup_cols: autonomy
      mappings:
      - name: autonomy_id
        pk: True
        type: AutoIncrement
      - name: autonomy
        value: ${ m["autonomy_name"] }
    - !!python/object:cubetl.olap.sql.DimensionMapper
      entity: !ref dp.ine.province
      table: ine_province
      connection: !ref dp.sql.connection
      lookup_cols: province
      mappings:
      - name: province_id
        pk: True
        type: Integer
        value: ${ m['province_id'] }
      - name: province
        value: ${ m['province_name'] }
    - !!python/object:cubetl.olap.sql.MultiTableHierarchyDimensionMapper
      entity: !ref dp.ine.autonomyprovince
    - !!python/object:cubetl.olap.sql.DimensionMapper
      entity: !ref dp.ine.nationality
      table: ine_nationality
      connection: !ref dp.sql.connection
      lookup_cols: nationality
      mappings:
      - name: id
        pk: True
        type: AutoIncrement
    - !!python/object:cubetl.olap.sql.DimensionMapper
      entity: !ref dp.ine.age
      table: ine_age
      connection: !ref dp.sql.connection
      lookup_cols: age
      mappings:
      - name: id
        pk: True
        type: AutoIncrement
    - !!python/object:cubetl.olap.sql.DimensionMapper
      entity: !ref dp.ine.genre
      table: ine_genre
      connection: !ref dp.sql.connection
      lookup_cols: genre
      mappings:
      - name: id
        pk: True
        type: AutoIncrement
    - !!python/object:cubetl.olap.sql.FactMapper
      entity: !ref dp.ine.census
      table: ine_census
      connection: !ref dp.sql.connection
      lookup_cols: datemonthly_id, autonomy_id, province_id, genre_id, nationality_id, age_id
      store_mode: insert
      auto_store:
      - !ref cubetl.datetime.datemonthly
      - !ref dp.ine.nationality
      - !ref dp.ine.genre
      - !ref dp.ine.age
      mappings:
      - name: id
        pk: True
        type: AutoIncrement
    '''

    ctx.add('ine.process.census', flow.Chain(steps=[

        ctx.get('cubetl.config.print'),

        # Generate a Cubes model
        cubes10.Cubes10ModelWriter(olapmapper=ctx.get('olap2sql.olapmapper'),
                                   model_path="ine.model.json"),
        #cubes10.Cubes10ModelWriter(config_path="ine.slicer.ini"),
        script.Delete(['cubesmodel', 'cubesmodel_json']),

        sql.Transaction(connection=ctx.get('ine.sql.connection')),

        fs.FileReader(path='census.px', encoding=None),
        pcaxis.PCAxisParser(),

        flow.Chain(fork=True, steps=[

            pcaxis.PCAxisIterator(),
            script.Delete(['data', 'pcaxis']),

            flow.Filter(condition="${ m['Sexo'] != 'Ambos sexos' }"),
            flow.Filter(condition="${ m['Grupo quinquenal de edad'] != 'Total' }"),
            flow.Filter(condition="${ m['Nacionalidad'] != 'Total' }"),
            flow.Filter(condition="${ m['Provincias'] != 'Total Nacional' }"),

            #flow.Skip(skip="${ random.randint(1, 1000) }"),
            #flow.Limit(limit=5000),

            script.Function(process_data),

            cache.CachedTableLookup(
                table=ctx.get("ine.autonomy_province.table"),
                lookup={'province': lambda m: m['province_name']}),

            ctx.get('cubetl.util.print'),

            #olap.Store(entity=ctx.get('ine.autonomy'),
            #           mapper=ctx.get('olap2sql.olapmapper')),
            #olap.Store(entity=ctx.get('ine.province'),
            #           mapper=ctx.get('olap2sql.olapmapper')),
            #olap.Store(entity=ctx.get('ine.autonomyprovince'),
            #           mapper=ctx.get('olap2sql.olapmapper')),
            olap.Store(entity=ctx.get('ine.census'),
                       mapper=ctx.get('olap2sql.olapmapper')),


            log.LogPerformance(),

            ]),

        ]))


def process_data(ctx, m):

    m['date'] = datetime.datetime(int(m['Periodo'].split(" ")[-1]), 7 if 'julio' in m['Periodo'] else 1, 1)
    m['nationality'] = m['Nacionalidad']
    m['age_range'] = m['Grupo quinquenal de edad']
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
    del(m['Sexo'])
    del(m['value'])

