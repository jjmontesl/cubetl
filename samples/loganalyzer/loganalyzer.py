
import random
from cubetl import text, flow, fs, script, olap, pcaxis, table, geoip
from cubetl.util import log
from cubetl.table import cache
from cubetl.sql import sql
from cubetl.olap import sqlschema
import datetime
from cubetl.cubes import cubes10
from cubetl.olap.sql import TableMapper
from cubetl.text import functions
from cubetl.net import useragent


def cubetl_config(ctx):

    #ctx.include('${ ctx.library_path }/datetime.py')
    #ctx.include('${ ctx.library_path }/geo.py')
    ctx.include('${ ctx.library_path }/http.py')
    #ctx.include('${ ctx.library_path }/net.py')

    ctx.props['db_url'] = 'sqlite:///loganalyzer.sqlite3'

    ctx.props['domain'] = 'cubesviewer.com'  #  ctx.interpolate('${ }')
    ctx.props['file_path'] = ctx.props.get('file_path', 'access.log')

    ctx.props['download_extensions'] = 'zip, tgz, gz, 7z, rar, iso, msi, exe, avi, mp3, mp4, ogg, mkv'
    ctx.props['download_extensions_list'] = [e.strip().lower() for e in ctx.props['download_extensions'].split(',')]
    ctx.props['download_size_bytes'] = 10 * 1024 * 1024

    # Database connection for loaded OLAP data
    ctx.add('loganalyzer.sql.connection',
            sql.Connection(url=ctx.interpolate(None, '${ ctx.props["db_url"] }')))

    # Generate a SQL star schema and mappings automatically
    sqlschema.OlapSQLSchema.generate_star_schema_mapper(ctx,
                                                        connection=ctx.get('loganalyzer.sql.connection'))
    ctx.get('olap2sql.olapmapper').entity_mapper(ctx.get('cubetl.http.request')).store_mode = TableMapper.STORE_MODE_INSERT

    '''
    !!python/object:cubetl.olap.OlapMapper
    id: cubetl.http.request.olapmapper
    mappers:
    #- !!python/object:cubetl.olap.sql.CompoundHierarchyDimensionMapper
    - !!python/object:cubetl.olap.sql.EmbeddedDimensionMapper
      entity: !ref cubetl.http.request.datetime
      #table: date
      #connection: !ref cubelogs.sql.connection
      eval:
      - name: _cubetl_datetime_date
        value: ${ m['datetime'] }
      mappings:
      - !ref cubetl.datetime.mappings
    - !!python/object:cubetl.olap.sql.CompoundHierarchyDimensionMapper
      entity: !ref cubetl.http.request.client_country
      table: country
      connection: !ref cubelogs.sql.connection
      eval:
      - name: geoip_cont_name
        default: Unknown
      - name: geoip_country_name
        default: Unknown
      - name: geoip_country_code
        default: UNKNOWN
      mappings:
      - !ref cubetl.geo.mappings
    - !!python/object:cubetl.olap.sql.CompoundHierarchyDimensionMapper
      entity: !ref cubetl.http.request.path
      table: request_path
      connection: !ref cubelogs.sql.connection
      lookup_cols: request_path1, request_path2, request_path3, request_path4
      mappings:
      - name: id
        pk: True
      - name: request_path1
        value: ${ m["path1"] }
      - name: request_path2
        value: ${ m["path2"] }
      - name: request_path3
        value: ${ m["path3"] }
      - name: request_path4
        value: ${ m["path4"] }
    - !!python/object:cubetl.olap.sql.DimensionMapper
      entity: !ref cubetl.http.request.client_address
      table: address
      connection: !ref cubelogs.sql.connection
      lookup_cols: address
      mappings:
      - name: id
        pk: True
      - name: address
        value: ${ m["address"] }
        type: String
    - !!python/object:cubetl.olap.sql.DimensionMapper
      entity: !ref cubetl.http.request.protocol
      table: protocol
      connection: !ref cubelogs.sql.connection
      mappings:
      - name: id
        value: ${ text.slugu(m["protocol"]) }
        pk: True
        type: String
      - name: protocol
        value: ${ m["protocol"] }
        type: String
    - !!python/object:cubetl.olap.sql.DimensionMapper
      entity: !ref cubetl.http.request.method
      table: http_method
      connection: !ref cubelogs.sql.connection
      lookup_cols: http_method
      mappings:
      - name: id
        pk: True
      - name: http_method
        value: ${ m["http_method"] }
        type: String
    - !!python/object:cubetl.olap.sql.CompoundHierarchyDimensionMapper
      entity: !ref cubetl.http.status
      table: http_status
      connection: !ref cubelogs.sql.connection
      lookup_cols: code
      mappings:
      - name: code
        pk: True
        type: Integer
        value: ${ m["status_code"] }
      - name: description
        value: ${ m["status_description"] }
        type: String
      - name: type
        value: ${ m["status_type"] }
        type: String
    - !!python/object:cubetl.olap.sql.CompoundHierarchyDimensionMapper
      entity: !ref cubetl.http.referer.domain
      table: domain3
      connection: !ref cubelogs.sql.connection
      lookup_cols: tld, domain, subdomain
      mappings:
      - name: id
        pk: True
    - !!python/object:cubetl.olap.sql.CompoundHierarchyDimensionMapper
      entity: !ref cubetl.http.referer
      table: http_referer
      connection: !ref cubelogs.sql.connection
      lookup_cols: referer_domain, referer_path
      mappings:
      - name: id
        pk: True
      - name: referer_domain
        value: ${ m["referer_domain"] }
        type: String
      - name: referer_path
        value: ${ m["referer_path"] }
        type: String
    - !!python/object:cubetl.olap.sql.CompoundHierarchyDimensionMapper
      entity: !ref cubetl.http.user_agent
      table: user_agent
      connection: !ref cubelogs.sql.connection
      lookup_cols: user_agent_family, user_agent_version
      mappings:
      - name: id
        pk: True
      - name: user_agent_family
        value: ${ m["user_agent_family"] }
        type: String
      - name: user_agent_version
        value: ${ m["user_agent_version"] }
        type: String
    - !!python/object:cubetl.olap.sql.CompoundHierarchyDimensionMapper
      entity: !ref cubetl.os.operating_system
      table: operating_system
      connection: !ref cubelogs.sql.connection
      lookup_cols: operating_system_family, operating_system_version
      mappings:
      - name: id
        pk: True
      - name: operating_system_family
        value: ${ m["operating_system_family"] }
        type: String
      - name: operating_system_version
        value: ${ m["operating_system_version"] }
        type: String
    - !!python/object:cubetl.olap.sql.CompoundHierarchyDimensionMapper
      entity: !ref cubetl.http.mimetype
      table: mimetype
      connection: !ref cubelogs.sql.connection
      lookup_cols: mimetype_type, mimetype_subtype
      mappings:
      - name: id
        pk: True
      - name: mimetype_type
        value: ${ m["mimetype_type"] }
        type: String
      - name: mimetype_subtype
        value: ${ m["mimetype_subtype"] }
        type: String
    - !!python/object:cubetl.olap.sql.DimensionMapper
      entity: !ref cubetl.os.device
      table: device
      connection: !ref cubelogs.sql.connection
      lookup_cols: device
      mappings:
      - name: device_id
        pk: True
        value: ${ text.slugu(m["device"]) }
        type: String
      - name: device
        value: ${ m["device"] }
        type: String
    - !!python/object:cubetl.olap.sql.EmbeddedDimensionMapper
      entity: !ref cubetl.http.request.is_bot
      mappings:
      - name: is_bot
        value: ${ m["ua_is_bot"] }
        type: Boolean
    - !!python/object:cubetl.olap.sql.EmbeddedDimensionMapper
      entity: !ref cubetl.http.request.referer_origin
      mappings:
      - name: referer_origin
        value: ${ m["referer_origin"] }
        type: String
    - !!python/object:cubetl.olap.sql.FactMapper
      entity: !ref cubetl.http.request
      table: http_request
      connection: !ref cubelogs.sql.connection
      store_mode: insert
      lookup_cols: id
      mappings:
      - name: id
        pk: True
    '''


    ctx.add('loganalyzer.process', flow.Chain(steps=[

        ctx.get('cubetl.config.print'),

        # Generate a Cubes model
        cubes10.Cubes10ModelWriter(olapmapper=ctx.get('olap2sql.olapmapper'),
                                   model_path="loganalyzer.model.json"),
        #cubes10.Cubes10ModelWriter(config_path="ine.slicer.ini"),
        script.Delete(['cubesmodel', 'cubesmodel_json']),

        sql.Transaction(connection=ctx.get('loganalyzer.sql.connection')),

        fs.FileLineReader(path='${ ctx.props["file_path"] }', encoding=None),

        ctx.get('cubetl.http.parse.apache_combined'),

        geoip.GeoIPFromAddress(data="${ m['address'] }"),
        useragent.UserAgentParse(data="${ m['user_agent_string'] }"),

        cache.CachedTableLookup(
            table=ctx.get("cubetl.http.status.table"),
            lookup={'status_code': lambda m: m['status_code']},
            default={'status_description': 'Unknown'}),

        script.Function(process_data),

        ctx.get('cubetl.util.print'),

        #olap.Store(entity=ctx.get('ine.autonomyprovince'),
        #           mapper=ctx.get('olap2sql.olapmapper')),
        olap.Store(entity=ctx.get('cubetl.http.request'),
                   mapper=ctx.get('olap2sql.olapmapper')),


        log.LogPerformance(),

        ]))


def process_data(ctx, m):

    m['datetime'] = functions.extract_date(m['date_string'], dayfirst=True)
    m['client_address'] = m['address']

    # For date dimension
    m['year'] = m['datetime'].year
    m['quarter'] = int((m["datetime"].month - 1) / 3) + 1
    m['month'] = m['datetime'].month
    m['day'] = m['datetime'].day
    m['week'] = int(m["datetime"].strftime('%W'))

    m['http_method'] = m['verb'].split(' ')[0]
    m['protocol'] = m['verb'].split(' ')[-1]

    m['referer_domain'] = functions.urlparse(m['referer']).hostname
    m['referer_path'] = functions.urlparse(m['referer']).path
    m['referer_origin'] = ("Internal" if m['referer_domain'].endswith(ctx.props['domain']) else "External") if m['referer_domain'] else "Unknown"

    m['tld'] = m['referer_domain'].split('.')[-1] if (m['referer_domain'] and len(m['referer_domain'].split('.')) > 0) else ''
    m['domain'] = m['referer_domain'].split('.')[-2] if (m['referer_domain'] and len(m['referer_domain'].split('.')) > 1) else ''
    m['subdomain'] = m['referer_domain'].split('.')[-3] if (m['referer_domain'] and len(m['referer_domain'].split('.')) > 2) else ''

    m['continent_code'] = functions.slugu(m["geoip_cont_name"]) if m["geoip_cont_name"] else "unknown"
    m['continent_name'] = m["geoip_cont_name"]
    m['country_iso2'] = m["geoip_country_code"]
    m['country_name'] = m["geoip_country_name"]

    m['user_agent_family'] = m['ua_user_agent_family']
    m['user_agent_version'] = m['ua_user_agent_version_string']
    m['operating_system_family'] = m['ua_os_family']
    m['operating_system_version'] = m['ua_os_version_string']
    m['device'] = m['ua_device_family']

    m['status_description'] = "%s %s" % (m['status_code'], m['status_description'])
    m['type_label'] = m['status_type']
    m['type_code'] = functions.slugu(m['status_type'])

    m['path'] = " ".join(m['verb'].split(' ')[1:-1]).split('?')[0]
    m['path1'] =  m['path'].split('/')[1] if len(m['path'].split('/')) > 1 else ""
    m['path2'] = m['path'].split('/')[2] if len(m['path'].split('/')) > 2 else ""
    m['path3'] = m['path'].split('/')[3] if len(m['path'].split('/')) > 3 else ""
    m['path4'] = "/".join(m['path'].split('/')[4:]) if len(m['path'].split('/')) > 4 else ""

    m['mimetype'] = functions.mimetype_guess(m['path']) or "Unknown/Unknown"
    m['mimetype_type'] = m['mimetype'].split('/')[0]
    m['mimetype_subtype'] = m['mimetype'].split('/')[1]

    m['file_extension'] = m['path'].split('.')[-1] if (len(m['path'].split('.')) > 0) else ""
    m['is_download'] = m['file_extension'].lower() in ctx.props['download_extensions_list'] or int(m['served_bytes']) >= int(ctx.props['download_size_bytes'])
    m['is_bot'] = (m['device'] == 'Spider')

    # response size category (small, medium, large...), and sub category (<1k, <10k, <100k, etc)
    # response size metric in log2

    # visitors / sessions / hits - track each user entry (visitor=ip+os+caching_analysis in session, session=time_span+ip+full-user-agent)
    # first visit, last visit, entry pages/exit pages

    # anomaly/500 (many 5xx any URL any IP in short period)
    # anomaly/404 (many 4xx in short period same IP -visit)

    # tracking javascript ??? (screen size, browser size, support, sessions, campaign / organic...)
    # IP PTR -> SOA authority ( IP owner? )
    # organic/etc by referer?? (requires db)

