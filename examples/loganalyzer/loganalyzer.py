# CubETL
# Copyright (c) 2013-2019 Jose Juan Montes

# This is a CubETL example
# See: https://github.com/jjmontesl/cubetl


from cubetl import flow, fs, script, olap, table, geoip
from cubetl.cubes import cubes10
from cubetl.http import useragent
from cubetl.olap import sqlschema, query
from cubetl.olap.sql import TableMapper
from cubetl.sql import sql
from cubetl.table import cache
from cubetl.text import functions
from cubetl.util import log


def cubetl_config(ctx):

    #ctx.include('${ ctx.library_path }/datetime.py')
    #ctx.include('${ ctx.library_path }/geo.py')
    #ctx.include('${ ctx.library_path }/net.py')
    ctx.include('${ ctx.library_path }/http.py')

    # Process configuration

    ctx.props['db_url'] = 'sqlite:///loganalyzer.sqlite3'

    ctx.props['domain'] = 'cubesviewer.com'  #  ctx.interpolate('${ }')
    ctx.props['file_path'] = ctx.props.get('file_path', 'access.log')

    ctx.props['download_extensions'] = 'zip, tgz, gz, 7z, rar, iso, msi, exe, avi, mp3, mp4, ogg, mkv, pdf'
    ctx.props['download_extensions_list'] = [e.strip().lower() for e in ctx.props['download_extensions'].split(',')]
    ctx.props['download_size_bytes'] = 10 * 1024 * 1024


    # Database connection for loaded OLAP data
    ctx.add('loganalyzer.sql.connection',
            #sql.Connection(url=lambda ctx: ctx.props.get("db_url", None))
            sql.Connection(url='sqlite:///loganalyzer.sqlite3'))


    # Generate a SQL star schema and mappings automatically
    sqlschema.OLAPToSQL.olap2sql(ctx, connection=ctx.get('loganalyzer.sql.connection'))
    ctx.get('olap2sql.olapmapper').entity_mapper(ctx.get('cubetl.http.request')).store_mode = TableMapper.STORE_MODE_INSERT


    # Processes a log file and loads the database for OLAP
    ctx.add('loganalyzer.process', flow.Chain(steps=[

        ctx.get('cubetl.config.print'),

        # Generate a Cubes model
        cubes10.Cubes10ModelWriter(olapmapper=ctx.get('olap2sql.olapmapper'),
                                   model_path="loganalyzer.cubes-model.json",
                                   config_path="loganalyzer.cubes-config.ini"),
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

        olap.Store(entity=ctx.get('cubetl.http.request'),
                   mapper=ctx.get('olap2sql.olapmapper')),

        log.LogPerformance(),

        ]))

    # This node runs several test queries
    ctx.add('loganalyzer.query', flow.Chain(steps=[

        #ctx.get('cubetl.config.print'),

        query.OlapQueryAggregate(fact=ctx.get('cubetl.http.request'),
                                 mapper=ctx.get('olap2sql.olapmapper'),
                                 #drills=['referer.source'],
                                 cuts={'contcountry.id': 16}),

        #query.OlapQueryFacts(fact=ctx.get('cubetl.http.request'),
        #                     mapper=ctx.get('olap2sql.olapmapper'),
        #                     cuts={'contcountry': 16}),

        #query.OlapQueryDimension(fact=ctx.get('cubetl.http.request'),
        #                         mapper=ctx.get('olap2sql.olapmapper'),
        #                         drill=['contcountry.country']),

        ctx.get('cubetl.util.print'),

        ]))


def process_data(ctx, m):

    m['datetime'] = functions.extract_date(m['date_string'], dayfirst=True)

    m['served_bytes'] = 0 if m['served_bytes'] == '-' else int(m['served_bytes'])

    m['client_address'] = m['address']

    # For date dimension
    m['year'] = m['datetime'].year
    m['quarter'] = int((m["datetime"].month - 1) / 3) + 1
    m['month'] = m['datetime'].month
    m['day'] = m['datetime'].day
    m['week'] = int(m["datetime"].strftime('%W'))

    m['http_method'] = m['verb'].split(' ')[0]
    m['protocol'] = m['verb'].split(' ')[-1]

    m['referer_domain'] = ctx.f.text.urlparse(m['referer']).hostname
    m['referer_path'] = ctx.f.text.urlparse(m['referer']).path
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

    m['is_mobile'] = m['ua_is_mobile']
    m['is_tablet'] = m['ua_is_tablet']
    m['is_pc'] = m['ua_is_pc']
    m['is_bot'] = m['ua_is_bot']

    m['status_description'] = "%s %s" % (m['status_code'], m['status_description'])
    m['status_type_label'] = m['status_type']
    m['status_type_code'] = functions.slugu(m['status_type'])

    m['path'] = " ".join(m['verb'].split(' ')[1:-1]).split('?')[0]
    m['path1'] = m['path'].split('/')[1] if len(m['path'].split('/')) > 1 else ""
    m['path2'] = m['path'].split('/')[2] if len(m['path'].split('/')) > 2 else ""
    m['path3'] = m['path'].split('/')[3] if len(m['path'].split('/')) > 3 else ""
    m['path4'] = "/".join(m['path'].split('/')[4:]) if len(m['path'].split('/')) > 4 else ""

    m['mimetype'] = functions.mimetype_guess(m['path']) or "Unknown/Unknown"
    m['mimetype_type'] = m['mimetype'].split('/')[0]
    m['mimetype_subtype'] = m['mimetype'].split('/')[1]

    m['file_name'] = m['path'].split('/')[-1] if (len(m['path'].split('/')) > 0) else ""
    m['file_extension'] = m['file_name'].split('.')[-1] if (len(m['file_name'].split('.')) > 1) else ""
    m['is_download'] = m['file_extension'].lower() in ctx.props['download_extensions_list'] or int(m['served_bytes']) >= int(ctx.props['download_size_bytes'])


'''
def process_sessions(ctx, m):

    # Check that time hasn't gone backwards
    #if 'last_datetime' in ctx.props and m['datetime'] < ctx.props['last_datetime']:
    #    raise Exception("Log date going backwards")
    #ctx.props['last_datetime'] = m['datetime']

    # Calculate sessions
    if 'sessions' not in ctx.props:
        ctx.props['sessions'] = {}
    sessions = ctx.props['sessions']

    session_expiry_seconds = 30 * 60
    session_key = "%s-%s" % (m['address'], m['user_agent_string'])

    new_session = True
    new_visitor = True
    if session_key in sessions:
        new_visitor = False
        last_datetime = sessions[session_key]['last_time']
        if (m['datetime'] - last_datetime).total_seconds() < session_expiry_seconds:
            new_session = False
    else:
        session = {'session_key': session_key,
                   'start_time': m['datetime'],
                   'end_time': None,
                   'last_time':  m['datetime'],
                   'visitor_type': 'new',  # 'returning', 'unknown'
        }

    # response size category (small, medium, large...), and sub category (<1k, <10k, <100k, etc)
    # response size metric in log2

    # visitors / sessions / hits - track each user entry (visitor=ip+os+caching_analysis in session, session=time_span+ip+full-user-agent)
    # first visit, last visit, entry pages/exit pages

    # anomaly/500 (many 5xx any URL any IP in short period)
    # anomaly/404 (many 4xx in short period same IP -visit)

    # tracking javascript ??? (screen size, browser size, support, sessions, campaign / organic...)
    # IP PTR -> SOA authority ( IP owner? )
    # organic/etc by referer?? (requires db)

'''
