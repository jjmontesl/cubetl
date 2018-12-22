
from cubetl.olap import Dimension, Key, Attribute, HierarchyDimension, Hierarchy, Fact, Measure
from cubetl.text import RegExp
from cubetl import table


def cubetl_config(ctx):

    ctx.include('${ ctx.library_path }/datetime.py')
    ctx.include('${ ctx.library_path }/geo.py')

    ctx.add('cubetl.http.request.client_address',
            Dimension(name='client_address', label='Address'))

    ctx.add('cubetl.http.request.client_domain',
            Dimension(name='client_domain', label='Domain'))

    ctx.add('cubetl.http.request.username',
            Dimension(name='username', label='Username'))

    ctx.add('cubetl.http.request.method',
            Dimension(name='http_method', label='HTTP Method'))

    ctx.add('cubetl.http.request.path', HierarchyDimension(
        name='request_path',
        label='Path',
        hierarchies=[
            Hierarchy(name='path14', label='Path', levels=[
                Dimension(name='path1', label='Path 1'),
                Dimension(name='path2', label='Path 2'),
                Dimension(name='path3', label='Path 3'),
                Dimension(name='path4', label='Path 4')])
        ]))

    ctx.add('cubetl.http.protocol',
            Dimension(name='protocol', label='Protocol'))


    ctx.add('cubetl.http.response.status.code',
            Dimension(name='status_code', label='Status', attributes=[
                Attribute(name='status_code', type='Integer', label='Status Code'),
                Attribute(name='status_description', type='String', label='Status Description')] ))

    ctx.add('cubetl.http.response.status.type',
            Dimension(name='status_type', label='Status Type', attributes=[
                Attribute(name='type_label', type='String', label='Status Type'),
                Attribute(name='type_code', type='String', label='Status Type Code')] ))

    ctx.add('cubetl.http.response.status', HierarchyDimension(
        name='status',
        label='Status',
        hierarchies=[
            Hierarchy(name='http_status', label='Status', levels=[
                ctx.get('cubetl.http.response.status.type'),
                ctx.get('cubetl.http.response.status.code')
                ])
        ]))

    '''
    ctx.add('cubetl.http.referer.path',
            Dimension(name='referer_path', label='Referer Path'))

    !!python/object:cubetl.olap.HierarchyDimension
    id: cubetl.http.referer
    name: referer
    label: Referer
    hierarchies:
    - name: referer
      label: Referer
      levels: referer_domain, referer_path
    levels:
    - !ref cubetl.http.referer.domain
    - !ref cubetl.http.referer.path
    '''

    ctx.add('cubetl.http.user_agent', HierarchyDimension(
        name='user_agent',
        label='User Agent',
        levels=[Dimension(name='user_agent_family', label='User Agent'),
                Dimension(name='user_agent_version', label='Version')]))

    ctx.add('cubetl.os.operating_system', HierarchyDimension(
        name='operating_system',
        label='Operating System',
        levels=[Dimension(name='operating_system_family', label='OS'),
                Dimension(name='operating_system_version', label='Version')]))

    ctx.add('cubetl.http.mimetype', HierarchyDimension(
        name='mimetype',
        label='MIME Type',
        levels=[Dimension(name='mimetype_type', label='Type'),
                Dimension(name='mimetype_subtype', label='Subtype')]))

    ctx.add('cubetl.os.device',
            Dimension(name='device', label='Device'))

    ctx.add('cubetl.http.request.is_bot',
            Dimension(name='is_bot', label='Is Bot', attributes=[
                Attribute(name='is_bot', type='Boolean', label='is Bot')] ))

    ctx.add('cubetl.http.request.referer_origin',
            Dimension(name='referer_origin', label='Referer Origin'))

    ctx.add('cubetl.http.request', Fact(
        name='http_request',
        label='HTTP Request',
        #natural_key=
        #notes='',
        dimensions=[
            #AttributeDimension('request_date', label="Date", entity=ctx.get('cubetl.datetime.date')),
            ctx.get('cubetl.datetime.date'),
            ctx.get('cubetl.http.protocol'),
            ctx.get('cubetl.http.request.client_address'),
            ctx.get('cubetl.http.request.username'),
            ctx.get('cubetl.http.request.method'),
            ctx.get('cubetl.http.request.path'),
            #ctx.get('cubetl.http.request.referer_domain'), (alias of domain)
            ctx.get('cubetl.http.request.referer_origin'),
            #ctx.get('cubetl.http.request.is_bot'),
            ctx.get('cubetl.geo.contcountry'),
            ctx.get('cubetl.http.response.status'),
            ctx.get('cubetl.http.mimetype'),
            #ctx.get('cubetl.http.referer'),
            ctx.get('cubetl.http.user_agent'),
            ctx.get('cubetl.os.operating_system'),
            ctx.get('cubetl.os.device'),
            ],
        measures=[
            Measure(name='served_bytes', type='Integer', label="Served Bytes"),
            #olap.Measure(name='serve_time', type='Float', label="Serving Time")
            ],
        attributes=[
            Attribute(name='user_agent_string', type='String', label='User Agent String'),
            Attribute(name='verb', type='String', label='Verb'),
            #Attribute(name='referer', type='String', label='Referer')
        ]))

    '''

    - !ref
    #- !ref cubetl.http.request.client_domain
    #- !ref cubetl.http.request.rlogname
    #- !ref cubetl.http.request.username
    '''


    ctx.add('cubetl.http.parse.apache_combined',
            RegExp(regexp=r'([\d\.]+) (-) (-) \[(.*?)\] "(.*?)" (\d+) (\d+) "(.*?)" "(.*?)"',
                   names='address, rlogname, username, date_string, verb, status_code, served_bytes, referer, user_agent_string'))


    ctx.add('cubetl.http.status.table', table.CSVMemoryTable(
        data='''
            status_code,status_description,status_type
            100,Continue,Informational
            101,Switching Protocols,Informational
            200,OK,Successful
            201,Created,Successful
            202,Accepted,Successful
            203,Non-Authoritative Information,Successful
            204,No Content,Successful
            205,Reset Content,Successful
            206,Partial Content,Successful
            300,Multiple Choices,Redirection
            301,Moved Permanently,Redirection
            302,Found,Redirection
            303,See Other,Redirection
            304,Not Modified,Redirection
            305,Use Proxy,Redirection
            307,Temporary Redirect,Redirection
            400,Bad Request,Client Error
            401,Unauthorized,Client Error
            402,Payment Required,Client Error
            403,Forbidden,Client Error
            404,Not Found,Client Error
            405,Method Not Allowed,Client Error
            406,Not Acceptable,Client Error
            407,Proxy Authentication Required,Client Error
            408,Request Timeout,Client Error
            409,Conflict,Client Error
            410,Gone,Client Error
            411,Length Required,Client Error
            412,Precondition Failed,Client Error
            413,Request Entity Too Large,Client Error
            414,Request-URI Too Long,Client Error
            415,Unsupported Media Type,Client Error
            416,Requested Range Not Satisfiable,Client Error
            417,Expectation Failed,Client Error
            500,Internal Server Error,Server Error
            501,Not Implemented,Server Error
            502,Bad Gateway,Server Error
            503,Service Unavailable,Server Error
            504,Gateway Timeout,Server Error
            505,HTTP Version Not Supported,Server Error
        '''))





