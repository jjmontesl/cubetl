
from cubetl.olap import Dimension, Key, Attribute, HierarchyDimension, Hierarchy,\
    DimensionAttribute


def cubetl_config(ctx):

    ctx.add('cubetl.datetime.year',
            Dimension(name='year',
                      label='Year',
                      role='year',
                      attributes=[Attribute(name='year', type='Integer', label='Year')]))

    ctx.add('cubetl.datetime.quarter',
            Dimension(name='quarter',
                      label='Quarter',
                      role='quarter',
                      attributes=[Attribute(name='quarter', type='Integer', label='Quarter')]))

    ctx.add('cubetl.datetime.month',
            Dimension(name='month',
                      label='Month',
                      role='month',
                      attributes=[Attribute(name='month', type='Integer', label='Month')]))

    ctx.add('cubetl.datetime.week',
            Dimension(name='week',
                      label='Week',
                      role='week',
                      attributes=[Attribute(name='week', type='Integer', label='Week')]))

    ctx.add('cubetl.datetime.day',
            Dimension(name='day',
                      label='Day',
                      role='day',
                      attributes=[Attribute(name='day', type='Integer', label='Day')]))

    ctx.add('cubetl.datetime.date', HierarchyDimension(
        name='date',
        label='Date',
        role='date',
        hierarchies=[Hierarchy(name='daily', label='Daily', levels=['year', 'month', 'day']),
                     Hierarchy(name='weekly', label='Weekly', levels=['year', 'week'])],
        attributes=[DimensionAttribute(dimension=ctx.get('cubetl.datetime.year')),
                    DimensionAttribute(dimension=ctx.get('cubetl.datetime.month')),
                    DimensionAttribute(dimension=ctx.get('cubetl.datetime.day')),
                    DimensionAttribute(dimension=ctx.get('cubetl.datetime.week'))]))

    ctx.add('cubetl.datetime.datemonthly', HierarchyDimension(
        name='datemonthly',
        label='Month',
        role='datemonthly',
        hierarchies=[Hierarchy(name='monthly', label='Monthly', levels=['year', 'quarter', 'month'])],
        attributes=[DimensionAttribute(dimension=ctx.get('cubetl.datetime.year')),
                    DimensionAttribute(dimension=ctx.get('cubetl.datetime.quarter')),
                    DimensionAttribute(dimension=ctx.get('cubetl.datetime.month'))]))

    ctx.add('cubetl.datetime.dim.dateyqmd', HierarchyDimension(
        name='dateyqmd',
        label='Date',
        role='date',
        hierarchies=[Hierarchy(name='daily', label='Daily', levels=['year', 'quarter', 'month', 'day']),
                     Hierarchy(name='weekly', label='Weekly', levels=['year', 'week'])],
        attributes=[DimensionAttribute(dimension=ctx.get('cubetl.datetime.year')),
                    DimensionAttribute(dimension=ctx.get('cubetl.datetime.quarter')),
                    DimensionAttribute(dimension=ctx.get('cubetl.datetime.month')),
                    DimensionAttribute(dimension=ctx.get('cubetl.datetime.day')),
                    DimensionAttribute(dimension=ctx.get('cubetl.datetime.week'))]))

    '''
    !!python/object:cubetl.core.Mappings
    id: cubetl.datetime.mappings
    mappings:
    - name: id
      value: ${ text.slugu(m["_cubetl_datetime_date"].strftime('%Y-%m-%d')) }
      pk: True
      type: String
    - name: year
      value: ${ m["_cubetl_datetime_date"].year }
    - name: quarter
      value: ${ int((m["_cubetl_datetime_date"].month - 1) / 3) + 1 }
    - name: month
      value: ${ m["_cubetl_datetime_date"].month }
    - name: week
      value: ${ int(m["_cubetl_datetime_date"].strftime('%W')) }
    - name: day
      value: ${ m["_cubetl_datetime_date"].day }
    - name: dow
      value: ${ m["_cubetl_datetime_date"].isoweekday() }
    '''
