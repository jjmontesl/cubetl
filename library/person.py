# CubETL
# Copyright (c) 2013-2019
# See AUTHORS and LICENSE files for more information.

# This file is part of the CubETL library.

# This file is meant to be included from other CubETL configuration files,
# it provides data types, schema and other objects.


from cubetl.olap import Dimension, Key, Attribute, HierarchyDimension, Hierarchy
from cubetl import table


'''
CubETL Person types library.

This file provides OLAP dimensions for typical personal information,
eg dimensions for "Name", "Gender", "Age"...
'''


def cubetl_config(ctx):

    ctx.add('cubetl.person.gender',
            Dimension(name='gender',
                      label='Gender',
                      attributes=[Attribute(name='gender', type='String', label='Gender'),
                                  Attribute(name='color', type='String', label='Color'),
                                  Attribute(name='icon', type='String', label='Icon')]))

    ctx.add('cubetl.person.gender.table',
            table.CSVMemoryTable(data='''
                key,label,color,icon
                male,Male,#1969ea,male
                female,Female,#d19ad3,female
            '''))

    ctx.add('cubetl.person.age_range',
            Dimension(name='age_range',
                      label='Age Range',
                      #roles=['range'],
                      order_attribute='age_num',
                      attributes=[Attribute(name='age_range', type='String', label='Age Range'),
                                  Attribute(name='age_num', type='Integer', label='Age Number')]))

