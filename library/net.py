# CubETL
# Copyright (c) 2013-2019
# See AUTHORS and LICENSE files for more information.

# This file is part of the CubETL library.

# This file is meant to be included from other CubETL configuration files,
# it provides data types, schema and other objects.

from cubetl.olap import Dimension, Key, Attribute, HierarchyDimension, Hierarchy, Fact, Measure, DimensionAttribute
from cubetl.text import RegExp
from cubetl import table


'''
CubETL HTTP types library.

This file provides OLAP dimensions for typical Internet related datasets,
eg dimensions for "Internet Domain".
'''


def cubetl_config(ctx):

    #ctx.include('${ ctx.library_path }/geo.py')

    ctx.add('cubetl.net.domain.tld',
            Dimension(name='tld', label='TLD'))
            # TODO: Reference GEO/country if available (add "N/A" to Geo/Country)

    ctx.add('cubetl.net.domain.domain',
            Dimension(name='domain', label='Domain'))

    ctx.add('cubetl.net.domain.subdomain',
            Dimension(name='subdomain', label='Subdomain'))

    ctx.add('cubetl.net.domain', HierarchyDimension(
        name='domain3',
        label='Domain',
        #info={"cv-formatter": "'.'.join()"},
        hierarchies=[Hierarchy(name='domain3', label='Domain', levels=['tld', 'domain', 'subdomain'])],
        attributes=[DimensionAttribute(ctx.get('cubetl.net.domain.tld')),
                    DimensionAttribute(ctx.get('cubetl.net.domain.domain')),
                    DimensionAttribute(ctx.get('cubetl.net.domain.subdomain'))]))

