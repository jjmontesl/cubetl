# CubETL
# Copyright (c) 2013-2019 CubETL Contributors

# This is a CubETL example
# See: https://github.com/jjmontesl/cubetl


import copy
import datetime
import logging
import sys

from cubetl import text, flow, fs, script, olap, pcaxis, table, util
from cubetl.core import Component, Node
from cubetl.elastic import elasticsearch
from cubetl.osm import osm_osmium
from cubetl.sql import sql
from cubetl.table import cache
from cubetl.text import functions
from cubetl.util import log


# Get an instance of a logger
logger = logging.getLogger(__name__)


def cubetl_config(ctx):

    ctx.props['file_path'] = ctx.props.get('file_path', 'spain-latest.osm.pbf')

    ctx.props['osmiumelastic.elastic.mappings'] = {
                "mappings": {
                  "properties": {
                    "description": {
                      "type": "text",
                    },
                    "location": {
                      "type": "geo_point"
                    },
                    "name": {
                      "type": "text",
                    },
                    "tagkeys": {
                      "type": "keyword",
                    },
                    "timestamp": {
                      "type": "date"
                    },
                    "type": {
                      "type": "keyword",
                    },
                    "uid": {
                      "type": "long"
                    },
                    #"user": {
                    #  "type": "keyword",
                    #  #"index": "not_analyzed"
                    #}
                }
            }
        }

    #ctx.include('${ ctx.library_path }/datetime.py')

    ctx.add('osmiumelastic.connection',
            elasticsearch.ElasticsearchConnection(url='http://localhost:9200'))


    ctx.add('osmiumelastic.process', flow.Chain(steps=[

        elasticsearch.IndexCreate(es=ctx.get("osmiumelastic.connection"),
                                  index="osm-index",
                                  mappings=ctx.props["osmiumelastic.elastic.mappings"]),

        osm_osmium.OsmiumNode(filename="spain-latest.osm.pbf"),

        util.PrettyPrint(depth=4),

        log.LogPerformance(),

        elasticsearch.Index(es=ctx.get("osmiumelastic.connection"),
                            index="osm-index",
                            doc_type="osm",
                            data_id=lambda m: m['id']),

    ]))


    ctx.add('osmiumelastic.search', flow.Chain(steps=[
        elasticsearch.Search(es=ctx.get("osmiumelastic.connection"),
                             index="osm-index",
                             query="${ m.get('q', None) }"),
        util.PrettyPrint(depth=4),
        log.LogPerformance(),
    ]))



"""
# Possible improvements:
# - add weights to items and roads to be used in scores and search results
# - implement scoring functions and options (somehow configurable)
# - alter name for search (remove tildes, etc... using unicode-aware reducer lib (? can't remembr the name))
# - add city / country / count.... administrative levels + handle containment
# - add interesting tags for searches (and remap metadata names to reduce index column usage)
# - add other interesting tags for search result (not for search)
# - add translated names for road and items (calle / plaza / avenida... estatua...)
# - support i18n in queries and results + support multiple/cross language queries
# - provide nominatim compatible query API
# - add result icons (ie. from OSM carto style + provide a nominatim-compatible set)
# - extras: zip codes / house numbers... / other data (?)
# - consider splitting all ways in all intersection (like DDD-OSM-3D) for better reverse geocoding
# - support incremental updates from OSM (study how - affects containmnent (?))
"""


