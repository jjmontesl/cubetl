

from cubetl.olap import Dimension, Key, Attribute, HierarchyDimension, Hierarchy, Fact, Measure
from cubetl.text import RegExp
from cubetl import table


def cubetl_config(ctx):

    ctx.add('cubetl.geo.continent',
            Dimension(name='continent', label='Continent', attributes=[
                Attribute(name='continent_code', type='String', label='Continent Code'),
                Attribute(name='continent_name', type='String', label='Continent'),
                ]))

    ctx.add('cubetl.geo.country', Dimension(
        name='country',
        label='Country',
        #info={'cv-geo-flag-field': 'country_iso',
        #      'cv-geo-ref-field': 'country_iso',
        #      'cv-geo-map-layer': 'world_countries'},
        attributes=[Attribute(name='country_iso2', type='String', label='Country Code'),
                    Attribute(name='country_name', type='String', label='Country')]))

    ctx.add('cubetl.geo.contcountry', HierarchyDimension(
        name='contcountry',
        label='Country',
        hierarchies=[
            Hierarchy(name='contcountry', label='Country', levels=[
                ctx.get('cubetl.geo.continent'),
                ctx.get('cubetl.geo.country')
                ])
        ]))

'''
---

!!python/object:cubetl.core.Mappings
id: cubetl.geo.mappings
mappings:
- name: continent_code
  value: ${ text.slugu(m["geoip_cont_name"]) }
- name: continent_name
  value: ${ m["geoip_cont_name"] }
- name: country_code
  value: ${ m["geoip_country_code"] }
  pk: True
  type: String
- name: country_name
  value: ${ m["geoip_country_name"] }

---

!!python/object:cubetl.table.MemoryTable
id: cubetl.geo.country.table

---

!!python/object:cubetl.flow.Chain
id: cubetl.geo.country.load
fork: True
steps:
- !!python/object:cubetl.util.log.Log
  message: Loading country information (content by geonames.org, under CC-A 3.0)
- !!python/object:cubetl.csv.CsvFileReader
  path: ${ ctx.library_path }/data/countryInfo.txt
  comment: "#"
  delimiter: "\t"
  ignore_missing: true
  headers: iso, iso3, iso_numeric, fips, country, capital, area_sq_km, population, continent, tld, currency_code, currency_name, phone, postal_code_format, postal_code_regex, languages, geoname_id, neighbours
- !!python/object:cubetl.script.Eval
  eval:
  - name: continent_name
    value: ""
  - name: continent_name
    value: ${ u"Africa" if m['continent'] == "AF" else m['continent_name'] }
  - name: continent_name
    value: ${ u"Asia" if m['continent'] == "AS" else m['continent_name'] }
  - name: continent_name
    value: ${ u"Europe" if m['continent'] == "EU" else m['continent_name'] }
  - name: continent_name
    value: ${ u"North America" if m['continent'] == "NA" else m['continent_name'] }
  - name: continent_name
    value: ${ u"South America" if m['continent'] == "SA" else m['continent_name'] }
  - name: continent_name
    value: ${ u"Oceania" if m['continent'] == "OC" else m['continent_name'] }
  - name: continent_name
    value: ${ u"Antarctica" if m['continent'] == "AN" else m['continent_name'] }
- !!python/object:cubetl.flow.Filter
  condition: ${ True if not '_cubetl_geo_country_load_filter' in m else m['_cubetl_geo_country_load_filter'](m) }
- !ref cubetl.util.print
- !!python/object:cubetl.table.TableInsert
  table: !ref cubetl.geo.country.table
  mappings:
  - name: country_iso
    value: ${ m['iso'] }
  - name: country_iso3
    value: ${ m['iso3'] }
  - name: country_iso_numeric
    value: ${ m['iso_numeric'] }
  - name: country_name
    value: ${ m['country'] }
  - name: country_capital
    value: ${ m['capital'] }
  - name: country_continent_code
    value: ${ m['continent'] }
  - name: country_continent_name
    value: ${ m['continent_name'] }

---

!!python/object:cubetl.table.cache.CachedTableLookup
id: cubetl.geo.country.lookup
table: !ref cubetl.geo.country.table
lookup:
  country_iso: ${ m["country_iso"] }
mappings:
- name: country_iso
- name: country_iso3
- name: country_iso_numeric
- name: country_name
- name: country_capital
- name: country_continent_name
- name: country_continent_code

---

# This is a virtual table backed by an HTTP request to OpenStreetMaps Nominatim service.
# Queries can be done on this table using the "query" message attribute, this process returns
# the first result returned by Nominatim service.
# Note that OSM Nonimatim service has a Usage Policy (http://wiki.openstreetmap.org/wiki/Nominatim_usage_policy).

!!python/object:cubetl.table.ProcessTable
id: cubetl.geo.osm.nominatim
fork: True
process: !!python/object:cubetl.flow.Chain
  id: cubetl.geo.country.load
  fork: False
  steps:
  - !!python/object:cubetl.util.log.Log
    message: Resolving geographic name (data and service by (c) OpenStreetMaps contributors, see "Nominatim usage policy" (http://wiki.openstreetmap.org/wiki/Nominatim_usage_policy))
    once: True
  - !!python/object:cubetl.http.HttpReader
    url: "http://nominatim.openstreetmap.org/search.php?format=json&q=${ urllib.quote_plus(m['query'].encode('utf-8')) }"
  - !!python/object:cubetl.json.JsonReader
    data: ${ m["data"] }
    name: nominatim_data
    iterate: False
  - !!python/object:cubetl.flow.Filter
    condition: ${ bool(m['nominatim_data']) }
  - !!python/object:cubetl.script.Eval
    eval:
    - name: data
      value: null
    - name: nominatim_data
      value: ${ m['nominatim_data'][0] }
    - name: nominatim_display_name
      value: ${ m['nominatim_data']['display_name'] }
    - name: nominatim_lat
      value: ${ float(m['nominatim_data']['lat']) }
    - name: nominatim_lon
      value: ${ float(m['nominatim_data']['lon']) }
    - name: nominatim_bbox_u
      value: ${ float(m['nominatim_data']['boundingbox'][1]) }
    - name: nominatim_bbox_d
      value: ${ float(m['nominatim_data']['boundingbox'][0]) }
    - name: nominatim_bbox_l
      value: ${ float(m['nominatim_data']['boundingbox'][2]) }
    - name: nominatim_bbox_r
      value: ${ float(m['nominatim_data']['boundingbox'][3]) }

  - !!python/object:cubetl.util.log.Log
    level: 10  # FIXME: Log node shall parse log levels
    message: Nominatim query "${ m['query'] }" resolved to "${ m['nominatim_display_name'] }"

---

'''

