
---

!include library/cubetl-datetime.yaml

---

!include library/cubetl-geo.yaml

---

!include library/cubetl-fakedata.yaml

---

!!python/object:cubetl.core.ContextProperties
id: webshop.contextproperties
path_data: examples/webshop-facts.csv

---

!!python/object:cubetl.sql.Connection
id: webshop.sql.connection
url: sqlite:///webshop.sqlite

---

!!python/object:cubetl.sql.Transaction
id: webshop.sql.transaction
connection: !ref webshop.sql.connection
#enabled: False

---

!!python/object:cubetl.olap.AliasDimension
id: webshop.dim.date_sale
name: date_sale
label: Sale Date
dimension: !ref cubetl.datetime.date

---

!!python/object:cubetl.olap.AliasDimension
id: webshop.dim.date_visit
name: date_visit
label: Visit Date
dimension: !ref cubetl.datetime.date

---

!!python/object:cubetl.olap.Dimension
id: webshop.dim.customer
name: customer
label: Customer
attributes:
- name: name
  type: String

---

!!python/object:cubetl.olap.Dimension
id: webshop.dim.product_category
name: product_category
label: Product Category
attributes:
- name: category_label
  type: String

---

!!python/object:cubetl.olap.Dimension
id: webshop.dim.product_product
name: product_product
label: Product
attributes:
- name: product_label
  type: String

---

!!python/object:cubetl.olap.HierarchyDimension
id: webshop.dim.product
name: product
label: Product
hierarchies:
- name: product
  label: Product
  levels: product_category, product_product
levels:
- !ref webshop.dim.product_category
- !ref webshop.dim.product_product

---

!!python/object:cubetl.olap.AliasDimension
id: webshop.dim.country
name: country
label: Country
dimension: !ref cubetl.geo.contcountry

---

!!python/object:cubetl.olap.Dimension
id: webshop.dim.source
name: source
label: Source
attributes:
- name: label
  type: String

---

!!python/object:cubetl.olap.Dimension
id: webshop.dim.browser
name: browser
label: Browser
attributes:
- name: name
  type: String

---

!!python/object:cubetl.olap.Dimension
id: webshop.dim.newsletter
name: newsletter
label: Newsletter
attributes:
- name: newsletter
  type: Boolean

---

!!python/object:cubetl.olap.Fact
id: webshop.fact.sales
name: webshop_sales
label: Webshop / Sales
dimensions:
- !ref webshop.dim.date_sale
- !ref webshop.dim.customer
- !ref webshop.dim.product
- !ref webshop.dim.country
measures:
- name: quantity
  label: Quantity
  type: Integer
- name: price_total
  label: Price Total
  type: Float
- name: delivery_days
  label: Delivery Days
  type: Integer

---

!!python/object:cubetl.olap.Fact
id: webshop.fact.visits
name: webshop_visits
label: Webshop / Visits
dimensions:
- !ref webshop.dim.date_visit
- !ref webshop.dim.country
- !ref webshop.dim.browser
- !ref webshop.dim.newsletter
measures:
- name: pageviews
  label: Page Views
  type: Integer
- name: time
  label: Session Time
  type: Integer

---

!!python/object:cubetl.olap.OlapMapper
id: webshop.olapmapper
#include:
mappers:
- !!python/object:cubetl.olap.sql.CompoundHierarchyDimensionMapper
  entity: !ref webshop.dim.date_sale
  table: dates
  connection: !ref webshop.sql.connection
  eval:
  - name: _cubetl_datetime_date
    value: ${ m['date_sale'] }
  mappings:
  - !ref cubetl.datetime.mappings
- !!python/object:cubetl.olap.sql.DimensionMapper
  entity: !ref webshop.dim.customer
  table: customer
  lookup_cols: name
  connection: !ref webshop.sql.connection
  mappings:
  - name: id
    pk: True
    type: AutoIncrement
  - name: name
    value: ${ m["customer.name"] }
- !!python/object:cubetl.olap.sql.CompoundHierarchyDimensionMapper
  entity: !ref webshop.dim.product
  table: product
  connection: !ref webshop.sql.connection
  lookup_cols: [ 'product_label' ]
  mappings:
  - name: id
    pk: True
    type: AutoIncrement
  - name: category_label
    value: ${ m["product.category"] }
  - name: product_label
    value: ${ m["product.name"] }
- !!python/object:cubetl.olap.sql.CompoundHierarchyDimensionMapper
  entity: !ref webshop.dim.country
  table: country
  connection: !ref webshop.sql.connection
  mappings:
  - name: continent_code
    value: ${ text.slugu(m["country.region"]) }
  - name: continent_name
    value: ${ m["country.region"] }
  - name: country_code
    value: ${ text.slugu(m["country.country"]) }
    pk: True
    type: String
  - name: country_name
    value: ${ m["country.country"] }
- !!python/object:cubetl.olap.sql.FactMapper
  entity: !ref webshop.fact.sales
  table: webshop_sales
  connection: !ref webshop.sql.connection
  lookup_cols: date_sale_id, company_id, product_id
  store_mode: insert
  mappings:
  - name: id
    pk: True
    type: AutoIncrement
- !!python/object:cubetl.olap.sql.CompoundHierarchyDimensionMapper
  entity: !ref webshop.dim.date_visit
  table: dates
  connection: !ref webshop.sql.connection
  eval:
  - name: _cubetl_datetime_date
    value: ${ m['date_visit'] }
  mappings:
  - !ref cubetl.datetime.mappings
- !!python/object:cubetl.olap.sql.DimensionMapper
  entity: !ref webshop.dim.browser
  table: browser
  lookup_cols: name
  connection: !ref webshop.sql.connection
  mappings:
  - name: id
    pk: True
    type: AutoIncrement
  - name: name
    value: ${ m["browser"] }
- !!python/object:cubetl.olap.sql.EmbeddedDimensionMapper
  entity: !ref webshop.dim.newsletter
- !!python/object:cubetl.olap.sql.FactMapper
  entity: !ref webshop.fact.visits
  table: webshop_visits
  connection: !ref webshop.sql.connection
  lookup_cols: visit_id
  mappings:
  - name: visit_id
    pk: True
    type: Integer

---

!!python/object:cubetl.cubes.Cubes10ModelWriter
id: webshop.export-cubes
olapmapper:
  !!python/object:cubetl.olap.OlapMapper
  include:
  - !ref webshop.olapmapper

---

!!python/object:cubetl.flow.Chain
id: webshop.process
steps:

- !ref cubetl.fake.faker
- !ref cubetl.fake.product.load

- !!python/object:cubetl.script.Eval
  eval:
  - name: _cubetl_geo_country_load_filter
    value: "${ lambda m: int(m.population) > 40000000 }"

- !ref cubetl.geo.country.load

- !!python/object:cubetl.flow.Chain
  fork: True
  steps:
  - !!python/object:cubetl.script.Script
    code: |
        dates = []
        date_cur = datetime.datetime(2012, 1, 1, 0, 0, 0)
        date_end = datetime.datetime(2016, 12, 31, 0, 0, 0)
        while date_cur <= date_end:
          dates.append(date_cur)
          date_cur = date_cur + datetime.timedelta(days=1)
        ctx.props["dates"] = dates
  - !ref webshop.sql.transaction
  - !!python/object:cubetl.flow.Multiplier
    name: date_sale
    values: ${ ctx.props["dates"] }
  - !!python/object:cubetl.olap.Store
    entity: !ref webshop.dim.date_sale
    mapper: !ref webshop.olapmapper

- !!python/object:cubetl.flow.Chain
  fork: True
  steps:

  - !ref webshop.sql.transaction

  - !!python/object:cubetl.flow.Multiplier
    name: iter
    values: ${ range(4550) }

  - !ref cubetl.fake.product.random

  - !!python/object:cubetl.script.Eval
    eval:
    - name: product.name
      value: ${ m["product_name"] }
    - name: product.category
      value: ${ m["product_category"] }

    - name: price_unit
      value: ${ text.extract_number(m['product_price']) }
    - name: quantity
      value: ${ abs(int(random.gauss (2, 3))) + 1 }
    - name: quantity
      value: ${ m["quantity"] if m["price_unit"] < 80.0 else int(0.5 + (0.5 * m["quantity"])) }
    - name: price_total
      value: ${ m['price_unit'] * m['quantity'] }

    - name: date_sale
      value: ${ random.choice(ctx.props["dates"]) }
    - name: delivery_days
      value: ${ abs(int(random.gauss(hash(m["product_category"]) % 3 + 3, hash(m["product_category"]) % 5 + 1))) }

    - name: customer.name
      value: ${ fake.limit("company", 145) }

  - !ref cubetl.geo.country.random

  - !ref cubetl.util.print

  - !!python/object:cubetl.script.Eval
    eval:
    - name: country.country
      value: ${ m["country_name"] }
    - name: country.region
      value: ${ m["country_continent_name"] }


  - !!python/object:cubetl.olap.Store
    entity: !ref webshop.fact.sales
    mapper: !ref webshop.olapmapper

  - !ref cubetl.util.logperformance

- !!python/object:cubetl.flow.Chain
  fork: True
  steps:
  - !ref webshop.sql.transaction

  - !!python/object:cubetl.flow.Multiplier
    name: visit
    values: ${ range(1, 9050) }

  - !ref cubetl.geo.country.random

  - !!python/object:cubetl.script.Script
    code: |
        import random

        m['visit_id'] = m['visit']

        m["country.region"] = m.country_continent_name
        m["country.country"] = m.country_name

        m["date_visit"] = random.choice (ctx.props["dates"])
        m["browser"] = random.choice ([u"Lynx", u"Internet Explorer 8.0", u"Internet Explorer 9.0", u"Safari", u"Android", u"Android", u"Firefox", u"Firefox", u"Firefox", u"Chrome", u"Chrome", u"Chrome", u"Chrome"])
        m["newsletter"] = random.choice([1, 0, 0, 0])  # ["Yes", "No", "No", "No"]
        m["source_label"] = random.choice([u"Web search", u"Web search", u"Web search", u"Direct link", u"Twitter", u"YouTube", u"Unknown"])

        m["pageviews"] = abs(int (random.gauss (10, 10))) + 1
        m["time"] = abs(int (random.gauss (90, 50)))

  - !ref cubetl.util.print

  - !!python/object:cubetl.olap.Store
    entity: !ref webshop.fact.visits
    mapper: !ref webshop.olapmapper
  - !ref cubetl.util.logperformance

---
