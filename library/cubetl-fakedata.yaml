
---

!!python/object:cubetl.table.MemoryTable
id: cubetl.fake.product.table

---

!!python/object:cubetl.flow.Chain
id: cubetl.fake.product.load
fork: True
steps:
- !!python/object:cubetl.csv.CsvFileReader
  path: ${ ctx.library_path }/data/fakedata-product.csv
- !!python/object:cubetl.table.TableInsert
  table: !ref cubetl.fake.product.table
  mappings:
  - name: product_category
    value: ${ m['product_category'] }
  - name: product_subcategory
    value: ${ m['product_subcategory'] }
  - name: product_name
    value: ${ m['product_name'] }
  - name: product_price
    value: ${ m['product_price'] }
  - name: product_unit
    value: ${ m['product_unit'] }

---

!!python/object:cubetl.table.TableRandomLookup
id: cubetl.fake.product.random
table: !ref cubetl.fake.product.table

---

!!python/object:cubetl.table.TableRandomLookup
id: cubetl.geo.country.random
table: !ref cubetl.geo.country.table

---

!!python/object:cubetl.flow.Chain
id: cubetl.fake.faker
fork: True
steps:
- !!python/object:cubetl.script.Script
  code: |
    from faker import Factory

    def fake_limit(attrib, limit = 50):
      if not "fake_limit" in ctx.props:
        import random
        ctx.props['fake_limit'] = {}
        ctx.props['fake_random'] = random.Random()
      if not attrib in ctx.props['fake_limit']:
        ctx.props['fake_limit'][attrib] = []
        for idx in range (limit):
          val = getattr(ctx._globals['fake'], attrib)()
          ctx.props['fake_limit'][attrib].append(val)

      return ctx.props['fake_random'].choice(ctx.props['fake_limit'][attrib])

    fake = Factory.create()
    ctx._globals["fake"] = fake
    ctx._globals["fake"].limit = fake_limit

