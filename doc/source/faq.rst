.. SCS developer documentation master file

FAQ
===

OLAP
----

* One of my mapped tables contains an extra column that I haven't mapped.

  Many entity mappers like DimensionMapper don't require manual mapping of all of the entity
  attributes. If your entity contains an attribute that hasn't been manually mapped, it will
  be added automatically. This also creates the corresponding column to be created on the
  respective table. When running CubETL in debug mode, a message is logged when this happens:

  .. code-block:: javascript

    DEBUG - Automatically adding mapping for attribute 'year' of Dimension(cubetl.datetime.year)
    since no mapping was defined for that attribute.



