import logging
from cubetl import olap
import cubetl
from cubetl.sql import sql
from cubetl.olap import sql as olapsql, HierarchyDimension, Key, Measure,\
    Attribute, DimensionAttribute
from cubetl.olap.sql import OlapMapping
from cubetl.core import Component
import re

# Get an instance of a logger
logger = logging.getLogger(__name__)


class OLAPToSQL(Component):

    @staticmethod
    def olap2sql(ctx, connection):
        """
        Automatically generates OLAP to SQL mappings using a normalized approach.
        """

        olapmapper = olap.OlapMapper()
        ctx.add('olap2sql.olapmapper', olapmapper)

        facts = ctx.find(type=cubetl.olap.Fact)
        for fact in facts:
            entity_mapper = OLAPToSQL.generate_star_schema_mapper_entity(ctx, connection, olapmapper, fact)
            ctx.add(entity_mapper.sqltable.urn, entity_mapper.sqltable)
            olapmapper.mappers.append(entity_mapper)

    @staticmethod
    def generate_star_schema_mapper_entity(ctx, connection, olapmapper, entity, prefix="olap2sql"):

        logger.info("Generating SQL schema for: %s", entity)

        # Map dimensions
        columns = []
        mappings = []

        # Key
        sqlcolumn = sql.SQLColumn(name='id', type='AutoIncrement', pk=True, label='Id', nullable=False)
        columns.append(sqlcolumn)
        key = Key("id", type="Integer", label="Id")
        columnmapping = OlapMapping(path=[key], sqlcolumn=sqlcolumn)
        mappings.append(columnmapping)

        # Dimensions
        for dimension_attribute in entity.get_dimensions():

            dimension = dimension_attribute.dimension
            logger.debug("Generating mappings for dimension: %s", dimension)

            # If there are hierarchies, optionally map all dimensions to a single table
            # instead of each dimensionattribute to a new table (see below)
            generate_flat_hierarchy = True
            flatten_entity = False
            if entity.hierarchies and generate_flat_hierarchy:
                flatten_entity = True

            if flatten_entity:
                logger.debug("Flattening dimension '%s' on '%s'.", dimension, entity)
                dimension_mapper = OLAPToSQL.generate_star_schema_mapper_entity(ctx, connection, olapmapper, dimension, prefix="%s.%s" % (prefix, dimension_attribute.name))
                for mapping in dimension_mapper.mappings:
                    # Skip primary keys
                    if mapping.sqlcolumn.pk:
                        continue
                    sqlcolumn = sql.SQLColumn(name=mapping.sqlcolumn.name, type=mapping.sqlcolumn.type, label=mapping.path[-1].label)
                    columns.append(sqlcolumn)
                    columnmapping = OlapMapping(path=[dimension] + mapping.path, sqlcolumn=sqlcolumn)
                    mappings.append(columnmapping)

            else:

                #logger.debug("Joined dimension '%s' on '%s'.", dimension, entity)
                #dimension_mapper = OlapSQLSchema.generate_star_schema_mapper_dimension(ctx, connection, dimension)
                dimension_mapper = OLAPToSQL.generate_star_schema_mapper_entity(ctx, connection, olapmapper, dimension, prefix="%s.%s" % (prefix, dimension_attribute.name))
                ctx.add(dimension_mapper.sqltable.urn, dimension_mapper.sqltable)
                olapmapper.mappers.append(dimension_mapper)

                pk = dimension_mapper.pk(ctx)
                # Generate a SQL column for the detail
                sqlcolumn = sql.SQLColumnFK(name=dimension.name + "_" + pk.sqlcolumn.name, type='Integer', fk_sqlcolumn=pk.sqlcolumn, pk=False, null=False, label=dimension.label)
                columns.append(sqlcolumn)
                columnmapping = OlapMapping(path=[dimension], sqlcolumn=sqlcolumn)
                mappings.append(columnmapping)

        # Map attributes
        for measure in entity.get_measures():
            logger.debug("Generating mappings for measure: %s", measure)
            # Generate a SQL column for the attribute
            sqlcolumn = sql.SQLColumn(name=measure.name, type=measure.type, label=measure.label)
            columns.append(sqlcolumn)
            columnmapping = OlapMapping(path=[measure], sqlcolumn=sqlcolumn)
            mappings.append(columnmapping)

        # Map attributes
        for attribute in entity.get_attributes():
            logger.debug("Generating mappings for attribute: %s", attribute)
            # Generate a SQL column for the detail
            sqlcolumn = sql.SQLColumn(name=attribute.name, type=attribute.type, label=attribute.label)
            columns.append(sqlcolumn)
            columnmapping = OlapMapping(path=[attribute], sqlcolumn=sqlcolumn)
            mappings.append(columnmapping)

        # Create a SQL table for the fact, and a TableMapper
        sqltable = sql.SQLTable(name=entity.name, connection=connection, columns=columns, label=entity.label)
        sqltable.urn = '%s.%s.table' % (prefix, entity.urn or entity.name)
        lookup_cols = [c.name for c in columns]
        # TODO: Use a natural key if provided by the fact or dimension

        entitymapper = olap.sql.TableMapper(entity=entity, sqltable=sqltable, mappings=mappings, lookup_cols=lookup_cols)

        return entitymapper


class SQLToOLAP(Component):
    """
    """

    def __init__(self):
        super().__init__()

    def process(self, ctx, m):
        SQLToOLAP.sql2olap(ctx)
        yield m

    @staticmethod
    def sql2olap(ctx, debug=False, prefix="sql2olap"):
        """
        This method generates a CubETL OLAP schema from an SQL schema defined by CubETL SQL components
        (such a schema can automatically be generated from an existing SQL database using `sql2cubetl`
        function).

        The process can be controlled via a dictionary of options passed via the `options` argument.

        Options:

          * `<object_uri>.type=ignore` ignores the given SQL column.
          * `<object_uri>.type=attribute` forces the SQL column to be used as fact attribute.
          * `<object_uri>.type=dimension` forces the SQL column to be used as dimension.

        Details:

        This method works by walking objects of class SQLTable in the context, and generating an
        cubetl.olap.Fact for each. Tables referenced via foreign keys are included as dimensions.
        """

        # TODO: New generation refactor

        # Create a new Dimension for each found field, unless configuration says they are the same dimension
        # (or can be deduced: ie: same column name + size + same user type (raw dates, 0/1 boolean...).

        # Then, instance an olap sql-to-olap (process tables and columns, generate olap and olap mappings)

        # Implement querying

        # Move these SQL/OLAP method to Cubetl components.

        # Normalize/formalize column/name/id/schema/database usage

        # (optionally, at the end, export to cubes)
        # (should theorically be able to create olap-2-star-schema mappings, then create tables and load)
        # (theorically, we should be able to generate the same mappings from the generated star-schema (would require identifying split dims/hierarchies)

        #exclude_columns = ['key', 'entity_id']
        #force_dimensions = dimensions if dimensions else []


        # Load datetime
        ctx.include(ctx.library_path + "/datetime.py")

        # Mappings for datetime
        datedimension = ctx.get("cubetl.datetime.date")

        facts = {}
        factdimensions = {}

        logger.info("Generating CubETL Olap schema from SQL schema.")

        sqltables = ctx.find(type=cubetl.sql.sql.SQLTable)
        for sqltable in sqltables:

            logger.info("Fact: %s" % sqltable.name)

            # Define fact
            fact_urn = "%s.fact.%s" % (prefix, sqltable.name)
            fact = ctx.add(fact_urn, olap.Fact(name=sqltable.name, label=sqltable.label))
            facts[fact.name] = fact

            # Create an olapmapper for this fact
            # TODO: review whether this is necessary
            olapmapper = olap.OlapMapper()
            #olapmapper.id = "cubesutils.%s.olapmapper" % (tablename)
            #olapmapper.mappers = []
            #olapmapper.include = []

            factmappings = []

            for dbcol in sqltable.columns:

                logger.info("Column: %s" % (dbcol))

                olap_type = _match_config(ctx.props, 'sql2olap.%s.type' % dbcol.urn, None)

                if olap_type == 'ignore':
                    logger.info("SQL2OLAP ignoring SQL column: %s", dbcol)
                    continue

                if dbcol.pk:
                    key_urn = "%s.fact.%s.key.%s" % (prefix, sqltable.name, dbcol.name)
                    key = ctx.add(key_urn, Key(name=dbcol.name, type=dbcol.type, label=dbcol.label))
                    fact.attributes.append(key)

                    factmapping = OlapMapping(path=[key], sqlcolumn=dbcol)
                    factmappings.append(factmapping)

                if isinstance(dbcol, cubetl.sql.sql.SQLColumnFK):
                    #

                    #if len(dbcol.foreign_keys) > 1:
                    #    raise Exception("Multiple foreign keys found for column: %s" % (dbcol.name))

                    related_fact_name = dbcol.fk_sqlcolumn.sqltable.name
                    if related_fact_name == sqltable.name:
                        # Reference to self
                        # TODO: This does not account for circular dependencies across other entities
                        logger.warn("Ignoring foreign key reference to self: %s", dbcol.name)
                        continue
                    related_fact = facts[related_fact_name]

                    # Create dimension attribute
                    dimension_attribute = olap.DimensionAttribute(related_fact, name=dbcol.name, label=dbcol.label)
                    fact.attributes.append(dimension_attribute)

                    # Create a mapping
                    factdimensionmapping = OlapMapping(path=[dimension_attribute], sqlcolumn=dbcol)
                    factmappings.append(factdimensionmapping)

                if not dbcol.pk and not isinstance(dbcol, cubetl.sql.sql.SQLColumnFK) and (olap_type == 'dimension' or (olap_type is None and dbcol.type == "String")):  # or (dbcol.name in force_dimensions)
                    # Embedded dimension (single column, string or integer, treated as a dimension)

                    # Create dimension
                    dim_urn = "%s.fact.%s.dim.%s" % (prefix, sqltable.name, dbcol.name)
                    dimension_attribute = olap.Attribute(name=dbcol.name, type=dbcol.type, label=dbcol.label)
                    dimension = olap.Dimension(name=dbcol.name, label=dbcol.label, attributes=[dimension_attribute])

                    fact.attributes.append(DimensionAttribute(dimension, dimension.name, dimension.label))

                    # This dimension is mapped in the parent table
                    factmapping = OlapMapping(path=[dimension, dimension_attribute], sqlcolumn=dbcol)
                    factmappings.append(factmapping)

                if not dbcol.pk and not isinstance(dbcol, cubetl.sql.sql.SQLColumnFK) and (olap_type == 'attribute'):
                    # Attribute (detail)
                    attribute = Attribute(name=dbcol.name, type=dbcol.type, label=dbcol.label)
                    fact.attributes.append(attribute)

                    factmapping = OlapMapping(path=[attribute], sqlcolumn=dbcol)
                    factmappings.append(factmapping)

                if not dbcol.pk and not isinstance(dbcol, cubetl.sql.sql.SQLColumnFK) and (olap_type == 'measure' or (olap_type is None and dbcol.type in ("Float", "Integer"))):

                    measure = Measure(name=dbcol.name, type=dbcol.type, label=dbcol.label)
                    fact.attributes.append(measure)

                    factmapping = OlapMapping(path=[measure], sqlcolumn=dbcol)
                    factmappings.append(factmapping)

                elif dbcol.type in ("DateTime"):

                    # Date dimension
                    datedimension = ctx.get("cubetl.datetime.date")

                    # Create dimension attribute
                    dimension_attribute = olap.DimensionAttribute(datedimension, name=dbcol.name, label=dbcol.label)
                    fact.attributes.append(dimension_attribute)

                    # TODO: This shall be common
                    #mapper = olap.sql.EmbeddedDimensionMapper(entity=datedimension, sqltable=None)
                    #olapmapper.mappers.append(mapper)

                    mapping = OlapMapping(path=[dimension_attribute, dimension_attribute.dimension.attribute('year')], sqlcolumn=dbcol, function=OlapMapping.FUNCTION_YEAR)
                    factmappings.append(mapping)

                    #mapping = OlapMapping(entity=datedimension, attribute=datedimension.attribute("quarter"), sqlcolumn=dbcol, function=OlapMapping.FUNCTION_QUARTER)
                    #factmappings.append(mapping)

                    mapping = OlapMapping(path=[dimension_attribute, dimension_attribute.dimension.attribute('month')], sqlcolumn=dbcol, function=OlapMapping.FUNCTION_MONTH)
                    factmappings.append(mapping)

                    mapping = OlapMapping(path=[dimension_attribute, dimension_attribute.dimension.attribute('day')], sqlcolumn=dbcol, function=OlapMapping.FUNCTION_DAY)
                    factmappings.append(mapping)

                    mapping = OlapMapping(path=[dimension_attribute, dimension_attribute.dimension.attribute('week')], sqlcolumn=dbcol, function=OlapMapping.FUNCTION_WEEK)
                    factmappings.append(mapping)

                    # Create an alias for this dimension seen from this datetime field point of view
                    # This approach creates a dimension for each different foreign key column name used
                    '''
                    aliasdimension_urn = "%s.dim.datetime.%s.alias.%s" % (prefix, datedimension.name, dbcol.name)
                    aliasdimension = ctx.get(aliasdimension_urn, False)
                    if not aliasdimension:
                        aliasdimension = ctx.add(aliasdimension_urn,
                                                 olap.AliasDimension(dimension=datedimension, name=dbcol.name, label=dbcol.label))
                    fact.dimensions.append(olap.DimensionAttribute(aliasdimension))

                    # Create a mapping
                    aliasdimensionmapping = OlapMapping(entity=aliasdimension, sqlcolumn=dbcol)
                    factmappings.append(aliasdimensionmapping)
                    mapper = olap.sql.AliasDimensionMapper(entity=aliasdimension)
                    mapper.mappings = [
                        # These mappings don't have a sqlcolumn because they are meant to be embedded
                        OlapMapping(entity=ctx.get("cubetl.datetime.year"), sqlcolumn=dbcol, function=OlapMapping.FUNCTION_YEAR),
                        OlapMapping(entity=ctx.get("cubetl.datetime.quarter"), sqlcolumn=dbcol, function=OlapMapping.FUNCTION_QUARTER),
                        OlapMapping(entity=ctx.get("cubetl.datetime.month"), sqlcolumn=dbcol, function=OlapMapping.FUNCTION_MONTH),
                        OlapMapping(entity=ctx.get("cubetl.datetime.day"), sqlcolumn=dbcol, function=OlapMapping.FUNCTION_DAY),
                        OlapMapping(entity=ctx.get("cubetl.datetime.week"), sqlcolumn=dbcol, function=OlapMapping.FUNCTION_WEEK)
                    ]
                    olapmapper.mappers.append(mapper)
                    '''


            '''
            if len(factmappings) == 0:
                factmappings = [ { 'name': 'index', 'pk': True, 'type': 'Integer' } ]
            '''

            mapper = olap.sql.TableMapper(entity=fact, sqltable=sqltable, mappings=factmappings)
            olapmapper.mappers.append(mapper)
            #ctx.register(mapper)  #, uri='%s:fact' % ctx.uri(sqltable)


            # IDs should be defined in mappings, not entity Keys
            #  mappings:
            #  - name: id
            #    pk: True
            #    type: Integer
            #    value: ${ int(m["id"]) }

        #printconfig = PrintConfig()
        #printflow = Chain(fork=True, steps=[printconfig])
        #result = ctx.process(printflow)

        '''
        process = sql.StoreRow(sqltable)
        result = ctx.process(process)

        connection = ctx.find(sql.Connection)[0]
        process = sql.Query(connection, lambda: "SELECT * FROM fin_account_accountmovement", embed=True)
        result = ctx.process(process)
        print(result)
        '''

        '''
        process = olap.OlapQueryAggregate()
        result = ctx.process(process, {'fact': 'fin_account_accountmovement', 'cuts': None, 'drill': None})
        print result
        '''

        olapmapper = olap.OlapMapper()
        olapmappers = ctx.find(type=cubetl.olap.OlapMapper)
        olapmapper.include = [i for i in olapmappers]
        olapmapper_urn = "%s.olapmapper" % (prefix)
        ctx.add(olapmapper_urn, olapmapper)

        return ctx


def _match_config(config, config_key, default=None):
    for ci_key, ci_value in config.items():
        pattern = ci_key.replace('.', '\.').replace('**', '.*').replace('*', '\w+')
        ci_re = re.compile(pattern)
        if ci_re.match(config_key):
            return ci_value
    return default
