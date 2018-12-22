import logging
from cubetl import olap
import cubetl
from cubetl.sql import sql
from cubetl.olap import sql as olapsql, HierarchyDimension, Key
from cubetl.olap.sql import OlapMapping

# Get an instance of a logger
logger = logging.getLogger(__name__)


class OlapSQLSchema():

    @staticmethod
    def generate_star_schema_mapper(ctx, connection):
        """
        Automatically generates OLAP to SQL mappings using a normalized approach.
        """

        olapmapper = olap.OlapMapper()
        ctx.add('olap2sql.olapmapper', olapmapper)

        facts = ctx.find(type=cubetl.olap.Fact)
        for fact in facts:
            OlapSQLSchema.generate_star_schema_mapper_fact(ctx, connection, olapmapper, fact)

    @staticmethod
    def generate_star_schema_mapper_dimension(ctx, connection, olapmapper, dimension):

        logger.info("Generating star schema for dimension: %s", dimension)

        columns = []
        mappings = []

        # Key
        # TODO: Use naturalkey if provided? how?
        sqlcolumn = sql.SQLColumn(name='id', type='AutoIncrement', pk=True, label='Id')
        columns.append(sqlcolumn)
        key = Key(dimension, "id", type="Integer", label="Id")
        columnmapping = OlapMapping(key, sqlcolumn)
        mappings.append(columnmapping)

        if isinstance(dimension, HierarchyDimension):
            for level in dimension.levels:
                for attribute in level.attributes:
                    logger.debug("Generating mappings for level: %s", attribute)
                    # Generate a SQL column for the attribute
                    sqlcolumn = sql.SQLColumn(name=attribute.name, type=attribute.type, label=attribute.label)
                    columns.append(sqlcolumn)
                    columnmapping = OlapMapping(attribute, sqlcolumn)
                    mappings.append(columnmapping)

        # Map attributes
        for attribute in dimension.attributes:
            logger.debug("Generating mappings for attribute: %s", attribute)
            # Generate a SQL column for the attribute
            sqlcolumn = sql.SQLColumn(name=attribute.name, type=attribute.type, label=attribute.label)
            columns.append(sqlcolumn)
            columnmapping = OlapMapping(attribute, sqlcolumn)
            mappings.append(columnmapping)

        # Create a SQL table for the dimension
        sqltable = sql.SQLTable(name=dimension.name, connection=connection, columns=columns, label=dimension.label)
        sqltable_urn = '%s.%s.table' % ("olap2sql", dimension.urn)
        ctx.add(sqltable_urn, sqltable)
        dimensionmapper = olapsql.DimensionMapper(entity=dimension, sqltable=sqltable, mappings=mappings)
        dimensionmapper.lookup_cols = [c.name for c in columns]
        olapmapper.mappers.append(dimensionmapper)

        return dimensionmapper

    @staticmethod
    def generate_star_schema_mapper_fact(ctx, connection, olapmapper, fact):

        logger.info("Generating star schema for fact: %s", fact)

        # Map dimensions
        columns = []
        mappings = []

        # Key
        sqlcolumn = sql.SQLColumn(name='id', type='AutoIncrement', pk=True, label='Id')
        columns.append(sqlcolumn)
        key = Key(fact, "id", type="Integer", label="Id")
        columnmapping = OlapMapping(key, sqlcolumn)
        mappings.append(columnmapping)

        # Dimensions
        for dimension in fact.dimensions:
            dimension_mapper = OlapSQLSchema.generate_star_schema_mapper_dimension(ctx, connection, olapmapper, dimension)
            pk = dimension_mapper.pk(ctx)
            # Generate a SQL column for the detail
            sqlcolumn = sql.SQLColumnFK(name=dimension.name + "_" + pk.sqlcolumn.name, type='Integer', fk_sqlcolumn=pk.sqlcolumn, pk=False, null=False, label=dimension.label)
            columns.append(sqlcolumn)
            columnmapping = OlapMapping(dimension, sqlcolumn)
            mappings.append(columnmapping)

        # Map attributes
        for measure in fact.measures:
            logger.debug("Generating mappings for measure: %s", measure)
            # Generate a SQL column for the attribute
            sqlcolumn = sql.SQLColumn(name=measure.name, type=measure.type, label=measure.label)
            columns.append(sqlcolumn)
            columnmapping = OlapMapping(measure, sqlcolumn)
            mappings.append(columnmapping)

        # Map attributes
        for attribute in fact.attributes:
            logger.debug("Generating mappings for attribute: %s", attribute)
            # Generate a SQL column for the detail
            sqlcolumn = sql.SQLColumn(name=attribute.name, type=attribute.type, label=attribute.label)
            columns.append(sqlcolumn)
            columnmapping = OlapMapping(attribute, sqlcolumn)
            mappings.append(columnmapping)

        # Create a SQL table for the fact, and a TableMapper
        sqltable = sql.SQLTable(name=fact.name, connection=connection, columns=columns, label=fact.label)
        sqltable_urn = '%s.%s.table' % ("olap2sql", fact.urn)
        ctx.add(sqltable_urn, sqltable)
        factmapper = olap.sql.FactMapper(entity=fact, sqltable=sqltable, mappings=mappings)
        # TODO: Use a natural key if provided by the fact or dimension
        factmapper.lookup_cols = [c.name for c in columns]
        olapmapper.mappers.append(factmapper)

        '''
        for dbcol in sqltable.columns:

            logger.info("Column: %s" % (dbcol))

            olap_type = _match_config(options, 'sql2olap.%s.type' % dbcol.urn, None)

            if olap_type == 'ignore':
                logger.info("SQL2OLAP ignoring SQL column (per settings): %s", dbcol)
                continue

            if dbcol.pk:
                key_urn = "%s.fact.%s.key.%s" % (prefix, sqltable.name, dbcol.name)
                key = ctx.add(key_urn, Key(entity=fact, name=dbcol.name, type=coltype(dbcol), label=dbcol.label))
                fact.keys.append(key)

                factmapping = OlapMapping(entity=key, sqlcolumn=dbcol)
                factmappings.append(factmapping)

        '''
