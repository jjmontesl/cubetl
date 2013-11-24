import logging
from lxml import etree
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import Table, MetaData, Column
from sqlalchemy.types import Integer, String, Float, Boolean, Unicode
import sys
from cubetl.core import Node, Component
from sqlalchemy.sql.expression import and_
from cubetl.functions.text import parsebool

# Get an instance of a logger
logger = logging.getLogger(__name__)

class Connection(Component):
    
    def __init__(self):
        self.url = None
        self._engine = None
        self._connection = None
        
    def initialize(self):
        if (self._engine == None):
            self._engine = create_engine(self.url)
            self._connection = self._engine.connect()
    
    def connection(self):
        self.initialize()
        return self._connection
    
    def engine(self):
        self.initialize()
        return self._engine

class SQLTable(Component):
    
    def __init__(self):

        self._pk = False

        self.name = None
        self.connection = None
        self.columns = [ ]
        
        self.create = True
        
        self.sa_table = None
        self.sa_metadata = None
        
        self._lookups = 0
        self._inserts = 0        
    
    def _get_sa_type(self, column):
        
        if (column["type"] == "Integer"):
            return Integer
        elif (column["type"] == "String"):
            if (not "length" in column): column["length"] = 128
            return Unicode(length = column["length"])
        elif (column["type"] == "Float"):
            return Float    
        elif (column["type"] == "Boolean"):
            return Boolean
        elif (column["type"] == "AutoIncrement"):
            return Integer
        else:
            raise Exception("Invalid data type: %s" % column["type"])
    
    def signal(self, ctx, s):
        
        super(SQLTable, self).signal(ctx, s)    
        self.connection.signal(ctx, s) 
        
        if (s == "initialize"):
            self.initialize(ctx)
        if (s == "finalize"):
            logger.info("sqltable %-18s lookups/inserts: %6d/%-6d " % 
                        (self.name, self._lookups, self._inserts))
    
    def initialize(self, ctx):
        
        if (self.sa_table == None):
        
            logger.debug("Loading table %s on %r" % (self.name, self))
            
            self.sa_metadata = MetaData()
            self.sa_table = Table(self.name, self.sa_metadata)

            # Drop?

            # Columns
            for column in self.columns:
                column["pk"] = False if (not "pk" in column) else parsebool(column["pk"])
                if (not "type" in column): column["type"] = "String"
                #if (not "value" in column): column["value"] = None
                logger.debug("Adding column %s" % column)
                self.sa_table.append_column( Column(column["name"], 
                                                    self._get_sa_type(column), 
                                                    primary_key = column["pk"], 
                                                    autoincrement = (True if column["type"] == "AutoIncrement" else False) ))
            
            # Check schema
            
            # Create if doesn't exist
            if (not self.connection.engine().has_table(self.name)):
                logger.info("Creating table %s" % self.name) 
                self.sa_table.create(self.connection.connection())
                
            # Extend?
            
            # Delete columns?
                
            
    def pk(self, ctx):
        """
        Returns the primary key column definitToClauion, or None if none defined.
        """
        
        if (self._pk == False):
            pk_cols = []
            for col in self.columns:
                if ("pk" in col):
                    if parsebool(col["pk"]):
                        pk_cols.append(col)
                        
            if (len(pk_cols) > 1):
                raise Exception("Table %s has multiple primary keys: %s" % (self.name, pk_cols))
            elif (len(pk_cols) == 1):
                self._pk = pk_cols[0]
            else:
                self._pk = None
                
        return self._pk
            
    def _attribsToClause(self, attribs):
        clauses = []
        for k, v in attribs.items():
            if isinstance(v, (list, tuple)):
                clauses.append(self.sa_table.c[k].in_(v))
            else:
                clauses.append(self.sa_table.c[k] == v)
        
        return and_(*clauses)            
            
    def _rowtodict(self, row):
        
        d = {}
        for column in self.columns:
            d[column["name"]] = getattr(row, column["name"])
    
        return d
            
    def find(self, ctx, attribs):
        
        self._lookups = self._lookups + 1
        
        query = self.sa_table.select(self._attribsToClause(attribs))
        rows = self.connection.connection().execute(query)

        for r in rows:
            # Ensure we return dicts, not RowProxys from SqlAlchemy
            yield self._rowtodict(r)
             
        
    def findone(self, ctx, attribs):
        
        if (len(attribs.keys()) == 0):
            raise Exception("Searching on table with no criteria (empty attribute set)")
        
        rows = self.find(ctx, attribs)
        rows = list(rows)
        if (len(rows) > 1):
            raise Exception("Found 0 or more than one row when searching for just one in table %s: %s" % (self.name, attribs))
        elif (len(rows) == 1):
            row = rows[0]   
        else:
            row = None
        
        logger.debug("Findone result on %s: %s = %s" % (self.name, attribs, row))
        return row
    
    def store(self, ctx, data, keys = []):
        self.initialize(ctx)
        
        # Use primary key if available
        pk = self.pk(ctx)
        if ((pk != None) and (pk["name"] in data)):
            keys = [pk["name"]]
        
        # Use keys
        qfilter = {}
        for key in keys:
            try:
                qfilter[key] = data[key]
            except KeyError, e:
                raise Exception("Could not find attibute '%s' in data when storing row data: %s" % (key, data))
        
        row = self.findone(ctx, qfilter)
                
        
        if (row):
            # TODO
            return row
        
        rid =  self.insert(ctx, data)
                        
        return rid
        
    
    def insert(self, ctx, data):
        
        self.initialize(ctx)
        
        row = {}
        
        for column in self.columns:
            if (column["type"] != "AutoIncrement"):
                try:
                    row[column["name"]] = data[column["name"]]
                except KeyError, e:
                    raise Exception("Missing attribute for column %s in table '%s' while inserting row: %s" % (e, self.name, data))
                
                # Checks
                if (ctx.debug2):
                    if ((column["type"] == "String") and (not isinstance(row[column["name"]], Unicode))):
                        logger.warn("Unicode column %s received non-unicode string %s " % (column["name"], row[column["name"]]))
                
        
        logger.debug ("Inserting row %s" % row)
        res = self.connection.connection().execute(self.sa_table.insert(row))

        pk = self.pk(ctx)
        row[pk["name"]] = res.inserted_primary_key[0]
        
        self._inserts = self._inserts +1
        
        if (pk != None):
            return row
        else:
            return None

class Transaction(Node):
    
    def __init__(self):
        
        super(Transaction, self).__init__()
        
        self.connection = None
        
        self._transaction = None
        
        self.enabled = True

    def signal(self, ctx, s):
        
        super(Transaction, self).signal(ctx, s)
        
        # If finalizing, close transaction
        if (s == "initialize"):
            self.enabled = parsebool(self.enabled)
        if (s == "finalize"):
            if (self.enabled):
                logger.info("Commiting database transaction")
                self._transaction.commit()
                self._transaction = None
        
    def process(self, ctx, m):
        
        # Store
        if (self._transaction != None):
            raise Exception("Trying to start transaction while one already exists is not supported")
        
        if (self.enabled):
            logger.info("Starting database transaction")
            self._transaction = self.connection.connection().begin()
        else:
            logger.debug("Not starting database transaction (Transaction node is disabled)")
        
        yield m        
        
        
class StoreRow(Node):
    
    def __init__(self):
        
        super(StoreRow, self).__init__()
        
        self.table = None
    
    def process(self, ctx, m):
        
        # Store
        self.table.store(ctx, m)
        
        yield m        
