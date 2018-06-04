import re
import csv
import io

from flask import abort, g

from sqlalchemy import MetaData, create_engine, desc, asc
from sqlalchemy.sql import select
from sqlalchemy.engine import reflection
from sqlalchemy import Table, func


def get_db(connection_name, table_name=None):
    """ Get a DBCon object and abort if connection or table does not exist """
    if connection_name not in g._dbconfigs:
        abort(404, "Database not found")

    db = DBCon(g._dbconfigs[connection_name])
    # non-viewable tables reported as not-found
    if table_name is not None:
        if not db.table_is_viewable(table_name):
            abort(404, "Table not found")
    return db


def row_as_csv(row):
    """ Return a csv string from iterable row """
    si = io.StringIO()
    csv.writer(si, quoting=csv.QUOTE_NONNUMERIC).writerow(row)
    return si.getvalue()


def csv_generator(results):
    """
    Create generator for returning csv formatted query results for use
    in streamed response
    """
    def stream():
        yield row_as_csv(results.keys())
        for row in results:
            yield row_as_csv(row)
    return stream


class DBCon:
    """
    Database connection object with query and reflection utilities
    """

    # supported databases
    DIALECTS = {
        # MySQL database via the MySQL Connector/Python driver
        'mysql': 'mysql+mysqlconnector'
    }

    def __init__(self, params):
        self.params = params
        self.connect_db()

        # table access params
        self.show_tables = params.get('show_tables', None)
        self.hide_tables = params.get('hide_tables', [])

    def table_is_viewable(self, table_name):
        """ Determine if table is viewable """
        # hiding overrides showing
        if table_name in self.hide_tables:
            return False
        # all tables shown if not specified
        if self.show_tables is None:
            return True
        else:
            return table_name in self.show_tables

    def connect_db(self):
        """ Create database connection and set associated properties """
        conn_str = "{}://{}:{}@{}:{}/{}"
        engine = create_engine(conn_str.format(
            self.DIALECTS[self.params['dialect']],
            self.params['user'], self.params['password'],
            self.params['host'], self.params['port'],
            self.params['database']
        ))
        self.engine = engine
        self.metadata = MetaData(bind=self.engine)
        self.inspector = reflection.Inspector.from_engine(self.engine)
        self.connection = engine.connect()

    def get_tables(self):
        """ Get an array of table names for this database """
        tables = []
        for t in self.inspector.get_table_names():
            if self.table_is_viewable(t):
                tables.append(t)
        return tables

    def get_table_count(self, table):
        """ Get the row count given a table name """
        table_obj = Table(table, self.metadata, autoload=True)
        sel = select([func.count()]).select_from(table_obj)
        records = self.connection.execute(sel)
        return records.first()[0]

    def get_column_names(self, table):
        """ Get all column names given a table name """
        cols = []
        for c in self.inspector.get_columns(table):
            cols.append(c['name'])
        return cols

    def get_columns(self, table, names):
        """ Get column objects given table name and column names """
        table_obj = Table(table, self.metadata, autoload=True)
        return [getattr(table_obj.c, fn) for fn in names]

    def get_table_data(self, table, fields, opts):
        """
        Return a result object given a table name, field string and
        option string
        """
        sel = self.get_select(table, fields)
        ordering, limit, offset = self.parse_query_opts(opts)

        if len(ordering):
            sel = sel.order_by(*ordering)
        if limit is not None:
            sel = sel.limit(limit)
        if offset is not None:
            sel = sel.offset(offset)

        return self.connection.execute(sel)

    def get_select(self, table, fields):
        """ Return a select query object given table name and field names. """
        if fields == "*":
            names = self.get_column_names(table)
        else:
            names = [c.strip() for c in fields.split(",")]
        return select(self.get_columns(table, names))

    def parse_orderby(self, exp):
        """
        Parse order by expression of the form <col>:(a|d) where <col> is the
        table column name and a=ascending, d=descending. Return an array
        of clauses returned by asc() or desc()
        """
        ob_clause = None
        m = re.search("^(\w+):(a|d)", exp)
        if m:
            ob_clause = {'a': asc, 'd': desc}[m.group(2)](m.group(1))
        return ob_clause

    def parse_limit(self, exp):
        """
        Parse limit expression of the form limit:#[:#] where the first # is
        the limit and the second # is the optional offset. Return a
        (limit, offset) tuple with parsed values or Nones.
        """
        limit, offset = None, None
        m = re.search("^limit:(\d+)(:(\d+))?", exp)
        if m:
            limit = int(m.group(1))
            if m.group(3):
                offset = int(m.group(3))
        return limit, offset

    def parse_query_opts(self, opts):
        """
        Parse query options from URL. Either orderby or limit/offset
        expressions. Parsing stops after the first limit expression found.
        """
        ordering = []
        limit, offset = None, None
        if opts is not None:
            for op in [op.strip() for op in opts.split(",")]:
                ob_clause = self.parse_orderby(op)
                limit, offset = self.parse_limit(op)

                if ob_clause is not None:
                    ordering.append(ob_clause)
                    continue
                if limit is not None:
                    break

        return (ordering, limit, offset)
