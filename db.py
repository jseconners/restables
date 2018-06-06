import re
from sqlalchemy import MetaData, create_engine, desc, asc
from sqlalchemy.sql import select
from sqlalchemy.engine import reflection
from sqlalchemy import Table, func


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

    def list_tables(self):
        """ Get an list of table names for this database """
        tables = []
        for t in self.inspector.get_table_names():
            if self.table_is_viewable(t):
                tables.append(t)
        return tables

    def get_table_count(self, table_name):
        """ Get the row count given a table name """
        table_obj = Table(table_name, self.metadata, autoload=True)
        sel = select([func.count()]).select_from(table_obj)
        records = self.connection.execute(sel)
        return records.first()[0]

    def get_columns(self, table_name):
        """ Get all column objects given a table name """
        cols = []
        for c in self.inspector.get_columns(table_name):
            cols.append(c)
        return cols

    def get_table_data(self, table_name, field_str, opt_str):
        """
        Return a result object given a table name, field string and
        option string
        """
        # set the current table for this query
        self.query_table = Table(table_name, self.metadata, autoload=True)

        # get select, ordering, limit and offset
        sel = select(self.__parse_field_str(field_str))
        ordering, limit, offset = self.__parse_query_opts(opt_str)

        if len(ordering):
            sel = sel.order_by(*ordering)
        if limit is not None:
            sel = sel.limit(limit)
        if offset is not None:
            sel = sel.offset(offset)

        return self.connection.execute(sel)

    def __parse_field_str(self, field_str):
        """ Parse field names from field string """
        if field_str == "*":
            return self.get_columns(self.query_table)

        columns = []
        for fn in [c.strip() for c in field_str.split(",")]:
            if fn not in self.query_table.c:
                raise ColumnError(fn)
            columns.append(self.query_table.c[fn])
        return columns

    def __parse_query_opts(self, opts):
        """
        Parse query options from URL. Either orderby or limit/offset
        expressions. Parsing stops after the first limit expression found.
        """
        ordering = []
        limit, offset = None, None
        if opts is not None:
            for op in [op.strip() for op in opts.split(",")]:
                ob_clause = self.__parse_orderby(op)
                limit, offset = self.__parse_limit(op)

                if ob_clause is not None:
                    ordering.append(ob_clause)
                    continue
                if limit is not None:
                    break
                else:
                    raise OptionError(op)

        return (ordering, limit, offset)

    def __parse_orderby(self, exp):
        """
        Parse order by expression of the form <col>:(a|d) where <col> is the
        table column name and a=ascending, d=descending. Return an array
        of clauses returned by asc() or desc()
        """
        ob_clause = None
        m = re.search("^(\w+):(a|d)", exp)
        if m:
            field_name = m.group(1)
            if field_name not in self.query_table.c:
                raise ColumnError(field_name)
            ob_clause = {'a': asc, 'd': desc}[m.group(2)](field_name)
        return ob_clause

    def __parse_limit(self, exp):
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


class DBConError(Exception):
    """ DBCon base exception class """
    pass


class ColumnError(DBConError):
    """ Error for invalid table column """

    def __init__(self, column_name):
        self.message = "Invalid column specified: {}".format(column_name)


class OptionError(DBConError):
    """ Error for invalid query option """

    def __init__(self, option):
        self.message = "Invalid option specified: {}".format(option)
