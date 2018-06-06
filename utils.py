import csv
import io
from db import DBCon
from flask import abort, g


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
