import os
import re
import yaml

from flask import Flask, abort, jsonify, g, stream_with_context, Response

from sqlalchemy import create_engine, MetaData, Table, desc, asc
from sqlalchemy.engine import reflection
from sqlalchemy.sql import select

app = Flask(__name__)

CONFIG_FILE = os.path.join(app.root_path, '..', 'config.yaml')

DIALECTS = {
    'mysql': 'mysql+mysqlconnector'
}


@app.before_request
def load_config():
    ''' Load app configuration '''
    g._config = None
    with open(CONFIG_FILE, 'r') as cfg_file:
        g._config = yaml.load(cfg_file)


@app.teardown_appcontext
def close_db(exception):
    db = g.get('_db', None)
    if db is not None:
        db['connection'].close()


def connect_db(connection_name):
    '''
    Set up database connection and other parameters for access in the
    application context
    '''
    db = g.get('_db', None)
    if db is None and connection_name in g._config['databases']:
        db = {
            'label': connection_name,
            'params': g._config['databases'][connection_name]
        }
        db['engine'] = get_dbengine(db['params'])
        db['metadata'] = MetaData(bind=db['engine'])
        db['inspector'] = reflection.Inspector.from_engine(db['engine'])
        db['connection'] = db['engine'].connect()
    return db


def get_dbengine(params):
    engine = None
    if params['dialect'] in DIALECTS:
        conn_str = "{}://{}:{}@{}:{}/{}"
        engine = create_engine(conn_str.format(
            DIALECTS[params['dialect']],
            params['user'], params['password'],
            params['host'], params['port'],
            params['database']
        ))
    return engine


def get_tables(db):
    return db['inspector'].get_table_names()


def get_select_fields(table, field_names):
    try:
        fields = [getattr(table.c, fn) for fn in field_names]
    except AttributeError as e:
        abort(400, "Invalid table field specified. Does not exist.")
    return fields


def parse_ordering(order_str):
    ordering = []
    if order_str is not None:
        for op in [op.strip() for op in order_str.split(",")]:
            m = re.search("^(\w+):(a|d)", op)
            if not m:
                abort(400, "Invalid field ordering specified")
            ordering.append({'a': asc, 'd': desc}[m.group(2)](m.group(1)))
    return ordering


def csv_record_generator(results, header):
    def stream():
        yield "{}\n".format(",".join(header))
        for row in results:
            yield "{}\n".format(",".join([str(x) for x in row]))
    return stream


@app.route('/')
def db_list():
    available_connections = g._config['databases'].keys()
    return jsonify(list(available_connections))


@app.route('/<connection_name>', methods=['GET'])
def db_info(connection_name):
    db = connect_db(connection_name)
    if db is None:
        # connection not found
        abort(404)

    return jsonify(get_tables(db))


@app.route('/<connection_name>/<table>', methods=['GET'])
def table_info(connection_name, table):

    db = connect_db(connection_name)
    if db is None:
        # connection not found
        abort(404)

    cols = []
    for c in db['inspector'].get_columns(table):
        cols.append(c['name'])

    return jsonify(cols)


@app.route('/<connection_name>/<table>/<field_str>', defaults={'order_str': None})
@app.route('/<connection_name>/<table>/<field_str>/<order_str>', methods=['GET'])
def table_data(connection_name, table, field_str, order_str):

    db = connect_db(connection_name)
    if db is None:
        # connection not found
        abort(404)

    data_table = Table(table, db['metadata'], autoload=True)
    if field_str == "*":
        field_names = [c['name'] for c in db['inspector'].get_columns(table)]
    else:
        field_names = [c.strip() for c in field_str.split(",")]

    # get table field objects and abort if invalid names passed by user
    fields = get_select_fields(data_table, field_names)

    # get order by expression objects or abort if invalid expressions used
    ordering = parse_ordering(order_str)

    # create select clause
    s = select(fields)

    # add order by clause(s) if available
    if len(ordering):
        s = s.order_by(*ordering)

    # execute query
    records = db['connection'].execute(s)

    # create csv record generator
    streamer = csv_record_generator(records, field_names)

    # stream records as csv
    res = Response(stream_with_context(streamer()))
    res.headers['Content-type'] = 'text/plain'
    return res
