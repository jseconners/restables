import yaml
import utils
from db import DBConError
from flask import Flask, jsonify, g, stream_with_context, Response, abort

app = Flask(__name__)


@app.before_request
def load_dbconfigs():
    """ Load database configurations """
    with app.open_instance_resource('dbs.yaml') as f:
        g._dbconfigs = yaml.load(f, Loader=yaml.FullLoader)


@app.route('/')
def db_list():
    """ Return a list of available configured connections """
    available_connections = g._dbconfigs.keys()
    return jsonify(list(available_connections))


@app.route('/<connection>', methods=['GET'])
def db_info(connection):
    """ Return a list of tables for this connection/database """
    db = utils.get_db(connection)
    info = {
        'tables': db.list_tables()
    }
    return jsonify(info)


@app.route('/<connection>/<table>', methods=['GET'])
def table_info(connection, table):
    """ Return table info, i.e. columns and row count """
    db = utils.get_db(connection, table)
    info = {
        'columns': db.get_column_names(table),
        'rows': db.get_table_count(table)
    }
    return jsonify(info)


@app.route('/<connection>/<table>/<fields>', defaults={'opts': None})
@app.route('/<connection>/<table>/<fields>/<opts>', methods=['GET'])
def table_data(connection, table, fields, opts):
    """
    Return plain/text CSV for table data, with specified field list and
    options list
    """
    db = utils.get_db(connection, table)
    try:
        results = db.get_table_data(table, fields, opts)
    except DBConError as db_error:
        abort(400, db_error.message)

    # create csv generator for results
    streamer = utils.csv_generator(results)

    # stream records as csv
    res = Response(stream_with_context(streamer()))
    res.headers['Content-type'] = 'text/plain'
    return res
