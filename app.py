import yaml
import utils
from flask import Flask, jsonify, g, stream_with_context, Response

app = Flask(__name__)


@app.before_request
def load_dbconfigs():
    """ Load database configurations """
    with app.open_instance_resource('dbs.yaml') as f:
        g._dbconfigs = yaml.load(f)


@app.route('/')
def db_list():
    """ Return a list of available configured connections """
    available_connections = g._dbconfigs.keys()
    return jsonify(list(available_connections))


@app.route('/<connection_name>', methods=['GET'])
def db_info(connection_name):
    """ Return a list of tables for this connection/database """
    db = utils.get_db(connection_name)
    info = {
        'tables': db.get_tables()
    }
    return jsonify(info)


@app.route('/<connection_name>/<table>', methods=['GET'])
def table_info(connection_name, table):
    """ Return table info, i.e. columns and row count """
    db = utils.get_db(connection_name, table)
    info = {
        'columns': db.get_column_names(table),
        'rows': db.get_table_count(table)
    }
    return jsonify(info)


@app.route('/<connection_name>/<table>/<fields>', defaults={'opts': None})
@app.route('/<connection_name>/<table>/<fields>/<opts>', methods=['GET'])
def table_data(connection_name, table, fields, opts):
    """
    Return plain/text CSV for table data, with specified field list and
    options list
    """
    db = utils.get_db(connection_name, table)
    results = db.get_table_data(table, fields, opts)

    # create csv generator for results
    streamer = utils.csv_generator(results)

    # stream records as csv
    res = Response(stream_with_context(streamer()))
    res.headers['Content-type'] = 'text/plain'
    return res
