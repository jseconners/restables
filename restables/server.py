import os
import yaml
import utils
from flask import Flask, jsonify, g, stream_with_context, Response

app = Flask(__name__)

CONFIG_FILE = os.path.join(app.root_path, '..', 'config.yaml')


@app.before_request
def load_config():
    """ Load app configuration """
    g._config = None
    with open(CONFIG_FILE, 'r') as cfg_file:
        g._config = yaml.load(cfg_file)


@app.route('/')
def db_list():
    available_connections = g._config['databases'].keys()
    return jsonify(list(available_connections))


@app.route('/<connection_name>', methods=['GET'])
def db_info(connection_name):
    db = utils.DBCon(g._config['databases'][connection_name])
    info = {
        'tables': db.get_tables()
    }
    return jsonify(info)


@app.route('/<connection_name>/<table>', methods=['GET'])
def table_info(connection_name, table):
    db = utils.DBCon(g._config['databases'][connection_name])

    info = {
        'columns': db.get_column_names(table),
        'rows': db.get_table_count(table)
    }
    return jsonify(info)


@app.route('/<connection_name>/<table>/<fields>', defaults={'opts': None})
@app.route('/<connection_name>/<table>/<fields>/<opts>', methods=['GET'])
def table_data(connection_name, table, fields, opts):
    db = utils.DBCon(g._config['databases'][connection_name])

    results = db.get_table_data(table, fields, opts)

    # create csv generator for results
    streamer = utils.csv_generator(results)

    # stream records as csv
    res = Response(stream_with_context(streamer()))
    res.headers['Content-type'] = 'text/plain'
    return res
