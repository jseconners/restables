import os
import yaml
from flask import Flask, abort, jsonify
import mysql.connector

app = Flask(__name__)

# Load config

config = None
config_file = os.path.join(app.root_path, '..', 'config.yaml')
with open(config_file, 'r') as cfg_file:
    config = yaml.load(cfg_file)


@app.route('/')
def welcome():
    return "<h1>Welcome to restables!</h1>"


@app.route('/<connection>', methods=['GET'])
def db_info(connection):

    if connection in config['databases']:
        cfg = config['databases'][connection]
        cnx = mysql.connector.connect(**cfg)

    cursor = cnx.cursor()
    query = ("SHOW TABLES FROM `{}`".format(cfg['database']))
    cursor.execute(query)

    res = []
    for (table, ) in cursor:
        res.append(table)

    return jsonify(res)


@app.route('/<connection>/<table>', methods=['GET'])
def table_info(connection, table):
    if connection in config['databases']:
        cfg = config['databases'][connection]
        cnx = mysql.connector.connect(**cfg)

    # get all the tables from the database
    cursor = cnx.cursor()
    query = ("SHOW TABLES FROM `{}`".format(cfg['database']))
    cursor.execute(query)

    tables = []
    for (t, ) in cursor:
        tables.append(t)

    # Table name is safe if the passed table name is valid, i.e. in the table
    # list from the db, then use it in the query.
    if (table in tables):
        query = ("SELECT * FROM `{}`.`{}`".format(cfg['database'], table))
        cursor.execute(query)
    else:
        abort(404)

    res = []
    for row in cursor:
        res.append(row)

    return jsonify(res)
