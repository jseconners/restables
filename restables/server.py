import os
import json
import yaml
from flask import Flask, abort, request

app = Flask(__name__)

# Load config

config = None
config_file = os.path.join(app.root_path, '..', 'config.yaml')
with open(config_file, 'r') as cfg_file:
    config = yaml.load(cfg_file)

@app.route('/')
def welcome():
    return "<h1>Welcome to restables!</h1>"

@app.route('/<table>', methods=['GET'])
def info(table):
    return table
