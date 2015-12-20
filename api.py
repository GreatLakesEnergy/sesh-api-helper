import os
import logging
import requests
import flask
import datetime
import json
import sqlalchemy
import rollbar
import rollbar.contrib.flask

from datetime import datetime, date, time
from flask import Flask, abort, request, flash, redirect, render_template, url_for, jsonify, got_request_exception
from flask_sqlalchemy import SQLAlchemy
from werkzeug.datastructures import MultiDict


app = Flask(__name__)
app.config.update(dict(
    SQLALCHEMY_DATABASE_URI='sqlite:///kraken.db',
    DEBUG=True,
    SECRET_KEY='development',
    ROLLBAR_TOKEN=None,
    LOG_LEVEL='DEBUG',
    ENVIRONMENT='development',
    TABLE_NAME='seshdash_bom_data_point',
    APIKEY=None,
    MAPPING=dict()
))
app.config.from_envvar('FLASK_SETTINGS', silent=True)

logging.basicConfig(level=getattr(logging, app.config['LOG_LEVEL'].upper(), None), filename='logs/' + app.config['ENVIRONMENT'] + '.log')

db = SQLAlchemy(app)
table = sqlalchemy.schema.Table(app.config['TABLE_NAME'], sqlalchemy.schema.MetaData(bind=db.engine), autoload=True)

@app.before_first_request
def init_rollbar():
    if app.config['ROLLBAR_TOKEN']:
        rollbar.init(
            app.config['ROLLBAR_TOKEN'],
            app.config['ENVIRONMENT'],
            # server root directory, makes tracebacks prettier
            root=os.path.dirname(os.path.realpath(__file__)),
            # flask already sets up logging
            allow_logging_basic_config=False)

        # send exceptions from `app` to rollbar, using flask's signal system.
        got_request_exception.connect(rollbar.contrib.flask.report_exception, app)

@app.before_request
def validate_api_key():
    if not request.args.get('apikey', None) == app.config['APIKEY']:
        abort(403)


@app.route("/ping")
def ping():
    logging.debug("pong")
    return "pong"


# simple endpoint that accepts data as get parameters
@app.route("/input/insert", methods=['GET'])
def insert():
    args = request.args.copy()
    args.pop('apikey') # todo: DRY
    insert_data(map_input_to_columns(args))
    return "OK"

# accepts an EMON post data command
# data is send as json in a get parameter called "data"
@app.route("/input/post.json", methods=['GET'])
def post():
    if not request.args.get('data', None):
        return ""
    args = request.args.copy()
    args.pop('apikey')

    data = MultiDict(json.loads(request.args.get('data')))
    if request.args.get('time', None):
        data['time'] = request.args.get('time')
    insert_data(map_input_to_columns(data))

    return "OK"

# accepts an EMON post bulk data command
# time must be a unix timestamp whereas for the other endpoints we require a UTC string. the reason for this is here we need to calculate with time whereas in the other endpoints we only hand it over to the database (which does not accept a unix timestamp)
@app.route('/input/bulk', methods=['GET'])
def bulk():
    if not request.args.get('data', None):
        return ""
    data = json.loads(request.args.get('data'))

    for index in app.config["BULK_INDEX_MAPPING"]:
        entry = data[index]
        column = app.config["BULK_INDEX_MAPPING"][index]

    for entry in data:
        print entry

    return "bulk"


def map_input_to_columns(args):
    fields = dict()
    for key in args:
        if app.config['MAPPING'].get(key, None):
            fields[app.config['MAPPING'].get(key)] = args[key]
        else:
            fields[key] = args[key]

    return fields

def insert_data(data):
    logging.debug('new input: %s' %(str(data)))
    sql = table.insert().values(data)
    db.engine.execute(sql).close()



if __name__ == "__main__":
    port = int(os.environ.get('FLASK_PORT', 5000))
    # Use 0.0.0.0 to make server visable externally
    host = os.environ.get('FLASK_HOST', '')
    app.run(host=host,port=port)

