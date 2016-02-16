import os
import logging
import requests
import flask
import datetime
import json
import sqlalchemy
import rollbar
import rollbar.contrib.flask

from datetime import datetime, date, time, timedelta
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
    MAPPING=dict(),
    BULK_INDEX_MAPPING = dict()
))
app.config.from_envvar('FLASK_SETTINGS', silent=True)

logging.basicConfig(level=getattr(logging, app.config['LOG_LEVEL'].upper(), None), filename='logs/' + app.config['ENVIRONMENT'] + '.log')


def get_table():
    return sqlalchemy.schema.Table(app.config['TABLE_NAME'], sqlalchemy.schema.MetaData(bind=app.engine), autoload=True)

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
    if args.has_key('apikey'): args.pop('apikey') # todo: DRY
    insert_data(map_input_to_columns(args))
    return "OK"

# accepts an EMON post data command
# data is send as json in a get parameter called "data"
@app.route("/input/post.json", methods=['GET'])
def post():
    if not request.args.get('data', None):
        return ""
    args = request.args.copy()
    if args.has_key('apikey'): args.pop('apikey') # todo: DRY

    data = MultiDict(json.loads(request.args.get('data')))
    if request.args.get('time', None):
        data['time'] = request.args.get('time')
    insert_data(map_input_to_columns(data))

    return "OK"

# accepts an EMON post bulk data command
# time must be a unix timestamp whereas for the other endpoints we require a UTC string. the reason for this is here we need to calculate with time whereas in the other endpoints we only hand it over to the database (which does not accept a unix timestamp)
@app.route('/input/bulk.json', methods=['GET'])
def bulk():
    if not request.args.get('data', None):
        return ""
    if not request.args.get('site_id', None):
        return ""

    data = json.loads(request.args.get('data'))
    site_id =  json.loads(request.args.get('site_id'))

    start_time = datetime.fromtimestamp(int(request.args.get('time')))
    for row in data:
        inserts = dict()
        #inserts['time'] = start_time + timedelta(seconds=row[0])
        inserts['time'] = datetime.fromtimestamp(int(row[0]))
        inserts['site_id'] = site_id #adding distinction between
        for index in app.config["BULK_INDEX_MAPPING"]:
            if len(row) >= index+1: # make sure we have an entry for that index. just in case
                inserts[app.config['BULK_INDEX_MAPPING'][index]] = row[index]
        logging.info("inserting %s"%inserts)
        insert_data(inserts)

    return "OK"


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
    table = get_table()
    sql = table.insert().values(data)
    app.engine.execute(sql).close()



if __name__ == "__main__":
    port = int(os.environ.get('FLASK_PORT', 5000))
    # Use 0.0.0.0 to make server visable externally
    host = os.environ.get('FLASK_HOST', '')
    app.engine = SQLAlchemy(app).engine
    app.run(host=host,port=port)

