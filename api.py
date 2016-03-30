import os
import logging
import requests
import flask
import datetime
import time
import json
import sqlalchemy
import rollbar
import rollbar.contrib.flask

from datetime import datetime, date, timedelta
from dateutil.parser import parser as date_parser
from flask import Flask, abort, request, flash, redirect, render_template, url_for, jsonify, got_request_exception
from flask_sqlalchemy import SQLAlchemy
from werkzeug.datastructures import MultiDict
from influxdb import client as influxClient


app = Flask(__name__)
app.config.update(dict(
    SQLALCHEMY_DATABASE_URI='sqlite:///kraken.db',
    DEBUG=True,
    SECRET_KEY='development',
    ROLLBAR_TOKEN=None,
    LOG_LEVEL='DEBUG',
    ENVIRONMENT='development',
    TABLE_NAME='seshdash_bom_data_point',
    STATUS_TABLE_NAME='RMC_Status',
    APIKEY=None,
    MAPPING=dict(),
    BULK_INDEX_MAPPING = dict(),
    INFLUXDB_HOST='localhost',
    INFLUXDB_PORT=8086,
    INFLUXDB_USER='',
    INFLUXDB_PASSWORD='',
    INFLUXDB_DATABASE='kraken'
))
app.config.from_envvar('FLASK_SETTINGS', silent=True)
logging.basicConfig(level=getattr(logging, app.config['LOG_LEVEL'].upper(), None), filename='logs/' + app.config['ENVIRONMENT'] + '.log')

if app.config['DEBUG']:
    logging.debug("config: " +str(app.config))

if(app.config['INFLUXDB_HOST'] != None):
    influx = influxClient.InfluxDBClient(app.config['INFLUXDB_HOST'], app.config['INFLUXDB_PORT'], app.config['INFLUXDB_USER'], app.config['INFLUXDB_PASSWORD'], app.config['INFLUXDB_DATABASE'])


def get_table(table_name=None):
    if table_name:
        return sqlalchemy.schema.Table(table_name, sqlalchemy.schema.MetaData(bind=app.engine), autoload=True)
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
        logging.debug("Rollbar intiated")

        # send exceptions from `app` to rollbar, using flask's signal system.
        got_request_exception.connect(rollbar.contrib.flask.report_exception, app)

@app.before_request
def validate_api_key():
    apikey = request.args.get('apikey', None)

    if not apikey == app.config['APIKEY']:
        abort(403) # default http status 403 if API key is invalid


@app.route("/ping")
def ping():
    logging.debug("pong")
    return "pong"


# simple endpoint that accepts data as get parameters
@app.route("/input/insert", methods=['GET'])
def insert():
    args = request.args.copy()
    if args.has_key('apikey'): args.pop('apikey')
    insert_data(map_input_to_columns(args))
    return "OK"

# accepts an EMON post data command
# data is send as json in a get parameter called "data"
@app.route("/input/post.json", methods=['GET'])
def post():
    if not request.args.get('data', None):
        return ""
    args = request.args.copy()
    if args.has_key('apikey'): args.pop('apikey')

    data = MultiDict(json.loads(request.args.get('data')))
    if request.args.get('time', None):
        data['timestamp'] = request.args.get('time')
    insert_data(map_input_to_columns(data))

    return "OK"

# accepts an EMON post bulk data command
# time must be a unix timestamp whereas for the other endpoints we require a UTC string. the reason for this is here we need to calculate with time whereas in the other endpoints we only hand it over to the database (which does not accept a unix timestamp)
@app.route('/input/bulk.json', methods=['GET','POST'])
def bulk():
    data = request.form['data']
    data = json.loads(data)

    if not data:
        logging.debug("No data recieved dropping")
        return ""
    if not request.args.get('site_id', None):
        logging.debug("No site_id  recieved dropping")
        return ""

    site_id =  json.loads(request.args.get('site_id'))
    start_time = datetime.fromtimestamp(int(request.form['time']))
    for row in data:
        inserts = dict()
        inserts['timestamp'] = datetime.fromtimestamp(int(row[0]))
        inserts['site_id'] = site_id # Adding distinction between sites
        logging.debug("got post in get "+str(inserts))
        node_id = int(row[1]) # Adding distinction between nodes
        table = None

        if not app.config['BULK_INDEX_MAPPING'].has_key(node_id):
            logging.warning("No table mapping found for in BULK_INDEX_MAPPING nodeid=%s dropping"%node_id)

            logging.warning(str( app.config['BULK_INDEX_MAPPING'].keys()))
            logging.warning(str( app.config['BULK_INDEX_MAPPING']))
            return "NO"

        for index in app.config["BULK_INDEX_MAPPING"][node_id]:
            if isinstance(index,int) and len(row) >= index+1: # Make sure we have an entry for that index. just in case
                inserts[app.config['BULK_INDEX_MAPPING'][node_id][index]] = row[index]

            # Find out which table the data needs to goto
            table = app.config['BULK_INDEX_MAPPING'][node_id]['table']
        logging.debug("inserting %s into table %s"%(inserts,table))

        # We need to send the data to the correct table according to the type of data it is
        insert_data(inserts,table)

    return "OK"

@app.route('/status', methods=['GET', 'POST'])
def status():
    data = json.loads(request.data)
    insert_mysql(data, app.config['STATUS_TABLE_NAME'])

    return "OK"


def map_input_to_columns(args):
    fields = dict()
    for key in args:
        if app.config['MAPPING'].get(key, None):
            fields[app.config['MAPPING'].get(key)] = args[key]
        else:
            fields[key] = args[key]

    return fields

def insert_data(data,table=None):
    logging.debug('new input: %s' %(str(data)))
    insert_mysql(data.copy(),table)
    if(app.config['INFLUXDB_HOST'] != None):
        insert_influx(data.copy())

def insert_mysql(data,table=None):
    if table:
        table = get_table(table_name = table)
    else:
        table = get_table()
    sql = table.insert().values(data)
    logging.debug('writing to mysql: %s' %(str(sql)))
    app.engine.execute(sql).close()

def insert_influx(data):
    points = []
    if data.has_key('timestamp'):
        t = data.pop('timestamp')
        if(type(t) != datetime):
            t = date_parser().parse(t.decode('utf-8'))
    else:
        t = datetime.now()
    timestamp = t.isoformat()

    tags = {}
    if data.has_key('site_id'):
        tags["site_id"] = int(data.pop('site_id'))
    logging.debug("Prepping data for influx %s"%str(data))
    for key in data:
        point = {
            "measurement": key,
            "time": timestamp,
            "tags": tags,
            "fields": {
                "value": float(data[key])
            }
        }
        points.append(point)
    logging.debug('writing influx points: %s' %(str(points)))
    influx.write_points(points)




if __name__ == "__main__":
    port = int(os.environ.get('FLASK_PORT', 5000))
    # Use 0.0.0.0 to make server visable externally
    host = os.environ.get('FLASK_HOST', '')
    app.engine = SQLAlchemy(app).engine
    app.run(host=host,port=port)

