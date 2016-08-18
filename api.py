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
import zlib
import msgpack

from datetime import datetime, date, timedelta
from dateutil.parser import parser as date_parser
from flask import Flask, abort, request, flash, redirect, render_template, url_for, jsonify, got_request_exception, g
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
    STATUS_TABLE_NAME='seshdash_rmc_status',
    ACCOUNTS_TABLE_NAME='seshdash_sesh_rmc_account',
    SITES_TABLE_NAME='seshdash_sesh_site',
    APIKEY=None,
    MAPPING=dict(),
    BULK_INDEX_MAPPING = dict(),
    MYSQL_INSERT=True,
    INFLUXDB_HOST='localhost',
    INFLUXDB_PORT=8086,
    INFLUXDB_USER='',
    INFLUXDB_PASSWORD='',
    INFLUXDB_DATABASE='kraken-test'
))

app.config.from_envvar('FLASK_SETTINGS', silent=True)
logging.basicConfig(level=getattr(logging, app.config['LOG_LEVEL'].upper(), None), filename='logs/' + app.config['ENVIRONMENT'] + '.log')

# Required if import the app
if not hasattr(app,'engine'):
    app.engine = SQLAlchemy(app).engine

if app.config['DEBUG']:
    logging.debug("config: " +str(app.config))

if 'APIKEY' in app.config:
    logging.warn('Deprecation warning: you have set an APIKEY in your app config. APIKEYS are now managed in the database')

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
    apikey = request.args.get('apikey', request.headers.get('X-Api-Key', None)) # get the API key from a request param or a header

    account_table = get_table(app.config['ACCOUNTS_TABLE_NAME'])
    site_table = get_table(app.config['SITES_TABLE_NAME'])

    # Get RMC account
    sql = get_table(app.config['ACCOUNTS_TABLE_NAME']).select().where(sqlalchemy.text('API_KEY = :k')).limit(1)
    result = app.engine.execute(sql, k=apikey)

    # Store rmc account info
    g.account = result.fetchone()

    result.close()

    if apikey == None or app.config['APIKEY'] != apikey:
        # No results returned for API key
        if g.account == None:
            abort(403)
        else:
            # Get site name TODO figure out how to do a join
            sql = site_table.select().where(sqlalchemy.text('id = :k')).limit(1)
            result = app.engine.execute(sql, k=g.account['site_id'])
            g.site = result.fetchone()
            result.close()



@app.before_request
def decompress_data():
    if request.data == "":
        return
    # Check if compressesed
    if request.headers.get('Content-Encoding', None) == 'gzip':
        try:
            request.data = zlib.decompress(request.get_data())
            logging.debug("decompressed %s "%request.data)
        except Exception,e:
            logging.warning("Unable to decompress dropping data : %s"%e)
            request.data =  None
            pass

    if request.headers.get('Content-Type', None) == 'application/x-msgpack':
        request.data = msgpack.unpackb(request.data)
    if request.headers.get('Content-Type', None) == 'application/json':
        request.data = json.loads(request.data)

@app.route("/ping")
def ping():
    logging.debug("pong")
    return "pong"


# simple endpoint that accepts data as get parameters
@app.route("/input/insert", methods=['GET'])
def insert():
    args = request.args.copy()
    args['site_id'] = g.account['site_id']
    args['site_name'] = g.site['site_name']
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
    # Attach meta tags
    args['site_id'] = g.account['site_id']
    args['site_name'] = g.site['site_name']
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

    data = request.data
    logging.debug("data:%s"%data)

    if not data:
        logging.debug("No data recieved dropping")
        return ""

    for row in data: 
        inserts = dict()
        inserts['timestamp'] = datetime.fromtimestamp(int(row[0]))
        inserts['site_id'] = g.account['site_id']
        inserts['site_name'] = g.site['site_name']
        node_id = int(row[1]) # Adding distinction between nodes
        bulk_index_mapping = generate_bulk_index_conf(g.site)

        if bulk_index_mapping.has_key(node_id):
            length_of_indices = len(row)
            sensor_data = bulk_index_mapping[node_id]

            # Generating an insert dictionary representing the mapping of the data to the sensor mapping(bulk_index_mappng)
            for index, item_name in sensor_data.items():
                if length_of_indices > index: # Make sure we have an entry for that index. just in case
                    inserts[item_name] = row[index]

            logging.debug("The data to be written to the database is: %s" % inserts)
            insert_data(inserts)
        else:
            logging.warning("The node_id=%s sent has no mapping for this site. SKIPPING" % node_id)

    return "OK"



def get_associated_sensors(site):
    """
    Queryies and returns associated sensors for a given
    site
    """
    associated_sensors = []
    site_sensor_mapping = get_sensor_mapping(site)

    for sensor_map in site_sensor_mapping:
        table = get_table('seshdash_' + sensor_map.sensor_type)
        query = table.select(sqlalchemy.and_(table.c.site_id == site.id, table.c.node_id==sensor_map.node_id ))
        sensor = query.execute().fetchall()
        associated_sensors = associated_sensors  + sensor

    return associated_sensors

def get_sensor_mapping(site):
    """
    Returns the sensor mappings for a 
    given site from the seshdash_sensor_mapping table
    """
    table_sensor_mapping = get_table('seshdash_sensor_mapping')
    selector = table_sensor_mapping.select(table_sensor_mapping.c.site_id == site.id)
    sensor_mapping = selector.execute().fetchall()
    return sensor_mapping



def generate_bulk_index_conf(site):
    """
    Generates the bulk index configuration
    for a given site
    """
    bulk_index_conf = {}
    associated_sensors = get_associated_sensors(site)

    for sensor in associated_sensors:
        bulk_index_conf[sensor.node_id] = generate_sensor_bulk_index_conf(sensor)

    return bulk_index_conf



def generate_sensor_bulk_index_conf(sensor):
    """
    Function to generate the sensor configurations
    for a given sensor table instance
    """
    bulk_index_conf_sensor = {}

    for i in range(1, len(sensor)): # Using this for the maximum rows of a sensor.. Find a way to loop over the sensor rows
        try:
            bulk_index_conf_sensor[i + 1 ] = getattr(sensor, 'index' + str(i) )
        except AttributeError:
            pass

    return bulk_index_conf_sensor





def map_input_to_columns(args):
    fields = dict()
    for key in args:
        if app.config['MAPPING'].get(key, None):
            fields[app.config['MAPPING'].get(key)] = args[key]
        else:
            fields[key] = args[key]

    return fields

def insert_data(data):
    """
    Inserts the data to influx db
    """
    logging.debug('new input: %s' %(str(data)))
    if(app.config['INFLUXDB_HOST'] != None):
        insert_influx(data.copy())



def insert_influx(data):
    points = []
    #Add status flag
    data['status'] = 1
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
    if data.has_key('site_name'):
        tags["site_name"] = data.pop('site_name')

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

