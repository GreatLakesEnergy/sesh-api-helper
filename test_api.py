import unittest
import os
import api
import tempfile
import sqlalchemy
import zlib
import json
import msgpack

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime
from influxdb import client as influxClient


class ApiTestCase(unittest.TestCase):


    @classmethod
    def setUpClass(cls):
        api.app.config['TESTING'] = True
        api.app.config['STATUS_TABLE_NAME'] = 'RMC_Status'
        api.app.config['BULK_MYSQL_INSERT'] = True


        engine = create_engine('sqlite:///', echo=False) # set echo=True for debugging
        metadata = MetaData()

        

        Table(api.app.config['STATUS_TABLE_NAME'], metadata,
            Column('id', Integer, primary_key=True),
            Column('rmc_id', Integer),
            Column('ip_address', String),
            Column('time', DateTime),
            Column('signal_strength', Integer)
        )

        Table(api.app.config['ACCOUNTS_TABLE_NAME'], metadata,
            Column('site_id', Integer, primary_key=True),
            Column('API_KEY', String),
        )

        Table(api.app.config['SITES_TABLE_NAME'], metadata,
            Column('id', Integer, primary_key=True),
            Column('site_name', String),
        )

        Table('seshdash_sensor_bmv', metadata, 
            Column('id', Integer, primary_key=True),
            Column('site_id', Integer),
            Column('node_id', Integer),
            Column('index1', String),
            Column('index2', String),
        )


        Table('seshdash_sensor_emontx', metadata, 
            Column('id', Integer, primary_key=True),
            Column('site_id', Integer),
            Column('node_id', Integer),
            Column('index1', String),
            Column('index2', String),
        )
        


        Table('seshdash_sensor_emonth', metadata, 
            Column('id', Integer, primary_key=True),
            Column('site_id', Integer),
            Column('node_id', Integer),
            Column('index1', String),
            Column('index2', String),
        )

        Table('seshdash_sensor_mapping', metadata, 
            Column('id', Integer, primary_key=True),
            Column('site_id', Integer),
            Column('node_id', Integer),
            Column('sensor_type', String)
        )

        metadata.create_all(engine)
        api.app.engine = engine


    def setUp(self):
        api.app.config['TESTING'] = True
        api.app.engine.execute("insert into " + api.app.config['ACCOUNTS_TABLE_NAME'] + " (API_KEY) values ('YAYTESTS')")
        api.app.engine.execute("insert into " + api.app.config['SITES_TABLE_NAME'] + " (site_name) values ('test_site')")

        # Adding sample sensors refferencing to the test site
        api.app.engine.execute("insert into seshdash_sensor_bmv (site_id, node_id, index1, index2) values (1, 9, 'power', 'battery_voltage')" )
        api.app.engine.execute("insert into seshdash_sensor_emontx (site_id, node_id, index1, index2) values (1, 11, 'power', 'battery_voltage')" )
        api.app.engine.execute("insert into seshdash_sensor_emonth (site_id, node_id, index1, index2) values (1, 3, 'humidity', 'battery_voltage')" )

        # Addding sensor mapping for each site
        api.app.engine.execute("insert into seshdash_sensor_mapping (site_id, node_id, sensor_type) values (1, 9, 'sensor_bmv')")
        api.app.engine.execute("insert into seshdash_sensor_mapping (site_id, node_id, sensor_type) values (1, 11, 'sensor_emontx')")
        api.app.engine.execute("insert into seshdash_sensor_mapping (site_id, node_id, sensor_type) values (1, 3, 'sensor_emonth')")

        self.app = api.app.test_client()
        if({u'name': api.app.config['INFLUXDB_DATABASE']} in api.influx.get_list_database()):
            api.influx.drop_database(api.app.config['INFLUXDB_DATABASE'])
        api.influx.create_database(api.app.config['INFLUXDB_DATABASE'])



    def test_apikey_required(self):
        for route in ['/ping', '/input/insert', '/input/post.json', '/input/bulk']:
            r = self.app.get(route)
            assert 403 == r.status_code

    def test_apikey_as_header(self):
        assert 200 == self.app.get('/ping', headers={'X-API-KEY': 'YAYTESTS'}).status_code

    def test_apikey_as_query_param(self):
        assert 200 == self.app.get('/ping?apikey=YAYTESTS').status_code

    #def test_msgpack_content(self):

    #def test_json_content(self):


    def test_insert(self):
        api.app.config['MAPPING'] = dict(pwr='power')
        r = self.app.get('/input/insert?battery_voltage=123&pwr=78.9&timestamp=2015-12-15T07:36:25Z', headers={'X-API-KEY': 'YAYTESTS'})
        assert 200 == r.status_code
        assert list(api.influx.query('select value from battery_voltage').get_points(measurement='battery_voltage'))[0]['value'] == 123.0
        assert list(api.influx.query('select value from power').get_points(measurement='power'))[0]['value'] == 78.9

    def test_post(self):
        api.app.config['MAPPING'] = dict(pwr='power')
        r = self.app.get('/input/post.json?apikey=YAYTESTS&data={"battery_voltage":123, "pwr": 78.9 ,"timestamp": "2015-12-15T07:36:25Z"}' ,headers={'X-API-KEY': 'YAYTESTS'})
        assert 200 == r.status_code


        assert list(api.influx.query('select value from battery_voltage').get_points(measurement='battery_voltage'))[0]['value'] == 123.0
        assert list(api.influx.query('select value from power').get_points(measurement='power'))[0]['value'] == 78.9


    def test_bulk(self):
        r = self.app.post('/input/bulk.json?site_id=1&apikey=YAYTESTS', data='[[121234123,9,16,1137],[2341234,11,17,1437]]', headers={'Content-Type': 'application/json'})

        assert 200 == r.status_code

        # Checking if data is mapped correctly and saved to influxs (power)
        power = list(api.influx.query('select value from power').get_points(measurement='power'))
        assert power[0]['value'] == 17
        assert power[1]['value'] == 16
        assert len(power), 2

        # Checking if the voltage data is mapped and saved correctly
        voltage = list(api.influx.query('select value from battery_voltage').get_points(measurement='battery_voltage'))
        assert voltage[0]['value'] == 1437
        assert voltage[1]['value'] == 1137
        assert len(voltage) == 2

        # Checking if the time is saved correctly to influx
        assert api.date_parser().parse(voltage[0]['time'].decode('utf-8')).year == 1970

    def test_bulk_compressed(self):
        data_compressed = zlib.compress('[[121234123,9,16,1137],[2341234,11,17,1437]]')
        headers = {'Content-Encoding': 'gzip', 'Content-Type': 'application/json'}
        api.app.config['BULK_MYSQL_INSERT'] = True

        r = self.app.post('/input/bulk.json?site_id=1&apikey=YAYTESTS',data=data_compressed, headers=headers)
        assert 200 == r.status_code
        

        # Checking if the data mapped and saved correctly
        power = list(api.influx.query('select value from power').get_points(measurement='power'))
        assert power[0]['value'] == 17
        assert power[1]['value'] == 16
        assert len(power) == 2

        # Checking if the voltage data is mapped and saved correctly
        voltage = list(api.influx.query('select value from battery_voltage').get_points(measurement='battery_voltage'))
        assert voltage[0]['value'] == 1437
        assert voltage[1]['value'] == 1137
        assert len(voltage) == 2

        # Check if the time is saved  correctly
        assert api.date_parser().parse(voltage[0]['time'].decode('utf-8')).year == 1970
        #TODO: test timestamp


    def test_ping(self):
        response = self.app.get('/ping?apikey=YAYTESTS')
        assert 'pong' in response.data



if __name__ == '__main__':
  unittest.main()

