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
        api.app.config['TABLE_NAME'] = 'test_table'
        api.app.config['TABLE_NAME2'] = 'test_table2'
        api.app.config['STATUS_TABLE_NAME'] = 'status_table'

        engine = create_engine('sqlite:///', echo=False) # set echo=True for debugging
        metadata = MetaData()

        Table(api.app.config['TABLE_NAME'], metadata,
            Column('id', Integer, primary_key=True),
            Column('site_id', Integer),
            Column('battery_voltage', Integer),
            Column('power', Integer),
            Column('timestamp', String()) # ToDo: do real test for time
            #Column('time', DateTime())
        )

        Table(api.app.config['TABLE_NAME2'], metadata,
            Column('id', Integer, primary_key=True),
            Column('site_id', Integer),
            Column('battery_voltage', Integer),
            Column('power', Integer),
            Column('timestamp', String()) # ToDo: do real test for time
            #Column('time', DateTime())
        )

        Table(api.app.config['STATUS_TABLE_NAME'], metadata,
            Column('id', Integer, primary_key=True),
            Column('rmc', Integer),
            Column('last_contact', DateTime),
            Column('signal_strength', Integer)
        )

        Table('Sesh_RMC_Account', metadata,
            Column('id', Integer, primary_key=True),
            Column('API_KEY', String),
        )


        metadata.create_all(engine)
        api.app.engine = engine


    def setUp(self):
        api.app.config['TESTING'] = True
        api.app.engine.execute("insert into Sesh_RMC_Account (API_KEY) values ('YAYTESTS')")
        self.app = api.app.test_client()
        if({u'name': api.app.config['INFLUXDB_DATABASE']} in api.influx.get_list_database()):
            api.influx.drop_database(api.app.config['INFLUXDB_DATABASE'])
        api.influx.create_database(api.app.config['INFLUXDB_DATABASE'])

    def tearDown(self):
        api.get_table().delete()


    def test_apikey_required(self):
        for route in ['/ping', '/input/insert', '/input/post.json', '/input/bulk', '/status']:
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
        r = self.app.get('/input/insert?apikey=YAYTESTS&battery_voltage=123&pwr=500&timestamp=2015-12-15T07:36:25Z')
        assert 200 == r.status_code
        assert self.get_last_entry()['battery_voltage'] == 123.0
        assert list(api.influx.query('select value from battery_voltage').get_points(measurement='battery_voltage'))[0]['value'], 123.0
        assert list(api.influx.query('select value from power').get_points(measurement='power'))[0]['value'], float('78.9')

        assert self.get_last_entry()['power'] == 500

    def test_post(self):
        api.app.config['MAPPING'] = dict(pwr='power')
        r = self.app.get('/input/post.json?apikey=YAYTESTS&data={"battery_voltage":123, "pwr":400 ,"timestamp": "2015-12-15T07:36:25Z"}')
        assert 200 == r.status_code
        assert self.get_last_entry()['battery_voltage'] == 123.0
        assert self.get_last_entry()['power'] == 400
        assert list(api.influx.query('select value from battery_voltage').get_points(measurement='battery_voltage'))[0]['value'], 123.0
        assert list(api.influx.query('select value from power').get_points(measurement='power'))[0]['value'], float('78.9')


    def test_bulk(self):
        api.app.config['BULK_INDEX_MAPPING'] = {9:{2:'power', 3:'battery_voltage','table':'test_table'},11:{2:'power', 3:'battery_voltage','table':'test_table2'}}
        r = self.app.post('/input/bulk.json?site_id=1&apikey=YAYTESTS&time=112312415',data='[[121234123,9,16,1137],[2341234,11,17,1437]]', headers={'Content-Type': 'application/json'})

        assert 200 == r.status_code
        rows = api.app.engine.execute(api.get_table(api.app.config['TABLE_NAME']).select().order_by(sqlalchemy.desc('id'))).fetchall()
        rows2 = api.app.engine.execute(api.get_table(api.app.config['TABLE_NAME2']).select().order_by(sqlalchemy.desc('id'))).fetchall()
        assert rows[0][2] == 1137
        assert rows[0][3] == 16
        assert rows2[0][2] == 1437
        assert rows2[0][3] == 17

        power = list(api.influx.query('select value from power').get_points(measurement='power'))
        assert power[0]['value'], 1437
        assert power[1]['value'], 1137.42
        assert len(power), 2
        voltage = list(api.influx.query('select value from battery_voltage').get_points(measurement='battery_voltage'))
        assert voltage[0]['value'], 3164
        assert len(voltage), 1
        assert api.date_parser().parse(voltage[0]['time'].decode('utf-8')).year == 1970
        #TODO: test timestamp

    def test_bulk_compressed(self):
        data_compressed = zlib.compress('[[121234123,9,16,1137],[2341234,11,17,1437]]')
        headers = {'Content-Encoding': 'gzip', 'Content-Type': 'application/json'}
        api.app.config['APIKEY'] = 'testing'
        api.app.config['BULK_INDEX_MAPPING'] = {9:{2:'power', 3:'battery_voltage','table':'test_table'},11:{2:'power', 3:'battery_voltage','table':'test_table2'}}
        r = self.app.post('/input/bulk.json?site_id=1&apikey=YAYTESTS&time=112312415',data=data_compressed, headers=headers)

        assert 200 == r.status_code
        rows = api.app.engine.execute(api.get_table(api.app.config['TABLE_NAME']).select().order_by(sqlalchemy.desc('id'))).fetchall()
        rows2 = api.app.engine.execute(api.get_table(api.app.config['TABLE_NAME2']).select().order_by(sqlalchemy.desc('id'))).fetchall()
        assert rows[0][2] == 1137
        assert rows[0][3] == 16
        assert rows2[0][2] == 1437
        assert rows2[0][3] == 17

        power = list(api.influx.query('select value from power').get_points(measurement='power'))
        assert power[0]['value'], 1437
        assert power[1]['value'], 1137.42
        assert len(power), 2
        voltage = list(api.influx.query('select value from battery_voltage').get_points(measurement='battery_voltage'))
        assert voltage[0]['value'], 3164
        assert len(voltage), 1
        assert api.date_parser().parse(voltage[0]['time'].decode('utf-8')).year == 1970
        #TODO: test timestamp



    def test_status(self):
        response = self.app.post('/status?apikey=YAYTESTS', data=json.dumps(dict(signal_strength=42)), headers={'Content-Type': 'application/json'})
        assert response.status_code, 200
        assert self.get_last_entry(api.app.config['STATUS_TABLE_NAME'])['signal_strength'] == 42

    # I have no idea how to test the before_request... so I just do this integration test and hope the rest works
    def test_msgpack_for_status(self):
        response = self.app.post('/status?apikey=YAYTESTS', data=msgpack.packb(dict(signal_strength=42)), headers={'Content-Type': 'application/x-msgpack'})
        assert response.status_code, 200
        assert self.get_last_entry(api.app.config['STATUS_TABLE_NAME'])['signal_strength'] == 42


    def test_ping(self):
        response = self.app.get('/ping?apikey=YAYTESTS')
        assert 'pong' in response.data

    def get_last_entry(self, table=None):
        return api.app.engine.execute(api.get_table(table).select().order_by(sqlalchemy.desc('id')).limit(1)).fetchone()


if __name__ == '__main__':
  unittest.main()

