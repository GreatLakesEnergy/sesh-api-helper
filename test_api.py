import unittest

import os
import api
import tempfile
import sqlalchemy

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime


class ApiTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        api.app.config['TESTING'] = True
        api.app.config['TABLE_NAME'] = 'test_table'

        engine = create_engine('sqlite://', echo=False) # set echo=True for debugging
        metadata = MetaData()

        user = Table(api.app.config['TABLE_NAME'], metadata,
            Column('id', Integer, primary_key=True),
            Column('site_id', Integer),
            Column('battery_voltage', Integer),
            Column('power', String(16)),
            Column('time', String()) # ToDo: do real test for time
            #Column('time', DateTime())
        )

        metadata.create_all(engine)
        api.app.engine = engine

    def setUp(self):
        api.app.config['TESTING'] = True
        api.app.config['APIKEY'] = None
        self.app = api.app.test_client()

    def tearDown(self):
        api.get_table().delete()


    def test_apikey_required(self):
        api.app.config['APIKEY'] = 'sunnysunday'
        for route in ['/ping', '/input/insert', '/input/post.json', '/input/bulk']:
            r = self.app.get(route)
            assert 403 == r.status_code

    def test_insert(self):
        api.app.config['MAPPING'] = dict(pwr='power')
        r = self.app.get('/input/insert?battery_voltage=123&pwr=POWER&time=2015-12-15T07:36:25Z')
        assert 200 == r.status_code
        assert self.get_last_entry()['battery_voltage'] == 123.0
        assert self.get_last_entry()['power'] == 'POWER'

    def test_post(self):
        api.app.config['MAPPING'] = dict(pwr='power')
        r = self.app.get('/input/post.json?data={"battery_voltage":123,"pwr": "POWER","time": "2015-12-15T07:36:25Z"}')
        assert 200 == r.status_code
        assert self.get_last_entry()['battery_voltage'] == 123.0
        assert self.get_last_entry()['power'] == 'POWER'

    def test_bulk(self):
        api.app.config['BULK_INDEX_MAPPING'] = {2:'power', 3:'battery_voltage'}
        r = self.app.get('/input/bulk.json?data=[[0,16,1137],[2,17,1437,3164]]&time=1231231421')
        assert 200 == r.status_code
        rows = api.app.engine.execute(api.get_table().select().order_by(sqlalchemy.desc('id'))).fetchall()
        assert rows[0]['power'] == unicode(1437)
        assert rows[0]['site_id'] == 17
        assert rows[0]['battery_voltage'] == 3164
        assert rows[1]['site_id'] == 16
        assert rows[1]['power'] == unicode(1137)
        assert rows[1]['battery_voltage'] == None
        #TODO: test timestamp


    def test_ping(self):
        response = self.app.get('/ping')
        assert 'pong' in response.data

    def get_last_entry(self):
        return api.app.engine.execute(api.get_table().select().order_by(sqlalchemy.desc('id')).limit(1)).fetchone()


if __name__ == '__main__':
  unittest.main()

