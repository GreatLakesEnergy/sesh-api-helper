import unittest

import os
import api
import tempfile
import sqlalchemy


class ApiTestCase(unittest.TestCase):

    def setUp(self):
        api.app.config['TESTING'] = True
        api.app.config['APIKEY'] = None
        self.app = api.app.test_client()

    def tearDown(self):
        api.table.delete()


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
        api.app.config['BULK_INDEX_MAPPING'] = {0:'power', 1:'battery_voltage'}
        r = self.app.get('/input/bulk?data=[[0,16,1137],[2,17,1437,3164]]&time=1231231421')
        assert 200 == r.status_code
        rows = api.db.engine.execute(api.table.select().order_by(sqlalchemy.desc('id'))).fetchall()
        assert row[0]['power'] == 1137
        assert row[0]['battery_voltage'] == None
        assert row[1]['power'] == 1437
        assert row[1]['battery_voltage'] == 3164
        #TODO: test timestamp


    def test_ping(self):
        response = self.app.get('/ping')
        assert 'pong' in response.data

    def get_last_entry(self):
        return api.db.engine.execute(api.table.select().order_by(sqlalchemy.desc('id')).limit(1)).fetchone()


if __name__ == '__main__':
  unittest.main()

