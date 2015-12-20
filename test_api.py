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
        # pending
        assert 1 == 1
        #self.app.get('/input/insert?bv=123&time=2015-12-15T07:36:25Z')
        #assert self.get_last_entry()['battery_voltage'] == 123.0

    def test_ping(self):
        response = self.app.get('/ping')
        assert 'pong' in response.data

    def get_last_entry(self):
        return api.db.engine.execute(api.table.select().order_by(sqlalchemy.desc('id')).limit(1)).fetchone()


if __name__ == '__main__':
  unittest.main()

