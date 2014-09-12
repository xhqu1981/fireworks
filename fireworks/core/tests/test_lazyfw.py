"""
Tests for 'lazy' firework classes.
"""
__author__ = 'Dan Gunter <dkgunter@lbl.gov>'

import logging
import os
import unittest
from fireworks.core import firework as FW

_log = logging.getLogger('fireworks.tests.test_lazyfw')
_hnd = logging.StreamHandler()
_hnd.setFormatter(logging.Formatter(
    "[%(levelname)s] %(asctime)s %(funcName)s:%(lineno)d - %(message)s"))
_log.addHandler(_hnd)
if os.environ.get('TEST_DEBUG', None):
    _log.setLevel(logging.DEBUG)
else:
    _log.setLevel(logging.ERROR)

g_fw = {1: {
    "fw_id": 1,
    "name": "Unnamed FW",
    "launches": [1],
    "archived_launches": [1],
    "state": "COMPLETED",
    "created_on": "2014-09-02T19:54:46.241519",
    "spec": {
        "_tasks": [
            {
                "_fw_name": "ScriptTask",
                "script": "echo 'To be, or not to be,'"
            }
        ]
    }
}}

g_launch = {1: {
    "fworker": {
        "category": "",
        "query": "{}",
        "name": "Automatically generated Worker",
        "env": {

        }
    },
    "time_start": "2014-09-02T19:55:00.694451",
    "trackers": [

    ],
    "ip": "127.0.0.1",
    "fw_id": 5000,
    "time_end": "2014-09-02T19:55:00.730140",
    "runtime_secs": 0.0356,
    "state": "COMPLETED",
    "launch_dir": "/tmp",
    "host": "localhost",
    "launch_id": 1,
    "action": {
        "update_spec": {

        },
        "mod_spec": [

        ],
        "stored_data": {

        },
        "exit": False,
        "detours": [

        ],
        "additions": [

        ],
        "defuse_children": False
    },
    "state_history": [
        {
            "updated_on": "2014-09-02T19:55:00.701578",
            "state": "RUNNING",
            "created_on": "2014-09-02T19:55:00.694451"
        },
        {
            "state": "COMPLETED",
            "created_on": "2014-09-02T19:55:00.730140"
        }
    ]
}}


class MockColl(object):
    def __init__(self, name, message_board):
        self.name = name
        self._mb = message_board

    def find(self, spec, *args, **kwargs):
        d = {'spec': spec, 'args': args}
        d.update(kwargs)
        d['coll'] = self.name
        d['op'] = 'find'
        self._mb.post(self.__class__.__name__, d)
        return self._find(d)

    def _find(self, d):
        return None

    def find_one(self, *args, **kwargs):
        return self.find(*args, **kwargs)[0]


class MockFWColl(MockColl):
    def _find(self, d):
        fw_id = d['spec']['fw_id']
        if len(d['fields']) == 1:  # Launches
            key = d['fields'][0]
            return [{key: [i for i in g_fw.keys() if i == fw_id]}]
        else:  # Fireworks
            rec = g_fw[fw_id].copy()
            return [rec]


class MockLaunchColl(MockColl):
    def _find(self, d):
        launch_id = d['spec']['launch_id']['$in'][0]
        return [g_launch[launch_id].copy()]


class MessageBoard(object):
    def __init__(self):
        self.messages = []
        _log.debug("MB init")

    def post(self, caller, info):
        _log.debug("MB post {}: {}".format(caller, info))
        self.messages.append(info)

    def last(self):
        return None if not self.messages else self.messages[-1]

    def __len__(self):
        return len(self.messages)


class MyFireWork(FW.FireWork):
    @classmethod
    def from_dict(cls, d):
        return MyFireWork([], spec={})

    def hello(self, mb):
        mb.post("MyFireWork", "hello")


class MainTestCase(unittest.TestCase):

    def setUp(self):
        self.mb = MessageBoard()
        self.fw_coll = MockFWColl("fireworks", self.mb)
        self.l_coll = MockLaunchColl("launches", self.mb)

    def test_lazyfw_get(self):
        fw_id = 1
        fw1 = g_fw[fw_id]
        fw = FW.LazyFirework(1, self.fw_coll, self.l_coll)
        _ = fw.fw_id
        self.assertEquals(len(self.mb), 0)
        r = fw.state
        self.assertEquals(len(self.mb), 1)
        self.assertEquals(r, fw1['state'])
        m = self.mb.messages[0]
        self.assertEquals(m['spec']['fw_id'], fw1['fw_id'])

    def test_lazyfw_get_method(self):
        fw = FW.LazyFirework(1, self.fw_coll, self.l_coll)
        _ = fw._rerun
        # no instantiation
        self.assertEquals(len(self.mb), 0)
        # when called, accesses fw + launches & thus 3 queries
        _ = fw._rerun()
        self.assertEquals(len(self.mb), 3)
        # no more queries
        _ = fw._rerun()
        self.assertEquals(len(self.mb), 3)

    def test_lazyfw_set_attr(self):
        fw_id = 1
        fw1 = g_fw[fw_id]
        fw = FW.LazyFirework(1, self.fw_coll, self.l_coll)
        fw.state = 'RIDONCULOUS'
        self.assertEquals(len(self.mb), 1)
        # setting non-existent attr should NOT raise error
        fw.porco_rosso = 1
        self.assertEqual(fw.porco_rosso, 1)

    def test_lazylaunches_get_attr(self):
        fw = FW.LazyFirework(1, self.fw_coll, self.l_coll)
        r1 = fw.launches
        # 2 queries
        self.assertEquals(len(self.mb), 2)
        r2 = fw.archived_launches
        # 1 more query
        self.assertEquals(len(self.mb), 3)

    def test_lazylaunches_set_attr(self):
        fw_id = 1
        fw1 = g_fw[fw_id]
        fw = FW.LazyFirework(1, self.fw_coll, self.l_coll)
        fw.launches = [1, 2, 3]
        # queries to create shallow FW
        self.assertEquals(len(self.mb), 1)
        x = fw.launches
        # no query for the launch data
        self.assertEquals(len(self.mb), 1)
        # this one requires a query now
        x = fw.archived_launches
        self.assertEquals(len(self.mb), 2)


if __name__ == '__main__':
    unittest.main()
