#!/usr/bin/env python
# -*- coding: utf8 -*-
# fmt: off
__license__    = "MIT"
__author__     = "Andrew Chung"
__maintainer__ = "Andrew Chung"
__email__      = "Andrew.Chung@dell.com"
__credits__    = []
__copyright__  = """"""
# fmt: on

import inspect
import logging
import os
import sys
import time
import unittest

# Test code is run from the tests directory. Add the parent directory to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "libs"))
import libs.remote_run as rr

LOG = logging.getLogger()


def setup_logger():
    DEFAULT_LOG_FORMAT = "%(created)f [%(levelname)s][%(module)s:%(lineno)d][%(threadName)s] %(message)s"
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT))
    LOG.addHandler(log_handler)
    LOG.setLevel(logging.DEBUG)


class TestRemoteRun(unittest.TestCase):
    def setUp(self):
        if not os.path.isfile(rr.DEFAULT_ISI_FOR_ARRAY_BIN):
            self.skipTest("Could not find isi_for_array binary")
        self.last_msg = None

    def tearDown(self):
        pass

    def remote_callback(self, client, client_id, msg=None):
        print("LOCAL CALLBACK (%s): %s\n" % (client_id, msg))
        self.last_msg = msg

    # @unittest.skip("")
    def test_01_hostname_local_host(self):
        test_data = [
            {
                "type": "onefs",
                "cmd": ["hostname"],
                "endpoint": "1",
            },
        ]
        test_obj = rr.RemoteRun()
        test_obj.connect(test_data)
        countdown = 50
        client_list = True
        while countdown > 0 and client_list:
            client_list = test_obj.get_client_list()
            time.sleep(1)
            countdown -= 1
        self.assertEqual(countdown > 0, True)
        self.assertEqual(len(client_list), 0)
        self.assertEqual(self.last_msg.get("state"), "end")

    # @unittest.skip("")
    def test_02_callback_with_cmd_default(self):
        test_data = [
            {
                "type": "default",
                "cmd": ["uname", "-a"],
            },
            {
                "type": "onefs",
                "endpoint": "1",
            },
        ]
        test_obj = rr.RemoteRun({"callback": self.remote_callback})
        test_obj.connect(test_data)
        countdown = 50
        client_list = True
        while countdown > 0 and client_list:
            client_list = test_obj.get_client_list()
            time.sleep(1)
            countdown -= 1
        self.assertEqual(countdown > 0, True)
        self.assertEqual(len(client_list), 0)
        self.assertEqual(self.last_msg.get("state"), "end")


if __name__ == "__main__":
    setup_logger()
    unittest.main()
