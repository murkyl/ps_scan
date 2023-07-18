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
        print(dir(rr))
        if not os.path.isfile(rr.DEFAULT_ISI_FOR_ARRAY_BIN):
            self.skipTest("Could not find isi_for_array binary")
        pass

    def tearDown(self):
        pass

    # @unittest.skip("")
    def test_01_uname_local_host(self):
        pass


if __name__ == "__main__":
    setup_logger()
    unittest.main()
