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
import select
import socket
import sys
import time
import threading
import tracemalloc
import unittest

# Test code is run from the tests directory. Add the parent directory to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "libs"))
from libs.hydra_socket import *
from libs.hydra_const import *
from libs.hydra import *

tracemalloc.start()
LOG = logging.getLogger()
TEST_MSG_1 = {"key": "value"}
TEST_MSG_2 = {"client": "data", "field": 2}


def setup_logger():
    DEFAULT_LOG_FORMAT = "%(created)f [%(levelname)s][%(module)s:%(lineno)d][%(threadName)s] %(message)s"
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT))
    LOG.addHandler(log_handler)
    LOG.setLevel(logging.DEBUG)


class TestHydraServer(HydraServer):
    def handler_client_command(self, client, msg):
        """
        Users should override this method to add their own handler for client commands.
        """
        LOG.debug("Client {client} command data: {data}".format(client=client, data=msg))
        if msg["type"] == "cmd" and msg["cmd"] == "closed":
            self._remove_client(client)
        elif msg["type"] == "data":
            if msg["data"].get("cmd") == "quit":
                self.shutdown()
                return
            print("**** GOT DATA: %s" % msg)


class TestHydra(unittest.TestCase):
    def setUp(self):
        self.hs = None
        self.hsock = None

    def tearDown(self):
        pass

    # @unittest.skip("")
    def test_1_simple_create_destroy(self):
        self.hs = TestHydraServer()
        self.hs.shutdown()

    # @unittest.skip("")
    def test_2_simple_create_destroy_async_server(self):
        self.hs = TestHydraServer({"async_server": True})
        self.hs.start()
        time.sleep(1)
        self.hs.shutdown()

    # @unittest.skip("")
    def test_3_client_connect_send_msg_then_server_shutdown_on_client(self):
        self.hs = TestHydraServer({"async_server": True})
        self.hs.start()
        self.hsock = HydraSocket()
        self.hsock.connect()
        self.hsock.send(TEST_MSG_1)
        self.hs.shutdown()

        # Client should get a closed message from the server shutting down
        rlist, _, _ = select.select([self.hsock], [], [], 2)
        msg = self.hsock.recv()
        self.assertEqual(msg, CMD_MSG_SOCKET_CLOSED)
        # Cleanup client socket
        self.hsock.disconnect()

    # @unittest.skip("")
    def test_4_multiple_client_connections(self):
        self.hs = TestHydraServer({"async_server": True})
        self.hs.start()
        clients = []
        for i in range(10):
            client = HydraSocket()
            client.connect()
            clients.append(client)
        # Shutdown server
        self.hs.shutdown()
        # Client should get a closed message from the server shutting down
        rlist, _, _ = select.select(clients, [], [], 2)
        for client in rlist:
            msg = client.recv()
            self.assertEqual(msg, CMD_MSG_SOCKET_CLOSED)
            client.disconnect()

    # @unittest.skip("")
    def test_5_send_data_to_multiple_clients(self):
        self.hs = TestHydraServer({"async_server": True})
        self.hs.start()
        clients = []
        for i in range(10):
            client = HydraSocket()
            client.connect()
            clients.append(client)

        # Wait a little for all the clients to connect
        time.sleep(2)
        for client in self.hs.get_clients().keys():
            self.hs.send(client, {"test": self.hs.get_clients()[client]["client_port"]})
        # Shutdown server
        self.hs.shutdown()
        # Client should get a closed message from the server shutting down
        rlist, _, _ = select.select(clients, [], [], 2)
        for client in rlist:
            msg = client.recv()
            self.assertEqual(msg, {"type": "data", "data": {"test": client.source_port}})
            client.disconnect()


if __name__ == "__main__":
    setup_logger()
    unittest.main()
