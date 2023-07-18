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


class TestHydraSocket(unittest.TestCase):
    def setUp(self):
        self.client_thread = None
        self.hsock = None
        self.listen_socket = None

    def setup_listen_socket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.assertTrue(sock)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        for i in range(10):
            try:
                sock.bind((DEFAULT_SERVER_ADDR, DEFAULT_SERVER_PORT))
                break
            except:
                pass
        sock.listen(DEFAULT_SOCKET_LISTEN_QUEUE)
        return sock

    def setup_hsock(self, hydra_obj, sock):
        success = hydra_obj.accept(sock)
        self.assertTrue(success)
        self.assertTrue(len(hydra_obj.wait_list) > 1)

    def setup_connect_server(self):
        client_conn = HydraSocket()
        client_conn.connect()
        return client_conn

    def setup_connect_new_client(self):
        connection, client_address = self.listen_socket.accept()
        connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
        new_hydra_handler = HydraSocket()
        new_hydra_handler.accept(connection)
        return new_hydra_handler

    def tearDown(self):
        self.teardown_listen_socket(self.listen_socket)
        self.listen_socket = None

        if self.client_thread:
            # Wait for thread to terminate
            self.client_thread.join()
        self.client_thread = None

        if self.hsock:
            self.hsock.disconnect()
            # Get the closed message
            rlist, _, _ = select.select([self.hsock], [], [], 2)
            msg = self.hsock.recv()
            self.assertEqual(msg, CMD_MSG_SOCKET_CLOSED)
        self.hsock = None

    def teardown_listen_socket(self, sock):
        if not sock:
            return
        if sock.fileno() != -1:
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()
        self.assertTrue(sock.fileno() == -1)

    def thread_client_connect_send_disconnect(self, log=False):
        if log:
            setup_logger()
        connection = self.setup_connect_server()
        connection.send(TEST_MSG_1)
        time.sleep(2)
        connection.disconnect()

    def thread_client_disconnect_during_large_transfer(self, log=False):
        if log:
            setup_logger()
        connection = self.setup_connect_server()
        connection.send({"blank": "." * 20000000})
        connection.disconnect()

    def thread_client_echo_data(self, log=False):
        if log:
            setup_logger()
        connection = self.setup_connect_server()
        rlist, _, _ = select.select([connection], [], [], 5)
        msg = connection.recv()
        connection.send(msg["data"])
        connection.flush()
        connection.disconnect()

    # @unittest.skip("")
    def test_1_simple_create_destroy(self):
        hsock = HydraSocket()
        self.assertTrue(isinstance(hsock, HydraSocket))
        hsock.disconnect()

    # @unittest.skip("")
    def test_2_simple_start_stop_server_handler(self):
        hsock = HydraSocket()
        sock = self.setup_listen_socket()
        self.setup_hsock(hsock, sock)

        hsock.disconnect()
        self.assertTrue(len(hsock.wait_list) == 0)

    # @unittest.skip("")
    def test_3_server_connect_1_client(self):
        self.listen_socket = self.setup_listen_socket()
        # Create client thread to initiate the TCP connection and send data
        self.client_thread = threading.Thread(target=self.thread_client_connect_send_disconnect, kwargs={"log": False})
        self.client_thread.start()

        # Wait for the client to connect
        rlist, _, _ = select.select([self.listen_socket], [], [], 2)
        self.assertTrue(len(rlist) > 0)

        # Accept the new connection, start the threads, and get back a HydraSocket object
        self.hsock = self.setup_connect_new_client()
        self.assertTrue(self.hsock != None)

        # Read data from client
        rlist, _, _ = select.select([self.hsock], [], [], 2)
        msg = self.hsock.recv()
        self.assertEqual(msg["data"], TEST_MSG_1)

    # @unittest.skip("")
    def test_5_server_connect_1_client_disconnect_large_transfer(self):
        self.listen_socket = self.setup_listen_socket()
        # Create client thread to initiate the TCP connection and send data
        self.client_thread = threading.Thread(
            target=self.thread_client_disconnect_during_large_transfer, kwargs={"log": False}
        )
        self.client_thread.start()

        # Wait for the client to connect
        rlist, _, _ = select.select([self.listen_socket], [], [], 2)
        self.assertTrue(len(rlist) > 0)

        # Accept the new connection, start the threads, and get back a HydraSocket object
        self.hsock = self.setup_connect_new_client()
        self.assertTrue(self.hsock != None)

    # @unittest.skip("")
    def test_6_server_send_data_to_client_and_echo(self):
        self.listen_socket = self.setup_listen_socket()
        # Create client thread to initiate the TCP connection and send data
        self.client_thread = threading.Thread(target=self.thread_client_echo_data, kwargs={"log": False})
        self.client_thread.start()

        # Wait for the client to connect
        rlist, _, _ = select.select([self.listen_socket], [], [], 2)
        self.assertTrue(len(rlist) > 0)

        # Accept the new connection, start the threads, and get back a HydraSocket object
        self.hsock = self.setup_connect_new_client()
        self.assertTrue(self.hsock != None)

        # Send data to client
        self.hsock.send(TEST_MSG_2)
        # Read echoed data from client
        rlist, _, _ = select.select([self.hsock], [], [], 4)
        msg = self.hsock.recv()
        self.assertEqual(msg["data"], TEST_MSG_2)


if __name__ == "__main__":
    setup_logger()
    unittest.main()
