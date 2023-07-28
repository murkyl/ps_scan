#!/usr/bin/env python
# -*- coding: utf8 -*-
# fmt: off
__license__    = "MIT"
__author__     = "Andrew Chung"
__maintainer__ = "Andrew Chung"
__email__      = "Andrew.Chung@dell.com"
__credits__    = []
__copyright__  = """"""
__all__        = [
    "HydraServer",
]
# fmt: on

import logging
import multiprocessing as mp
import select
import socket
import threading


from .hydra_const import *
from .hydra_socket import HydraSocket

LOG = logging.getLogger(__name__)


class HydraServer(object):
    def __init__(self, args={}):
        self.async_server = args.get("async_server", DEFAULT_ASYNC_SERVER)
        self.poll_interval = args.get("poll_interval", DEFAULT_POLL_INTERVAL)
        self.server_addr = args.get("server_addr", DEFAULT_BIND_ADDR)
        self.server_port = args.get("server_port", DEFAULT_SERVER_PORT)
        self.socket_listen_queue = args.get("socket_listen_queue", DEFAULT_SOCKET_LISTEN_QUEUE)
        self.clients = {}  # Connected client list
        self.serve_continue = False  # Flag used to keep the server running
        self.server_socket = None  # Socket used to listen for new connections
        self.server_thread = None  # Thread object when using async server mode
        self.wait_list = []  # List of sockets and pipes for use in select
        self.wait_shutdown = mp.Event()  # Flag used to have a synchronous shutdown

    def _cleanup(self):
        self.server_socket.close()
        for key in list(self.clients.keys()):
            self._remove_client(key)

    def _connect_new_client(self):
        connection, client_address = self.server_socket.accept()
        LOG.debug("New connection from client {addr}, FD: {fd}".format(addr=client_address, fd=connection.fileno()))
        connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
        hsock = HydraSocket()
        client_obj = {
            "client_address": client_address,
            "client_port": connection.getpeername()[1],
            "connection": connection,
            "hsock": hsock,
        }
        hsock.accept(connection)
        self.clients[hsock] = client_obj
        self.wait_list.append(hsock)

    def _remove_client(self, client):
        try:
            LOG.debug("Removing client from select list: {client}".format(client=client))
            if client in self.wait_list:
                self.wait_list.remove(client)
            if client not in self.clients.keys():
                return
            cleaup_client = self.clients[client]
            del self.clients[client]
            cleaup_client["hsock"].flush()
            cleaup_client["hsock"].disconnect()
        except Exception as e:
            LOG.exception("Unexpected exception when removing client: {err}".format(err=e))

    def _serve_forever(self):
        self.serve_continue = True
        while self.serve_continue:
            rlist, _, xlist = select.select(self.wait_list, [], self.wait_list, self.poll_interval)
            for s in rlist:
                if s is self.server_socket:
                    # Handle new client connections
                    self._connect_new_client()
                    continue
                if s in self.clients.keys():
                    self.handler_client_command(s, s.recv())
                    continue
                # This is somehow an unknown client. Remove it from the listen list
                self._remove_client(s)
            for s in xlist:
                LOG.debug("Socket exception in server_forever: {client}".format(client=s))
                self._remove_client(s)
        LOG.debug("Shutting down Hydra server")
        self._cleanup()
        self.wait_shutdown.set()

    def _setup_server(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
        self.server_socket.bind((self.server_addr, self.server_port))
        self.server_socket.listen(DEFAULT_SOCKET_LISTEN_QUEUE)
        self.wait_list.append(self.server_socket)

    def get_clients(self):
        return self.clients

    def handler_client_command(self, client, msg):
        """
        Users should override this method to add their own handler for client commands.
        """
        LOG.debug("Client {client} command data: {data}".format(client=client, data=msg))
        if msg["type"] == "cmd" and msg["cmd"] == "closed":
            self._remove_client(client)

    def handler_client_connect(self, client):
        pass

    def send(self, client_id, msg):
        LOG.debug("Sending message to client: {client}".format(client=client_id))
        if client_id not in self.clients:
            LOG.error("Unable to find client to send message: {client}".format(client=client_id))
        hsock = self.clients[client_id]["hsock"]
        hsock.send(msg)

    def start(self):
        if not self.server_socket:
            self._setup_server()
        if self.async_server:
            self.server_thread = threading.Thread(target=self._serve_forever, kwargs={})
            self.server_thread.start()
            return self.server_thread
        self._serve_forever()
        return None

    def shutdown(self, wait=True):
        if not self.serve_continue or not self.server_socket:
            return
        self.serve_continue = False
        if wait:
            self.wait_shutdown.clear()
            self.wait_shutdown.wait()
