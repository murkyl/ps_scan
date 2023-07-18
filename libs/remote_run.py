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
    "RemoteRun",
]
# fmt: on

import logging
import subprocess
import threading


DEFAULT_ISI_FOR_ARRAY_BIN = "/usr/bin/isi_for_array"
DEFAULT_PROC_POLL_TIMEOUT = 5
LOG = logging.getLogger(__name__)


# TODO:
# Improve decode_clients with support for node arrays like 1-3, 5, and also for decoding node types like hybrid nodes,
# all flash nodes, avoiding A series nodes, and all nodes with memory of at least 64 GB
class RemoteRun(object):
    def __init__(self, args={}):
        self.remote_clients = {}
        self.callback_function = args.get("callback", self.handle_callback)
        self.poll_timeout = args.get("poll_timeout", DEFAULT_PROC_POLL_TIMEOUT)
        
    def _decode_clients(self, client_config):
        clients = []
        default = {}
        for client in config:
            if client.get("type") == "default":
                default = client
            else:
                clients.append(client)
        for client in clients:
            if default:
                # Merge defaults into each client
                pass
            # Perform additional client processing
            pass
        return clients, default

    def _threaded_client(self, client, event, default={}):
        cmd = client.get("cmd", default.get("cmd", []))
        code = 0
        endpoint = client.get("endpoint", default.get("endpoint"))
        response = None
        valid = True
        # Validate command
        if not valid:
            self.callback_function(client, {"state": "invalid_params"})
            return
        self.callback_function(client, {"state": "start"})
        # Execute command based on endpoint
        if client["type"] == "onefs":
            subproc = subprocess.Popen(
                [DEFAULT_ISI_FOR_ARRAY_BIN, "-n", endpoint] + cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        elif client["type"] == "ssh":
            pass
        # Wait for the process to terminate or for a signal to terminate
        valid = subproc.poll()
        while valid is None or event.is_set():
            event.wait(self.poll_timeout)
            valid = subproc.poll()
        stdout, stderr = subproc.communicate()
        response = "\n".join([stdout, stderr])
        self.callback_function(client, {"state": "end", "exit_code": code, "response": response})

    def connect(self, clients=[]):
        if not isinstance(clients, list):
            clients = [clients]
        # Loop through the client config and expand into individual clients and the default value if present
        working_list, default = self._decode_clients(clients)
        # Connect to each client
        for client in working_list:
            event_handle = thread.event()
            thread_handle = threading.Thread(
                target=self._threaded_client,
                kwargs={
                    "client": client,
                    "default": default,
                    "event": event_handle,
                }
            )
        remote_clients[thread_handle] = {
            "thread_handle": thread_handle,
            "client": client,
            "default": default,
            "event": event_handle,
        }

    def disconnect(self, client):
        remote_client = self.remote_clients.get(client)
        if not remote_client:
            return False
        remote_client["event"].set()
        del self.remote_clients[client]
        return True

    def get_client_list(self):
        return list(self.remote_clients().keys())

    def handle_callback(self, client, msg=None):
        pass

    def shutdown(self):
        keys = list(self.remote_clients.keys())
        for key in keys:
            self.disconnect(key)
