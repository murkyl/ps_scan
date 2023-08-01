#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
PowerScale file scanner
"""
# fmt: off
__title__         = "ps_scan"
__version__       = "1.0.0"
__date__          = "31 July 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
# fmt: on
import copy
import logging
import multiprocessing as mp
import os
import queue
import subprocess
import time

import libs.hydra as Hydra
import libs.remote_run as RR

LOG = logging.getLogger()


class PSScanServer(Hydra.HydraServer):
    def __init__(self, args={}):
        args["async_server"] = True
        super(PSScanServer, self).__init__(args=args)
        self.connect_addr = args.get("server_connect_addr", None)
        self.message_queue = queue.Queue()
        self.node_list = args.get("node_list", None)
        self.proc_list = []
        self.remote_state = None
        self.script_path = args.get("script_path", None)
        if not (self.connect_addr and self.script_path):
            raise Exception("Server connect address and script path is required")

    def handler_client_command(self, client, msg):
        """
        Users should override this method to add their own handler for client commands.
        """
        # LOG.debug("Client {addr} command data: {data}".format(addr=self.clients[client]["client_address"], data=msg))
        if not msg or (msg["type"] == "cmd" and msg["cmd"] == "closed"):
            self.message_queue.put({"type": "client_closed", "data": msg, "client": client})
            self._remove_client(client)
        elif msg["type"] == "data":
            self.message_queue.put({"type": "client_command", "data": msg["data"], "client": client})

    def handler_client_connect(self, client):
        # LOG.debug("Client {addr} just connected".format(addr=client))
        self.message_queue.put({"type": "client_connect", "client": client})

    def launch_remote_processes(self):
        remote_server_addr = self.connect_addr
        remote_server_port = self.server_port
        script_path = self.script_path
        CMD = [
            "python",
            script_path,
            "client",
            "--port",
            str(remote_server_port),
            "--addr",
            remote_server_addr,
        ]
        if self.node_list:
            for node in self.node_list:
                if node.get("type") != "default":
                    node["cmd"] = CMD
            self.remote_state = RR.RemoteRun({"callback": self.remote_callback})
            self.remote_state.connect(self.node_list)

    def parse_message(self, msg):
        if msg["type"] == "quit":
            LOG.debug("Quit command received: %s" % msg)
        elif msg["type"] == "client_closed":
            LOG.debug("Client socket closed: %s" % msg)
        elif msg["type"] == "client_command":
            LOG.debug("Client socket command: %s" % msg)
        elif msg["type"] == "client_connect":
            LOG.debug("Client socket connected: %s" % msg)
        elif msg["type"] == "remote_msg":
            # This type of command is sent from the remote_run module that handles spawning processes on other machines
            # TODO: Add code to handle re-launching dead processes
            LOG.debug("Remote process message: %s" % msg)
        else:
            LOG.debug("MSG is: %s" % msg)

    def remote_callback(self, client, client_id, msg=None):
        # LOG.debug("Remote client {client_id} returned message: {msg}".format(client_id=client_id, msg=msg))
        self.message_queue.put({"type": "remote_msg", "data": msg, "client_id": client_id, "client": client})

    def serve(self):
        LOG.info("Starting server")
        start_time = time.time()
        thread_id = self.start()
        self.launch_remote_processes()
        # Start main processing loop, send initial directories to process to clients
        # Wait for clients to request work and redistribute work as needed.

        try:
            continue_running = True
            while continue_running:
                now = time.time()
                try:
                    queue_item = self.message_queue.get(timeout=1)
                except queue.Empty as qe:
                    queue_item = None
                if queue_item:
                    self.parse_message(queue_item)
                LOG.debug("CHECK!")
                if now - start_time > 10:
                    continue_running = False
        except Exception as e:
            LOG.exception(e)
        """
        print("WAIT FOR PROC COMPLETE")
        for i in range(100):
            print("i is: %s" % i)
            remove_list = []
            for remote_entry in remote_list:
                poll_result = remote_entry["proc"].poll()
                if poll_result is not None:
                    print("Command returned: %s" % remote_entry)
                    remove_list.append(remote_entry)
                else:
                    print("Poll result: %s" % poll_result)
            for x in remove_list:
                remote_list.remove(x)
            if not remote_list:
                print("Breaking out")
                break
            time.sleep(1)
        """
        LOG.debug("ps_scan coordinator exiting")
        self.shutdown()


class PSScanClient(object):
    def __init__(self, args={}):
        self.server_addr = args.get("server_addr", Hydra.DEFAULT_SERVER_ADDR)
        self.server_port = args.get("server_port", Hydra.DEFAULT_SERVER_PORT)
        self.test_cases = args.get("test_cases", [1])

    def connect(self):
        LOG.info("Connecting to server at {svr}:{port}".format(svr=self.server_addr, port=self.server_port))

        """
        Test case of a normal connect, send, then disconnect
        """
        if 1 in self.test_cases:
            test_client = Hydra.HydraSocket(
                {
                    "server_addr": self.server_addr,
                    "server_port": self.server_port,
                }
            )
            connected = test_client.connect()
            if not connected:
                LOG.info("Unable to connect to server")
            test_client.send({"some": "dict"})
            LOG.info("Sleeping 1 second")
            time.sleep(1)
            test_client.disconnect()

def get_local_internal_addr():
    # TODO: Add checks for running on OneFS and other OSes
    subproc = subprocess.Popen(
        ["isi_nodes", "-L", "\"%{internal}\""],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = subproc.communicate()
    addr = stdout.strip().replace("\"", "")
    return addr

def get_script_path():
    return os.path.abspath(__file__)

def main():
    import optparse
    import sys

    node_list = [
        {
            "endpoint": "5",
            "type": "onefs",
        },
        {
            "endpoint": "6",
            "type": "onefs",
        },
        {
            "endpoint": "7",
            "type": "onefs",
        },
    ]
    # TODO: Re-write better CLI parser
    parser = optparse.OptionParser()
    parser.add_option("--port", action="store", type="int", default=Hydra.DEFAULT_SERVER_PORT)
    parser.add_option("--addr", action="store", default=Hydra.DEFAULT_SERVER_ADDR)
    parser.add_option("--test", action="append", type="int", default=[])
    if len(sys.argv) < 2 or args[0] not in ("client", "server", "auto"):
        print("{prog} [--port=#] [--addr=#] <server|client|auto>".format(prog=sys.argv[0]))
        sys.exit(1)
    (options, args) = parser.parse_args(sys.argv[1:])

    ps_scan_server_options = {
        "script_path": get_script_path(),
        "server_port": options.port,
        "server_addr": options.addr,
        "server_connect_addr": get_local_internal_addr(),
        # TODO: Need to get this from config file or CLI options
        "node_list": None,
    }

    if args[0] == "client":
        LOG.info("Starting client")
        client = PSScanClient({"server_port": options.port, "server_addr": options.addr})
        client.connect()
    elif args[0] == "server":
        LOG.info("Starting server")
        svr = PSScanServer(ps_scan_server_options)
        svr.serve()
    elif args[0] == "auto":
        LOG.info("Starting server")
        ps_scan_server_options["node_list"] = node_list
        svr = PSScanServer(ps_scan_server_options)
        svr.serve()


if __name__ == "__main__" or __file__ == None:
    DEFAULT_LOG_FORMAT = (
        "%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - (%(process)d|%(threadName)s) %(message)s"
    )
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT))
    LOG.addHandler(log_handler)
    LOG.setLevel(logging.DEBUG)
    # Support scripts built into executable on Windows
    try:
        mp.freeze_support()
        if hasattr(mp, "set_start_method"):
            # Force all OS to behave the same when spawning new process
            mp.set_start_method("spawn")
    except:
        # Ignore these errors as they are either already set or do not apply to this system
        pass
    main()
