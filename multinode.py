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
import subprocess
import time

import libs.hydra as Hydra
import libs.remote_run

LOG = logging.getLogger()


class PSScanServer(Hydra.HydraServer):
    def __init__(self, args={}):
        self.node_list = args.get("node_list", None)
        if self.node_list:
            args["async_server"] = True
        super(PSScanServer, self).__init__(args=args)

    def handler_client_command(self, client, msg):
        """
        Users should override this method to add their own handler for client commands.
        """
        LOG.debug("Client {addr} command data: {data}".format(addr=self.clients[client]["client_address"], data=msg))
        if not msg or (msg["type"] == "cmd" and msg["cmd"] == "closed"):
            self._remove_client(client)
        elif msg["type"] == "data":
            if msg["data"].get("cmd") == "quit":
                self.shutdown()

    def serve(self):
        LOG.info("Starting server")
        remote_server_addr = "192.168.3.40"
        remote_server_port = self.server_port
        CMD = [
            "python",
            "/ifs/zx/ps_scan/multinode.py",
            "client",
            "--port",
            str(remote_server_port),
            "--addr",
            str(remote_server_addr),
        ]
        thread_id = self.start()
        remote_list = []
        if self.node_list:
            print("Thread ID: %s" % thread_id)
            for node in self.node_list:
                if node["type"] == "onefs":
                    BASE_CMD = ["/usr/bin/isi_for_array", "-n", node["endpoint"]]
                    subproc = subprocess.Popen(BASE_CMD + CMD)
                    remote_entry = copy.deepcopy(node)
                    remote_entry["proc"] = subproc
                    remote_list.append(remote_entry)
                    print("Fired off: %s" % remote_entry)
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
        LOG.debug("Exiting main server")
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

        """
        Test case for aborting during large transfer.
        """
        if 2 in self.test_cases:
            test_client = Hydra.HydraSocket(
                {
                    "server_addr": server,
                    "server_port": port,
                }
            )
            connected = test_client.connect()
            if not connected:
                LOG.info("Unable to connect to server")
            rlist, _, xlist = select.select([test_client], [], [test_client], 20)
            print("MSG: %s" % test_client.recv())

        """
        Test case of a client that lets the server sever the connection
        """
        if 3 in self.test_cases:
            test_client3 = Hydra.HydraClient(
                {
                    "server_addr": server,
                    "server_port": port,
                }
            )
            test_client3.connect()
            test_client3.send_msg({"some": "dict"})
            # LOG.info("Sleeping for 30 seconds, abort the server to test client connection")
            # time.sleep(30)
            LOG.info("Putting client in select waiting for message for 30 seconds")
            rlist, _, xlist = select.select([test_client3], [], [test_client3], 30)
            print("RList: %s" % rlist)
            print("XList: %s" % xlist)

def main():
    import sys
    import optparse

    node_list = [
        {
            "endpoint": "5",
            "password": None,
            "type": "onefs",
            "user": None,
            "cmd": [],
        },
        {
            "endpoint": "6",
            "password": None,
            "type": "onefs",
            "user": None,
            "cmd": [],
        },
        {
            "endpoint": "7",
            "password": None,
            "type": "onefs",
            "user": None,
            "cmd": [],
        },
    ]
    parser = optparse.OptionParser()
    parser.add_option("--port", action="store", type="int", default=Hydra.DEFAULT_SERVER_PORT)
    parser.add_option("--addr", action="store", default=Hydra.DEFAULT_SERVER_ADDR)
    parser.add_option("--test", action="append", type="int", default=[])
    if len(sys.argv) < 2:
        print("{prog} [--port=#] [--addr=#] [--test=#] <server|client|auto>".format(prog=sys.argv[0]))
        sys.exit(1)
    (options, args) = parser.parse_args(sys.argv[1:])
    if args[0] not in ("client", "server", "auto"):
        print("{prog} [--port=#] [--addr=#] [--test=#] <server|client|auto>".format(prog=sys.argv[0]))
        sys.exit(1)
    if args[0] == "client":
        if not options.test:
            options.test.append(1)
        client = PSScanClient({"server_port": options.port, "server_addr": options.addr, "test_cases": options.test})
        client.connect()
    elif args[0] == "server":
        svr = PSScanServer({"server_port": options.port, "server_addr": options.addr})
        svr.serve()
    elif args[0] == "auto":
        svr = PSScanServer({"server_port": options.port, "server_addr": options.addr, "node_list": node_list})
        svr.serve()


if __name__ == "__main__" or __file__ == None:
    DEFAULT_LOG_FORMAT = (
        "%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - (%(process)d|%(threadName)s) %(message)s"
    )
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT))
    LOG.addHandler(log_handler)
    LOG.setLevel(logging.DEBUG)
    LOG.info("Starting server or client test")
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
