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
import platform
import queue
import select
import signal
import subprocess
import time

import libs.hydra as Hydra
import libs.remote_run as RR

LOG = logging.getLogger()


class PSScanClient(object):
    def __init__(self, args={}):
        # TODO: Set default for poll interval
        self.poll_interval = args.get("poll_interval", 1)
        self.server_addr = args.get("server_addr", Hydra.DEFAULT_SERVER_ADDR)
        self.server_port = args.get("server_port", Hydra.DEFAULT_SERVER_PORT)
        self.socket = Hydra.HydraSocket(
            {
                "server_addr": self.server_addr,
                "server_port": self.server_port,
            }
        )
        self.wait_list = [self.socket]

    def parse_config_update_log_level(self, cfg):
        log_level = cfg.get("log_level")
        if not log_level:
            LOG.error("log_level missing from cfg while updating the log level.")
            return
        LOG.setLevel(log_level)

    def parse_config_update_logger(self, cfg):
        format_string_vars = {
            "filename": platform.node(),
        }
        try:
            logger_block = cfg.get("logger")
            if logger_block["destination"] == "file":
                log_filename = logger_block["filename"].format(**format_string_vars)
                log_handler = logging.FileHandler(log_filename)
                log_handler.setFormatter(logging.Formatter(logger_block["format"]))
                LOG.handlers[:] = []
                LOG.addHandler(log_handler)
                LOG.setLevel(logger_block["level"])
        except KeyError as ke:
            print("ERROR: Logger filename string is invalid: {txt}".format(txt=str(ke)))
        except Exception as e:
            print("ERROR: Unhandled exception while trying to configure logger: {txt}".format(txt=str(ke)))

    def parse_message(self, msg):
        msg_type = msg.get("type")
        if msg_type == "config_update":
            cfg = msg.get("config")
            if "logger" in cfg:
                self.parse_config_update_logger(cfg)
            if "log_level" in cfg:
                self.parse_config_update_log_level(cfg)

    def connect(self):
        LOG.info("Connecting to server at {svr}:{port}".format(svr=self.server_addr, port=self.server_port))
        connected = self.socket.connect()
        if not connected:
            LOG.info("Unable to connect to server")
            return
        continue_running = True
        while continue_running:
            rlist, _, xlist = select.select(self.wait_list, [], self.wait_list, self.poll_interval)
            if rlist:
                data = self.socket.recv()
                msg_type = data.get("type")
                if msg_type == "cmd":
                    cmd = data.get("cmd")
                    LOG.debug("Command received: {cmd}".format(cmd=cmd))
                    if cmd == "closed":
                        self.wait_list.remove(self.socket)
                        self.socket.disconnect()
                        continue_running = False
                        continue
                elif msg_type == "data":
                    msg_data = data.get("data")
                    self.parse_message(msg_data)
                elif msg_type is None:
                    LOG.debug("Socket ready to read but no data was received. We should shutdown now.")
                    self.wait_list.remove(self.socket)
                    self.socket.disconnect()
                    continue_running = False
                else:
                    LOG.debug("Unexpected message received: {data}".format(data=data))
            if xlist:
                LOG.error("Socket encountered an error or was closed")
                self.wait_list.remove(self.socket)
                continue_running = False
                break
            LOG.debug("TOCK!")


class PSScanCommandClient(object):
    def __init__(self, args={}):
        self.commands = args.get("commands", [])
        self.server_addr = args.get("server_addr", Hydra.DEFAULT_SERVER_ADDR)
        self.server_port = args.get("server_port", Hydra.DEFAULT_SERVER_PORT)
        self.socket = Hydra.HydraSocket(
            {
                "server_addr": self.server_addr,
                "server_port": self.server_port,
            }
        )

    def connect(self):
        if not self.commands:
            LOG.info("No commands to send. No connection to server required.")
            return
        LOG.info("Connecting to server at {svr}:{port}".format(svr=self.server_addr, port=self.server_port))
        connected = self.socket.connect()
        if not connected:
            LOG.info("Unable to connect to server")
            return
        if self.commands[0] == "quit":
            LOG.info('Sending "quit" command to server')
            self.socket.send({"type": "cmd", "cmd": "quit"})
        time.sleep(1)
        self.socket.disconnect()


class PSScanServer(Hydra.HydraServer):
    def __init__(self, args={}):
        args["async_server"] = True
        super(PSScanServer, self).__init__(args=args)
        self.connect_addr = args.get("server_connect_addr", None)
        self.continue_running = True
        self.msg_q = queue.Queue()
        self.node_list = args.get("node_list", None)
        # TODO: Set queue timeout
        self.queue_timeout = 1
        self.remote_state = None
        self.script_path = args.get("script_path", None)
        self.work_q = queue.Queue()
        if not (self.connect_addr and self.script_path):
            raise Exception("Server connect address and script path is required")
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.handler_signal_interrupt)
        signal.signal(signal.SIGUSR1, self.handler_signal_usr1)

    def handler_client_command(self, client, msg):
        """
        Users should override this method to add their own handler for client commands.
        """
        if not msg or (msg["type"] == "cmd" and msg["cmd"] == "closed"):
            self.msg_q.put({"type": "control_client_closed", "data": msg, "client": client})
            self._remove_client(client)
        elif msg["type"] == "data":
            self.msg_q.put({"type": "client_data", "data": msg["data"], "client": client})

    def handler_client_connect(self, client):
        self.msg_q.put({"type": "control_client_connect", "client": client})

    def handler_signal_interrupt(self, signum, frame):
        self.continue_running = False
        LOG.debug("Termination signal received. Program is exiting.")

    def handler_signal_usr1(self, signum, frame):
        LOG.debug("SIGUSR1 signal received. Toggling debug.")
        cur_level = LOG.getEffectiveLevel()
        if cur_level != logging.DEBUG:
            LOG.setLevel(logging.DEBUG)
        else:
            LOG.setLevel(logging.INFO)
        # Send a log level update to all clients
        msg = {
            "type": "config_update",
            "config": {
                "log_level": LOG.getEffectiveLevel(),
            },
        }
        self.send_all_clients(msg)

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
        if msg["type"] == "client_data":
            LOG.debug("Client data: {data}".format(data=msg))
            data = msg["data"]
            data_type = data.get("type")
            if data_type == "cmd":
                cmd = data.get("cmd")
                if cmd == "quit":
                    LOG.debug("Quit command received: {data}".format(data=data))
                    return {"cmd": "quit"}
            elif data_type == "dir_list":
                pass
        elif msg["type"] == "control_client_closed":
            LOG.debug("Client socket closed: {data}".format(data=msg))
        elif msg["type"] == "control_client_connect":
            LOG.debug("Client socket connected: {data}".format(data=msg))
            # Send configuration to client
            self.send(
                msg["client"],
                {
                    "type": "config_update",
                    "config": {
                        "logger": {
                            "format": "%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - (%(process)d|%(threadName)s) %(message)s",
                            "destination": "file",
                            "filename": "log-{filename}.txt",
                            "level": LOG.getEffectiveLevel(),
                        }
                    },
                },
            )
            # Send up to 1 directory in our work queue to each connected client
            if not self.work_q.empty():
                try:
                    work_item = self.work_q.get(False)
                    self.send(
                        msg["client"],
                        {
                            "type": "dir_list",
                            "work_item": [work_item],
                        },
                    )
                except queue.Empty as qe:
                    LOG.error("Work queue was not empty but unable to get a work item to send to the new client")
        elif msg["type"] == "control_remote_msg":
            # This type of command is sent from the remote_run module that handles spawning processes on other machines
            # TODO: Add code to handle re-launching dead processes
            # TODO: Log any console output if there is an error
            LOG.debug(
                "Remote process message from client ID {client}: {data}".format(client=msg["client_id"], data=msg)
            )
        else:
            LOG.debug("MSG is: {data}".format(data=msg))
        return {}

    def remote_callback(self, client, client_id, msg=None):
        self.msg_q.put({"type": "control_remote_msg", "data": msg, "client_id": client_id, "client": client})

    def send_all_clients(self, msg):
        # TODO:
        pass

    def serve(self):
        LOG.info("Starting server")
        start_time = time.time()
        thread_id = self.start()
        self.launch_remote_processes()
        # Start main processing loop
        # Wait for clients to connect, request work, and redistribute work as needed.
        while self.continue_running:
            now = time.time()
            try:
                queue_item = self.msg_q.get(timeout=self.queue_timeout)
            except queue.Empty as qe:
                queue_item = None
            except Exception as e:
                LOG.exception(e)
                self.continue_running = False
                continue
            else:
                try:
                    reponse = self.parse_message(queue_item)
                    if reponse.get("cmd") == "quit":
                        self.continue_running = False
                except Exception as e:
                    # parse_message should handle exceptions. Any uncaught exceptions should terminate the program.
                    LOG.exception(e)
                    self.continue_running = False
                    continue
            LOG.debug("TICK!")
        LOG.debug("ps_scan shutting down")
        self.shutdown()
        LOG.debug("ps_scan shutdown complete")


def get_local_internal_addr():
    # TODO: Add checks for running on OneFS and other OSes
    subproc = subprocess.Popen(
        ["isi_nodes", "-L", '"%{internal}"'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = subproc.communicate()
    addr = stdout.strip().replace('"', "")
    return addr


def get_script_path():
    return os.path.abspath(__file__)


def main():
    import optparse
    import sys

    # TODO: Need to get this from config file or CLI options
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
    parser.add_option("--cmd", action="append", type="str", default=[])
    if len(sys.argv) < 2 or sys.argv[1] not in ("auto", "client", "command", "server"):
        print("{prog} [--port=#] [--addr=#] <auto|client|command|server>".format(prog=sys.argv[0]))
        sys.exit(1)
    (options, args) = parser.parse_args(sys.argv[1:])

    ps_scan_server_options = {
        "script_path": get_script_path(),
        "server_port": options.port,
        "server_addr": options.addr,
        "server_connect_addr": get_local_internal_addr(),
        "node_list": None,
    }

    if args[0] == "client":
        LOG.info("Starting client")
        client = PSScanClient({"server_port": options.port, "server_addr": options.addr})
        client.connect()
    elif args[0] == "command":
        LOG.info("Sending command to server")
        client = PSScanCommandClient(
            {
                "commands": options.cmd,
                "server_port": options.port,
                "server_addr": options.addr,
            }
        )
        client.connect()
    elif args[0] in ("auto", "server"):
        LOG.info("Starting server")
        if args[0] == "auto":
            # Setting the node list will cause the server to automatically launch clients
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
