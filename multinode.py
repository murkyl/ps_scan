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


PS_CMD_DUMPSTATE = "dumpstate"
PS_CMD_QUIT = "quit"
PS_CMD_TOGGLEDEBUG = "toggledebug"
MSG_TYPE_CLIENT_DATA = "client_data"
MSG_TYPE_CLIENT_CLOSED = "client_closed"
MSG_TYPE_CLIENT_CONNECT = "client_connect"
MSG_TYPE_COMMAND = "cmd"
MSG_TYPE_CONFIG_UPDATE = "config_update"
MSG_TYPE_DEBUG = "debug"
MSG_TYPE_DIR_LIST = "dir_list"
MSG_TYPE_REMOTE_CALLBACK = "remote_callback"


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
        LOG.debug("DEBUG: parse_message: {msg}".format(msg=msg))
        msg_type = msg.get("type")
        if msg_type == MSG_TYPE_CONFIG_UPDATE:
            cfg = msg.get("config")
            if "logger" in cfg:
                self.parse_config_update_logger(cfg)
            if "log_level" in cfg:
                self.parse_config_update_log_level(cfg)
        elif msg_type == MSG_TYPE_DEBUG:
            dbg = msg.get("cmd")
            if "dump_state" in dbg:
                self.dump_state()

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
                if msg_type == MSG_TYPE_COMMAND:
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

    def dump_state(self):
        LOG.critical("\nDumping state\n" + "=" * 20)
        pass


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
        cmd = self.commands[0]
        LOG.info('Sending "{cmd}" command to server'.format(cmd=cmd))
        if cmd in (PS_CMD_DUMPSTATE, PS_CMD_TOGGLEDEBUG, PS_CMD_QUIT):
            self.socket.send({"type": MSG_TYPE_COMMAND, "cmd": cmd})
        else:
            LOG.error("Unknown command: {cmd}".format(cmd=cmd))
        time.sleep(1)
        self.socket.disconnect()


class PSScanServer(Hydra.HydraServer):
    def __init__(self, args={}):
        """
        Argument keys:
        server_connect_addr - FQDN/IP that clients should use to connect
        node_list - List of clients to auto-start
        script_path - Full path to script to run on clients
        """
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
        signal.signal(signal.SIGUSR2, self.handler_signal_usr2)

    def _exec_dump_state(self):
        msg = {
            "type": MSG_TYPE_DEBUG,
            "cmd": {
                "dump_state": True,
            },
        }
        self.send_all_clients(msg)
        self.dump_state()

    def _exec_send_config_update(self, client):
        self.send(
            client,
            {
                "type": MSG_TYPE_CONFIG_UPDATE,
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

    def _exec_send_one_work_item(self, client):
        if self.work_q.empty():
            return
        try:
            work_item = self.work_q.get(False)
            self.send(
                client,
                {
                    "type": MSG_TYPE_DIR_LIST,
                    "work_item": [work_item],
                },
            )
        except queue.Empty as qe:
            LOG.error("Work queue was not empty but unable to get a work item to send to the new client")

    def _exec_toggle_debug(self):
        cur_level = LOG.getEffectiveLevel()
        if cur_level != logging.DEBUG:
            LOG.setLevel(logging.DEBUG)
        else:
            LOG.setLevel(logging.INFO)
        # Send a log level update to all clients
        msg = {
            "type": MSG_TYPE_CONFIG_UPDATE,
            "config": {
                "log_level": LOG.getEffectiveLevel(),
            },
        }
        self.send_all_clients(msg)

    def dump_state(self):
        LOG.critical("\nDumping state\n" + "=" * 20)
        pass

    def handler_client_command(self, client, msg):
        """
        Users should override this method to add their own handler for client commands.
        """
        if not msg or (msg["type"] == MSG_TYPE_COMMAND and msg["cmd"] == "closed"):
            self.msg_q.put({"type": MSG_TYPE_CLIENT_CLOSED, "data": msg, "client": client})
            self._remove_client(client)
        elif msg["type"] == "data":
            self.msg_q.put({"type": MSG_TYPE_CLIENT_DATA, "data": msg["data"], "client": client})

    def handler_client_connect(self, client):
        self.msg_q.put({"type": MSG_TYPE_CLIENT_CONNECT, "client": client})

    def handler_signal_interrupt(self, signum, frame):
        self.continue_running = False
        LOG.debug("Termination signal received. Program is exiting.")

    def handler_signal_usr1(self, signum, frame):
        LOG.debug("SIGUSR1 signal received. Toggling debug.")
        self._exec_toggle_debug()

    def handler_signal_usr2(self, signum, frame):
        LOG.debug("SIGUSR2 signal received. Dumping state.")
        self._exec_dump_state()

    def launch_remote_processes(self):
        remote_server_addr = self.connect_addr
        remote_server_port = self.server_port
        script_path = self.script_path
        run_cmd = [
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
                    node["cmd"] = run_cmd
            self.remote_state = RR.RemoteRun({"callback": self.remote_callback})
            self.remote_state.connect(self.node_list)

    def parse_message(self, msg):
        if msg["type"] == MSG_TYPE_CLIENT_DATA:
            LOG.debug("Client data: {data}".format(data=msg))
            data = msg["data"]
            data_type = data.get("type")
            if data_type == "cmd":
                cmd = data.get("cmd")
                LOG.debug("Command received: {cmd}".format(cmd=cmd))
                if cmd == PS_CMD_QUIT:
                    return {"cmd": PS_CMD_QUIT}
                elif cmd == PS_CMD_DUMPSTATE:
                    self._exec_dump_state()
                elif cmd == PS_CMD_TOGGLEDEBUG:
                    self._exec_toggle_debug()
                else:
                    LOG.error("Unknown command received: {cmd}".format(cmd=cmd))
            elif data_type == MSG_TYPE_DIR_LIST:
                pass
        elif msg["type"] == MSG_TYPE_CLIENT_CLOSED:
            LOG.debug("Client socket closed: {data}".format(data=msg))
        elif msg["type"] == MSG_TYPE_CLIENT_CONNECT:
            LOG.debug("Client socket connected: {data}".format(data=msg))
            # Send configuration to client
            self._exec_send_config_update(msg["client"])
            # Send up to 1 directory in our work queue to each connected client
            self._exec_send_one_work_item(msg["client"])
        elif msg["type"] == MSG_TYPE_REMOTE_CALLBACK:
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
        self.msg_q.put({"type": MSG_TYPE_REMOTE_CALLBACK, "data": msg, "client_id": client_id, "client": client})

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
                    if reponse.get("cmd") == PS_CMD_QUIT:
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
