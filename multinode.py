#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
PowerScale file scanner
"""
# fmt: off
__title__         = "ps_scan"
__version__       = "0.1.0"
__date__          = "03 August 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
# fmt: on
# import copy
import logging
import multiprocessing as mp
import optparse
import os
import platform
import queue
import select
import signal
import subprocess
import sys
import time

try:
    import resource
except:
    pass

import elasticsearch_wrapper
import scanit
import user_handlers
import helpers.misc as misc
from helpers.cli_parser import *
from helpers.constants import *

# import helpers.sliding_window_stats as sliding_window_stats
import libs.hydra as Hydra
import libs.remote_run as RR


LOG = logging.getLogger()


CLIENT_STATE_IDLE = "idle"
CLIENT_STATE_STARTING = "starting"
CLIENT_STATE_RUNNING = "running"
CLIENT_STATE_STOPPED = "stopped"
DEFAULT_QUEUE_TIMEOUT = 1
PS_CMD_DUMPSTATE = "dumpstate"
PS_CMD_QUIT = "quit"
PS_CMD_TOGGLEDEBUG = "toggledebug"
MSG_TYPE_CLIENT_DATA = "client_data"
MSG_TYPE_CLIENT_CLOSED = "client_closed"
MSG_TYPE_CLIENT_CONNECT = "client_connect"
MSG_TYPE_CLIENT_DIR_LIST = "client_dir_list"
MSG_TYPE_CLIENT_QUIT = "client_quit"
MSG_TYPE_CLIENT_REQ_DIR_LIST = "client_req_dir_list"
MSG_TYPE_CLIENT_STATE_IDLE = "client_state_idle"
MSG_TYPE_CLIENT_STATE_RUNNING = "client_state_running"
MSG_TYPE_CLIENT_STATE_STOPPED = "client_state_stopped"
MSG_TYPE_CLIENT_STATUS_DIR_COUNT = "client_status_dir_count"
MSG_TYPE_CLIENT_STATUS_STATS = "client_status_stats"
MSG_TYPE_COMMAND = "cmd"
MSG_TYPE_CONFIG_UPDATE = "config_update"
MSG_TYPE_DEBUG = "debug"
MSG_TYPE_REMOTE_CALLBACK = "remote_callback"


class PSScanClient(object):
    def __init__(self, args={}):
        """Initialize PSScanClient

        Parameters
        ----------
        args: dictionary
            dir_output_interval: int - Time in seconds between each directory queue update to the server
            poll_interval: int - Time in seconds to wait in the select statement
            server_addr: str - IP/FQDN of the ps_scan server process
            server_port: int - Port to connect to the ps_scan server
            stats_interval: int - Time in seconds between each statistics update to the server
        """
        self.dir_output_count = 0
        self.dir_output_interval = args.get("dir_output_interval", DEFAULT_DIR_OUTPUT_INTERVAL)
        self.dir_request_interval = args.get("dir_request_interval", DEFAULT_DIR_REQUEST_INTERVAL)
        self.poll_interval = args.get("poll_interval", DEFAULT_CMD_POLL_INTERVAL)
        self.scanner = scanit.ScanIt()
        self.server_addr = args.get("server_addr", Hydra.DEFAULT_SERVER_ADDR)
        self.server_port = args.get("server_port", Hydra.DEFAULT_SERVER_PORT)
        self.sent_data = 0
        self.socket = Hydra.HydraSocket(
            {
                "server_addr": self.server_addr,
                "server_port": self.server_port,
            }
        )
        self.stats_output_count = 0
        self.stats_output_interval = args.get("stats_interval", DEFAULT_STATS_OUTPUT_INTERVAL)
        self.status = CLIENT_STATE_STARTING
        self.wait_list = [self.socket]
        self.want_data = time.time()
        self.work_list = []
        self._init_scanner()
        #custom_state = self.scanner.get_custom_state()
        #user_handlers.init_custom_state(custom_state[0])

    def _exec_send_req_dir_list(self):
        self.socket.send(
            {
                "type": MSG_TYPE_CLIENT_REQ_DIR_LIST,
            }
        )
    
    def _exec_send_client_state_idle(self):
        self.socket.send(
            {
                "type": MSG_TYPE_CLIENT_STATE_IDLE,
            }
        )
        
    def _exec_send_client_state_running(self):
        self.socket.send(
            {
                "type": MSG_TYPE_CLIENT_STATE_RUNNING,
            }
        )
    def _exec_send_status_stats(self, now):
        stats_data = self.stats_merge(now)
        self.socket.send(
            {
                "type": MSG_TYPE_CLIENT_STATUS_STATS,
                "data": stats_data,
            }
        )
        LOG.debug("LOCAL STATS: {stats}".format(stats=stats_data))

    def _exec_send_status_dir_count(self):
        self.socket.send(
            {
                "type": MSG_TYPE_CLIENT_STATUS_DIR_COUNT,
                "data": len(self.work_list),  # TODO: Need to consolidate data from each subprocess
            }
        )

    def _init_scanner(self):
        s = self.scanner
        for attrib in [
            "dir_chunk",
            "dir_priority_count",
            "file_chunk",
            "file_q_cutoff",
            "file_q_min_cutoff",
        ]:
            pass
        s.exit_on_idle = False
        #s.handler_custom_stats =
        #s.handler_file = 
        #s.handler_init_thread =
        s.num_threads = 4 # TODO:
        s.processing_type = scanit.PROCESS_TYPE_ADVANCED

    def parse_config_update_log_level(self, cfg):
        log_level = cfg.get("log_level")
        if not log_level:
            LOG.error("log_level missing from cfg while updating the log level.")
            return
        LOG.setLevel(log_level)

    def parse_config_update_logger(self, cfg):
        format_string_vars = {
            "filename": platform.node(),
            "pid": os.getpid(),
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
            sys.stderr.write("ERROR: Logger filename string is invalid: {txt}\n".format(txt=str(ke)))
        except Exception as e:
            sys.stderr.write("ERROR: Unhandled exception while trying to configure logger: {txt}\n".format(txt=str(ke)))

    def parse_message(self, msg, now):
        LOG.debug("DEBUG: parse_message: {msg}".format(msg=msg))
        msg_type = msg.get("type")
        if msg_type == MSG_TYPE_CLIENT_DIR_LIST:
            self.work_list.extend(msg["work_item"])
            if self.work_list:
                self.exec_send_client_state_running()
                self.state = CLIENT_STATE_RUNNING
        elif msg_type == MSG_TYPE_CLIENT_QUIT:
            self.disconnect()
        elif msg_type == MSG_TYPE_CONFIG_UPDATE:
            cfg = msg.get("config")
            if "logger" in cfg:
                self.parse_config_update_logger(cfg)
            if "log_level" in cfg:
                self.parse_config_update_log_level(cfg)
        elif msg_type == MSG_TYPE_DEBUG:
            dbg = msg.get("cmd")
            if "dump_state" in dbg:
                self.dump_state()
        else:
            LOG.debug("Unhandled message: {msg}".format(msg=msg))

    def connect(self):
        LOG.info("Connecting to server at {svr}:{port}".format(svr=self.server_addr, port=self.server_port))
        connected = self.socket.connect()
        if not connected:
            LOG.info("Unable to connect to server")
            return
        continue_running = True
        start_wall = time.time()
        self.scanner.start()
        
        # Main client processing loop
        while continue_running:
            rlist, _, xlist = select.select(self.wait_list, [], self.wait_list, self.poll_interval)
            now = time.time()
            cur_dir_q_size = self.scanner.get_dir_queue_size()
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
                    self.parse_message(msg_data, now)
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

            # Determine if we should send a statistics update
            cur_stats_count = (now - start_wall) // self.stats_output_interval
            if cur_stats_count > self.stats_output_count:
                self.stats_output_count = cur_stats_count
                self._exec_send_status_stats(now)

            # Determine if we should send a directory queue count update
            cur_dir_count = (now - start_wall) // self.dir_output_interval
            if cur_dir_count > self.dir_output_count:
                self.dir_output_count = cur_dir_count
                self._exec_send_status_dir_count()
            
            # Ask parent process for more data if required, limit data requests to dir_request_interval seconds
            if (cur_dir_q_size == 0) and (now - self.want_data > self.dir_request_interval):
                self.want_data = now
                self._exec_send_req_dir_list()
            
            # Check if the scanner is idle
            if (
                not self.work_list
                and not self.scanner.get_file_queue_size()
                and not self.scanner.is_processing()
                and self.status != CLIENT_STATE_IDLE
            ):
                self.status = CLIENT_STATE_IDLE
                self._exec_send_client_state_idle()
            

    def dump_state(self):
        LOG.critical("\nDumping state\n" + "=" * 20)
        state = {}
        for member in [
            "dir_output_count",
            "dir_output_interval",
            "poll_interval",
            "sent_data",
            "server_addr",
            "server_port",
            "server_socket",
            "stats_output_count",
            "stats_output_interval",
            "wait_list",
            "want_data",
            "work_list",
        ]:
            state[member] = str(getattr(self))
        LOG.critical(state)

    def stats_merge(self, now):
        return {}


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

    def send_command(self):
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
        """Initialize PSScanClient

        Parameters
        ----------
        args: dictionary
            node_list: list - List of clients to auto-start. Format of each entry is in remote_run module
            queue_timeout: int - Number of seconds to wait for new messages before continuing with the processing loop
            request_work_interval: int - Number of seconds between requests to a client to return work
            scan_path: list - List of paths to scan
            script_path: str - Full path to script to run on clients
            server_connect_addr:str - FQDN/IP that clients should use to connect
            stats_interval: int - Number of seconds between each statistics update
        """
        args["async_server"] = True
        super(PSScanServer, self).__init__(args=args)
        self.client_count = 0
        self.client_state = {}
        self.connect_addr = args.get("server_connect_addr", None)
        self.continue_running = True
        self.msg_q = queue.Queue()
        self.node_list = args.get("node_list", None)
        self.queue_timeout = args.get("queue_timeout", DEFAULT_QUEUE_TIMEOUT)
        self.remote_state = None
        self.request_work_interval = args.get("request_work_interval", DEFAULT_REQUEST_WORK_INTERVAL)
        self.script_path = args.get("script_path", None)
        self.stats_output_count = 0
        self.stats_output_interval = args.get("stats_interval", DEFAULT_STATS_OUTPUT_INTERVAL)
        self.work_list = args.get("scan_path", [])
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
                        "filename": "log-{filename}-{pid}.txt",
                        "level": LOG.getEffectiveLevel(),
                    }
                },
            },
        )

    def _exec_send_one_work_item(self, client):
        if not self.work_list:
            return False
        try:
            work_item = self.work_list.pop(0)
            self.send(
                client,
                {
                    "type": MSG_TYPE_CLIENT_DIR_LIST,
                    "work_item": [work_item],
                },
            )
            return True
        except queue.Empty as qe:
            LOG.error("Work queue was not empty but unable to get a work item to send to the new client")
        return False

    def _exec_send_quit(self, client):
        self.send(
            client,
            {
                "type": MSG_TYPE_CLIENT_QUIT,
            },
        )

    def _exec_send_req_dir_list(self, client):
        self.send(
            client,
            {
                "type": MSG_TYPE_CLIENT_REQ_DIR_LIST,
            },
        )

    def _exec_send_work_items(self, client, work_items):
        self.send(
            client,
            {
                "type": MSG_TYPE_CLIENT_DIR_LIST,
                "work_item": work_items,
            },
        )
        return True

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
        state = {}
        for member in [
            "client_count",
            "connect_addr",
            "continue_running",
            "node_list",
            "queue_timeout",
            "remote_state",
            "request_work_interval",
            "script_path",
            "stats_output_count",
            "stats_output_interval",
            "work_list",
        ]:
            state[member] = str(getattr(self))
        LOG.critical(state)

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

    def output_statistics(self, now):
        # TODO: Fix this!
        # temp_stats = misc.merge_process_stats(process_states) or {}
        # new_files_processed = temp_stats.get("files_processed", stats_last_files_processed)
        # stats_fps_window.add_sample(new_files_processed - stats_last_files_processed)
        # stats_last_files_processed = new_files_processed
        # print_interim_statistics(
        #    temp_stats,
        #    now,
        #    start_wall,
        #    stats_fps_window.get_all_windows(),
        #    options.stats_interval,
        # )
        LOG.debug("DEBUG: OUTPUT STATS")
        pass

    def output_statistics_final(self, now, total_time):
        # TODO: Merge stats and output
        LOG.debug("DEBUG: OUTPUT FINAL STATS")
        pass

    def parse_message(self, msg, now):
        if msg["type"] == MSG_TYPE_CLIENT_DATA:
            client_idx = msg["client"]
            cur_client = self.client_state.get(client_idx)
            data = msg["data"]
            cid = cur_client["id"]
            LOG.debug("[Client {cid}] - Data: {data}".format(cid=cid, data=msg))
            data_type = data.get("type")
            if data_type == "cmd":
                cmd = data.get("cmd")
                LOG.debug("[Client {cid}] - Command: {cmd}".format(cid=cid, cmd=cmd))
                if cmd == PS_CMD_QUIT:
                    return {"cmd": PS_CMD_QUIT}
                elif cmd == PS_CMD_DUMPSTATE:
                    self._exec_dump_state()
                elif cmd == PS_CMD_TOGGLEDEBUG:
                    self._exec_toggle_debug()
                else:
                    LOG.error("[Client {cid}] - Unknown command: {cmd}".format(cid=cid, cmd=cmd))
            elif data_type == MSG_TYPE_CLIENT_DIR_LIST:
                cur_client["sent_data"] = 0
                cur_client["want_data"] = 0
                # Extend directory work list with items returned by the client
                self.work_list.extend(data["data"])
            elif data_type == MSG_TYPE_CLIENT_STATE_IDLE:
                cur_client["status"] = CLIENT_STATE_IDLE
                cur_client["want_data"] = now
            elif data_type == MSG_TYPE_CLIENT_STATE_RUNNING:
                cur_client["status"] = CLIENT_STATE_RUNNING
                cur_client["want_data"] = 0
            elif data_type == MSG_TYPE_CLIENT_STATE_STOPPED:
                cur_client["status"] = CLIENT_STATE_STOPPED
                cur_client["want_data"] = 0
            elif data_type == MSG_TYPE_CLIENT_STATUS_DIR_COUNT:
                cur_client["dir_count"] = data["data"]
            elif data_type == MSG_TYPE_CLIENT_STATUS_STATS:
                cur_client["stats"] = data["data"]
                cur_client["stats_time"] = now
            elif data_type == MSG_TYPE_CLIENT_REQ_DIR_LIST:
                cur_client["want_data"] = now
            else:
                LOG.error("[Client {cid}] - Unknown command: {cmd}".format(cid=cid, cmd=data_type))
        elif msg["type"] == MSG_TYPE_CLIENT_CLOSED:
            cur_client = self.client_state.get(msg["client"])
            LOG.debug("[Client {cid}] - Socket closed: {data}".format(cid=cur_client["id"], data=msg))
            cur_client["dir_count"] = 0
            cur_client["sent_data"] = 0
            cur_client["status"] = CLIENT_STATE_STOPPED
            cur_client["want_data"] = 0
        elif msg["type"] == MSG_TYPE_CLIENT_CONNECT:
            client_idx = msg["client"]
            self.client_count += 1
            LOG.debug("[Client {cid}] - Socket connected: {data}".format(cid=self.client_count, data=msg))
            state_obj = {
                "client": client_idx,
                "dir_count": 0,
                "id": self.client_count,
                "sent_data": 0,
                "stats": {},
                "stats_time": None,
                "status": CLIENT_STATE_STARTING,
                "want_data": now,
            }
            # Send configuration to client
            self._exec_send_config_update(client_idx)
            # Send up to 1 directory in our work queue to each connected client
            work_sent = self._exec_send_one_work_item(client_idx)
            if work_sent:
                state_obj["want_data"] = 0
            self.client_state[client_idx] = state_obj
        elif msg["type"] == MSG_TYPE_REMOTE_CALLBACK:
            # This type of command is sent from the remote_run module that handles spawning processes on other machines
            # TODO: Add code to handle re-launching dead processes
            # TODO: Log any console output if there is an error
            LOG.debug("Remote process message from client {client}: {data}".format(client=msg["client"], data=msg))
        else:
            LOG.debug("Unhandled message received: {data}".format(data=msg))
        return {}

    def remote_callback(self, client, client_id, msg=None):
        self.msg_q.put({"type": MSG_TYPE_REMOTE_CALLBACK, "data": msg, "client_id": client_id, "client": client})

    def serve(self):
        LOG.info("Starting server")
        start_wall = time.time()
        self.start()
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
                    reponse = self.parse_message(queue_item, now)
                    if reponse.get("cmd") == PS_CMD_QUIT:
                        self.continue_running = False
                except Exception as e:
                    # parse_message should handle exceptions. Any uncaught exceptions should terminate the program.
                    LOG.exception(e)
                    self.continue_running = False
                    continue

            # Output statistics
            #   The -1 is for a 1 second offset to allow time for stats to come from processes
            cur_stats_count = (now - start_wall) // self.stats_output_interval
            if cur_stats_count > self.stats_output_count:
                self.stats_output_count = cur_stats_count
                self.output_statistics(now)

            # Check all our client states to gather which are idle, which have work dirs, and which want work
            self.continue_running = False
            idle_clients = 0
            have_dirs_clients = []
            want_work_clients = []
            client_keys = self.client_state.keys()
            for key in client_keys:
                client = self.client_state[key]
                if not self.continue_running and client["status"] != CLIENT_STATE_STOPPED:
                    self.continue_running = True
                if client["status"] in (CLIENT_STATE_IDLE, CLIENT_STATE_STOPPED):
                    idle_clients += 1
                # Check if we need to request or send any directories to existing processes
                if client["want_data"]:
                    want_work_clients.append(client)
                # Any processes that have directories are checked
                if client["dir_count"] > 1:
                    have_dirs_clients.append(client)
            if not self.continue_running and self.work_list:
                # If there are no connected clients and there is work to do then continue running
                self.continue_running = True

            # If all sub-processes are idle and we have no work items, we can terminate all the scanner processes
            if idle_clients == len(client_keys) and not self.work_list:
                for key in client_keys:
                    client = self.client_state[key]
                    if client["status"] != CLIENT_STATE_STOPPED:
                        self._exec_send_quit(key)
                # Skip any further processing and just wait for processes to end
                continue

            # Send out our directories to all processes that want work if we have work to send
            if want_work_clients and self.work_list:
                LOG.debug("DEBUG: Server has work and has clients that want work")
                got_work_clients = []
                len_dir_list = len(self.work_list)
                len_want_work_procs = len(want_work_clients)
                increment = (len_dir_list // len_want_work_procs) + (1 * (len_dir_list % len_want_work_procs != 0))
                index = 0
                for client_key in want_work_clients:
                    work_dirs = self.work_list[index : index + increment]
                    if not work_dirs:
                        continue
                    self._exec_send_work_items(client_key, work_dirs)
                    self.client_state[client_key]["want_data"] = 0
                    index += increment
                    got_work_clients.append(client_key)
                # Remove from the want_work_clients list, any clients that got work sent to it
                for client_key in got_work_clients:
                    want_work_clients.remove(client_key)
                # Clear the dir_list variable now since we have sent all our work out
                self.work_list[:] = []

            # If processes want work and we know some processes have work, request those processes return work
            if want_work_clients and have_dirs_clients:
                LOG.debug("DEBUG: WANT WORK PROCS & HAVE DIR PROCS")
                for client_key in have_dirs_clients:
                    client = self.client_state[client_key]
                    LOG.debug("DEBUG: CLIENT: %s has dirs, evaluating if we should send message" % client_key)
                    # Limit the number of times we request data from each client to request_work_interval seconds
                    if (now - client["sent_data"]) > self.request_work_interval:
                        LOG.debug("DEBUG: ACTUALLY SENDING CMD_REQ_DIR to client: %s" % client_key)
                        self._exec_send_req_dir_list(client_key)
                        client["sent_data"] = now
        total_wall_time = time.time() - start_wall
        self.output_statistics_final(now, total_wall_time)
        LOG.info("{prog} shutting down.".format(prog=__title__))
        self.shutdown()
        LOG.info("{prog} shutdown complete.".format(prog=__title__))

    def shutdown(self):
        super(PSScanServer, self).shutdown()
        self.remote_state.terminate()


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


def set_resource_limits(min_memory=DEFAULT_ULIMIT_MEMORY, force=False):
    if not misc.is_onefs_os() and not force:
        return
    try:
        LOG.debug("VMEM ulimit values: {val}".format(val=resource.getrlimit(resource.RLIMIT_VMEM)))
    except Exception as e:
        pass
    try:
        physmem = int(misc.sysctl("hw.physmem"))
    except Exception as e:
        LOG.info("Unable to query physical memory sysctl hw.physmem: {err}".format(err=e))
        physmem = 0
    try:
        if physmem > min_memory or force:
            resource.setrlimit(resource.RLIMIT_VMEM, (options.ulimit_memory, options.ulimit_memory))
            LOG.info("Set VMEM ulimit to: {val} bytes".format(val=options.ulimit_memory))
        else:
            LOG.info("Machine does not meet minimum physical memory size to increase memory limit automatically.")
    except Exception as e:
        LOG.info("Unable to set resource limit setting on this platform.")


def main():
    # TODO: Leverage ps_scan CLI parser and extend
    parser = optparse.OptionParser()
    parser.add_option("--port", action="store", type="int", default=Hydra.DEFAULT_SERVER_PORT)
    parser.add_option("--addr", action="store", default=Hydra.DEFAULT_SERVER_ADDR)
    parser.add_option("--cmd", action="append", type="str", default=[])
    if len(sys.argv) < 2 or sys.argv[1] not in ("auto", "client", "command", "server"):
        sys.stderr.write("{prog} [--port=#] [--addr=#] <auto|client|command|server>\n".format(prog=sys.argv[0]))
        sys.exit(1)
    (options, args) = parser.parse_args(sys.argv[1:])

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
        client.send_command()
    elif args[0] in ("auto", "server"):
        LOG.info("Starting server")
        # TODO: Need to get this from config file or CLI options
        node_list = [
            {
                "endpoint": "5",
                "type": "onefs",
            },
            #{
            #    "endpoint": "5",
            #    "type": "onefs",
            #},
            {
                "endpoint": "6",
                "type": "onefs",
            },
            #{
            #    "endpoint": "6",
            #    "type": "onefs",
            #},
            #{
            #    "endpoint": "7",
            #    "type": "onefs",
            #},
            #{
            #    "endpoint": "7",
            #    "type": "onefs",
            #},
        ]
        ps_scan_server_options = {
            "scan_path": ["/ifs"],
            "script_path": get_script_path(),
            "server_port": options.port,
            "server_addr": options.addr,
            "server_connect_addr": get_local_internal_addr(),
            "node_list": None,
        }
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
