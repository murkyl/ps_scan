#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
PowerScale file scanner client
"""
# fmt: off
__title__         = "ps_scan_client"
__version__       = "0.1.0"
__date__          = "15 August 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
__all__ = [
    "PSScanClient",
]
# fmt: on
import json
import logging
import os
import platform
import select
import sys
import time

from helpers.constants import *
import helpers.scanit as scanit
import helpers.scanner as scanner
import helpers.user_handlers as user_handlers
import libs.hydra as Hydra


LOG = logging.getLogger(__name__)


class PSScanClient(object):
    def __init__(self, args={}):
        """Initialize PSScanClient

        Parameters
        ----------
        args: dictionary
            debug: int - Debug flag count from the CLI. The higher this number, the more debug may be logged
            dir_output_interval: int - Time in seconds between each directory queue size update to the server
            dir_request_interval: int - Limit how often a client can request more work directories, in seconds
            dirq_chunk: int - Scanner: Maximum number of directories to bundle in each work unit. For example, if a
                directory listing contained 100 subdirectories, then it would be broken into ceil(100/dirq_chunk) pieces
            dirq_priority: int - Scanner: Number of threads that are allowed to walk directories when the number of
                files drop below a certain threshold. These directory threads are a way to prioritize processing a new
                directory to get more files queued up. If the number of files drops below a lower threshold, then all
                new available threads will become directory processing threads.
            dirq_request_percentage: float - Percentage of the queued working directories to return per request. As an
                example, if a scanner has 200 directories yet to be processed and this value is set to 0.5, then when a
                server requests to return some directories for other clients to process, the scanner would return
                100 * 0.5 = 50 directories
            fileq_chunk: int - Scanner: Maximum number of files to bundle in each work unit
            fileq_cutoff: int - The higher of 2 limits on the number of queued files. When the number of queued files
                falls below this number, the dirq_priority setting is used to determine if more threads need to be
                processing directories
            file_q_min_cutoff: int - The lower of 2 limits on the number of queued files. When the number of queued
                files falls below this number, all new available threads will process directories to queue up more files
                for processing
            nodepool_translation: dict - Dictionary mapping diskpool/nodepool ID numbers to a nodepool name
            num_threads: int - Number of threads the file scanner will use
            poll_interval: int - Time in seconds to wait in the select statement
            scanner_file_handler: function pointer - Pointer to a method that will process a list of files
            server_addr: str - IP/FQDN of the ps_scan server process
            server_port: int - Port to connect to the ps_scan server
            stats_interval: int - Time in seconds between each statistics update to the server
        """
        self.cli_options = args
        self.client_config = {}
        self.client_id = ""
        self.config_update_count = 0
        self.debug_count = args.get("debug", 0)
        self.dir_output_count = 0
        self.max_work_items = args.get("max_work_items", DEFAULT_MAX_WORK_ITEMS_PER_REQUEST)
        self.poll_interval = args.get("poll_interval", DEFAULT_CMD_POLL_INTERVAL)
        self.scanner = scanit.ScanIt()
        # TODO: Change how file handler is called
        self.scanner_file_handler = args.get("scanner_file_handler", scanner.file_handler_basic)
        self.sent_data = 0
        self.server_addr = args.get("server_addr", Hydra.DEFAULT_SERVER_ADDR)
        self.server_port = args.get("server_port", Hydra.DEFAULT_SERVER_PORT)
        self.socket = Hydra.HydraSocket(
            {
                "server_addr": self.server_addr,
                "server_port": self.server_port,
            }
        )
        self.stats_output_count = 0
        self.status = CLIENT_STATE_STARTING
        self.wait_list = [self.socket]
        self.want_data = time.time()
        self.dir_q_count = 0
        self._process_args(args)
        self._init_scanner()

    def _exec_send_dir_list(self, dir_list):
        self.socket.send(
            {
                "type": MSG_TYPE_CLIENT_DIR_LIST,
                "work_item": dir_list,
                "dir_q_count": self.dir_q_count,
            }
        )

    def _exec_send_req_dir_list(self):
        self.socket.send(
            {
                "type": MSG_TYPE_CLIENT_REQ_DIR_LIST,
            }
        )
        if self.debug_count > 1:
            LOG.debug(
                {
                    "msg": "Asking server for more work",
                    "dir_q_count": self.dir_q_count,
                    "file_q_count": self.scanner.get_file_queue_size(),
                }
            )

    def _exec_send_client_state(self, new_state):
        LOG.debug(
            {
                "msg": "State change",
                "old_state": self.status,
                "new_state": new_state,
            }
        )
        self.status = new_state
        msg_type = {
            CLIENT_STATE_IDLE: MSG_TYPE_CLIENT_STATE_IDLE,
            CLIENT_STATE_RUNNING: MSG_TYPE_CLIENT_STATE_RUNNING,
        }.get(new_state)
        self.socket.send(
            {
                "type": msg_type,
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
        LOG.debug({"msg": "Local statistics", "statistics": stats_data})

    def _exec_send_status_dir_count(self):
        self.socket.send(
            {
                "type": MSG_TYPE_CLIENT_STATUS_DIR_COUNT,
                "data": self.dir_q_count,
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
            "num_threads",
            "max_work_items",
        ]:
            setattr(s, attrib, getattr(self, "scanner_" + attrib))
        s.exit_on_idle = False
        # TODO: Change how custom states, init and file handler work
        s.handler_custom_stats = user_handlers.custom_stats_handler
        s.handler_init_thread = user_handlers.init_thread
        s.handler_file = self.scanner_file_handler
        # TODO: Change how the user handler is passed in and initialized
        custom_state, custom_threads_state = s.get_custom_state()
        user_handlers.init_custom_state(custom_state, self.cli_options)

    def _process_args(self, args, use_default=True):
        cfg_fields = [
            ["dir_output_interval", "dir_output_interval", DEFAULT_DIR_OUTPUT_INTERVAL],
            ["dir_request_interval", "dir_request_interval", DEFAULT_DIR_REQUEST_INTERVAL],
            ["nodepool_translation", "nodepool_translation", {}],
            ["scanner_dir_chunk", "dirq_chunk", scanit.DEFAULT_QUEUE_DIR_CHUNK_SIZE],
            ["scanner_dir_priority_count", "dirq_priority", scanit.DEFAULT_DIR_PRIORITY_COUNT],
            ["scanner_dir_request_percent", "dirq_request_percentage", DEFAULT_DIRQ_REQUEST_PERCENTAGE],
            ["scanner_file_chunk", "fileq_chunk", scanit.DEFAULT_QUEUE_FILE_CHUNK_SIZE],
            ["scanner_file_q_cutoff", "fileq_cutoff", scanit.DEFAULT_FILE_QUEUE_CUTOFF],
            ["scanner_file_q_min_cutoff", "file_q_min_cutoff", scanit.DEFAULT_FILE_QUEUE_MIN_CUTOFF],
            # TODO: Changing the number of threads does not work dynamically at this time. The thread count is configured
            #     currently as a CLI option when ps_scan_server launches the client
            ["scanner_num_threads", "threads", DEFAULT_THREAD_COUNT],
            ["scanner_max_work_items", "max_work_items", DEFAULT_MAX_WORK_ITEMS_PER_REQUEST],
            ["stats_output_interval", "stats_interval", DEFAULT_STATS_OUTPUT_INTERVAL],
        ]
        for field in cfg_fields:
            def_val = field[2] if use_default else getattr(self, field[0])
            setattr(self, field[0], args.get(field[1], def_val))

    def connect(self):
        LOG.info(
            {
                "msg": "Connecting to server",
                "server": self.server_addr,
                "port": self.server_port,
            }
        )
        connected = self.socket.connect()
        if not connected:
            LOG.info({"msg": "Unable to connect to server"})
            return
        continue_running = True
        start_wall = time.time()
        self.scanner.start()
        # Send initial empty stats block to the server
        self._exec_send_status_stats(time.time())

        # Main client processing loop
        while continue_running:
            try:
                rlist, _, xlist = select.select(self.wait_list, [], self.wait_list, self.poll_interval)
                now = time.time()
                self.dir_q_count = self.scanner.get_dir_queue_size()
                if self.debug_count > 2:
                    LOG.debug({"msg": "Select call returned", "dir_q_count": self.dir_q_count})
                if rlist:
                    data = self.socket.recv()
                    msg_type = data.get("type")
                    if msg_type == MSG_TYPE_COMMAND:
                        cmd = data.get("cmd")
                        LOG.debug(
                            {
                                "msg": "Command received",
                                "command": cmd,
                            }
                        )
                        if cmd == "closed":
                            self.disconnect()
                            continue_running = False
                            continue
                    elif msg_type == "data":
                        msg_data = data.get("data")
                        response = self.parse_message(msg_data, now)
                        if response.get("cmd") == PS_CMD_QUIT:
                            self.disconnect()
                            continue_running = False
                            continue
                    elif msg_type is None:
                        LOG.debug({"msg": "Socket ready to read but no data was received. Shutting down"})
                        self.disconnect()
                        continue_running = False
                        continue
                    else:
                        LOG.debug({"msg": "Unexpected message received", "data": data})
                if xlist:
                    LOG.error({"msg": "Socket encountered an error or was closed"})
                    self.disconnect()
                    break

                # Determine if we should send a statistics update
                cur_stats_count = int((now - start_wall) // self.stats_output_interval)
                if cur_stats_count > self.stats_output_count:
                    self.stats_output_count = cur_stats_count
                    if self.debug_count > 1:
                        LOG.debug(
                            {
                                "msg": "Sending statistics update",
                                "stats_count": cur_stats_count,
                            }
                        )
                    self._exec_send_status_stats(now)

                # Determine if we should send a directory queue count update
                cur_dir_count = int((now - start_wall) // self.dir_output_interval)
                if cur_dir_count > self.dir_output_count:
                    if self.debug_count > 1:
                        if cur_dir_count != (self.dir_output_count + 1):
                            LOG.debug(
                                {
                                    "msg": "Dir count update jumped",
                                    "expected_dir_count": self.dir_output_count + 1,
                                    "calculated_dir_count": cur_dir_count,
                                }
                            )
                        LOG.debug(
                            {
                                "msg": "Sending dir count update",
                                "dir_update_count": cur_dir_count,
                                "dir_q_count": self.dir_q_count,
                            }
                        )
                    self.dir_output_count = cur_dir_count
                    self._exec_send_status_dir_count()

                # Ask server for more data if required, limit data requests to dir_request_interval seconds
                # We will request new directories even if we have files left to process to try and keep the scanner
                # from running out of work
                if (not self.dir_q_count) and ((now - self.want_data) > self.dir_request_interval):
                    self.want_data = now
                    self._exec_send_req_dir_list()

                # Check if the scanner is idle
                if (
                    not self.dir_q_count
                    and not self.scanner.get_file_queue_size()
                    and not self.scanner.is_processing()
                    and self.status != CLIENT_STATE_IDLE
                ):
                    self._exec_send_client_state(CLIENT_STATE_IDLE)
                    # Send a stats update whenever we go idle
                    self._exec_send_status_stats(now)
            except Exception as e:
                LOG.exception({"msg": "Unhandled exception", "exception": str(e)})
                self.disconnect()

    def disconnect(self):
        custom_state, custom_threads_state = self.scanner.get_custom_state()
        user_handlers.shutdown(custom_state, custom_threads_state)
        if self.scanner:
            self.scanner.terminate()
            self.scanner = None
        if self.socket in self.wait_list:
            self.wait_list.remove(self.socket)
        if self.socket:
            self.socket.disconnect()
            self.socket = None

    def dump_state(self):
        LOG.critical({"msg": "Dumping state" + "=" * 20})
        state = {}
        for member in [
            "client_config",
            "debug_count",
            "dir_output_count",
            "dir_output_interval",
            "dir_q_count",
            "dir_request_interval",
            "max_work_items",
            "poll_interval",
            "scanner_dir_chunk",
            "scanner_dir_priority_count",
            "scanner_dir_request_percent",
            "scanner_file_chunk",
            "scanner_file_q_cutoff",
            "scanner_file_q_min_cutoff",
            "scanner_num_threads",
            "sent_data",
            "server_addr",
            "server_port",
            "stats_output_count",
            "stats_output_interval",
            "status",
            "wait_list",
            "want_data",
        ]:
            state[member] = str(getattr(self, member))
        state["dir_q_size"] = self.scanner.get_dir_queue_size()
        state["file_q_size"] = self.scanner.get_file_queue_size()
        LOG.critical(json.dumps(state, indent=2, sort_keys=True, default=lambda o: "<not serializable>"))

    def parse_config_update(self, cfg):
        # TODO: Re-architect how config updates are sent to the user handler/plug-in
        custom_state, custom_threads_state = self.scanner.get_custom_state()
        LOG.debug(
            {
                "msg": "Received configuration update",
                "configuration": cfg,
            }
        )
        try:
            user_handlers.update_config(custom_state, cfg)
        except Exception as e:
            LOG.exception(
                {"msg": "Error during update_config", "exception": str(e)},
            )
        self.client_config = cfg
        self._process_args(cfg.get("cli_options", {}), use_default=False)
        self.config_update_count += 1

    def parse_config_update_log_level(self, cfg):
        log_level = cfg.get("log_level")
        if not log_level:
            LOG.error({"msg": "log_level missing from cfg while updating the log level"})
            return
        LOG.setLevel(log_level)

    def parse_config_update_logger(self, cfg):
        try:
            logger_block = cfg.get("logger")
            if not logger_block:
                return
            if logger_block["destination"] == "file":
                format_string_vars = {
                    "hostname": platform.node(),
                    "pid": os.getpid(),
                    "prefix": logger_block["prefix"],
                    "suffix": logger_block["suffix"],
                }
                log_filename = logger_block["filename"].format(**format_string_vars)
                log_handler = logging.FileHandler(log_filename)
            else:
                log_handler = logging.StreamHandler()
            log_handler.setFormatter(logging.Formatter(logger_block["format"]))
            rootlogger = logging.getLogger("")
            rootlogger.handlers[:] = []
            rootlogger.addHandler(log_handler)
            rootlogger.setLevel(logger_block["level"])
            self.debug_count = logger_block.get("debug_count", 0)
            LOG.debug(
                {
                    "msg": "Logger configuration updated",
                    "log_level": logger_block["level"],
                    "debug_count": self.debug_count,
                }
            )
        except KeyError as ke:
            sys.stderr.write("ERROR: Logger filename string is invalid: {txt}\n".format(txt=str(ke)))
        except Exception as e:
            sys.stderr.write("ERROR: Unhandled exception while trying to configure logger: {txt}\n".format(txt=str(ke)))

    def parse_message(self, msg, now):
        msg_type = msg.get("type")
        if msg_type == MSG_TYPE_CLIENT_DIR_LIST:
            work_items = msg.get("work_item")
            if not work_items:
                return
            LOG.debug(
                {
                    "msg": "Received: Work items to process",
                    "command": msg_type,
                    "work_items_count": len(work_items),
                }
            )
            self.scanner.add_scan_path(work_items)
            self.want_data = 0
            self.dir_q_count = self.scanner.get_dir_queue_size()
            if self.debug_count > 1:
                LOG.debug(
                    {
                        "msg": "Current work list count",
                        "dir_q_count": self.dir_q_count,
                    }
                )
            if self.status != CLIENT_STATE_RUNNING:
                self._exec_send_client_state(CLIENT_STATE_RUNNING)
        elif msg_type == MSG_TYPE_CLIENT_QUIT:
            LOG.debug(
                {
                    "msg": "Received: Quit message",
                    "command": msg_type,
                }
            )
            return {"cmd": PS_CMD_QUIT}
        elif msg_type == MSG_TYPE_CLIENT_REQ_DIR_LIST:
            dir_list = self.scanner.get_dir_queue_items(
                num_items=1, percentage=msg.get("pct", self.scanner_dir_request_percent)
            )
            if dir_list:
                # Update the number of directories we have queued after removing a chunk to return to the server
                self.dir_q_count = self.scanner.get_dir_queue_size()
                self._exec_send_dir_list(dir_list)
            LOG.debug(
                {
                    "msg": "Received: Return work items",
                    "command": msg_type,
                    "dir_q_count": self.dir_q_count,
                    "returned_items_count": len(dir_list),
                }
            )
        elif msg_type == MSG_TYPE_CONFIG_UPDATE:
            cfg = msg.get("config")
            LOG.debug(
                {
                    "msg": "Received: Configuration update",
                    "command": msg_type,
                    "update_items": cfg.keys(),
                }
            )
            if "logger" in cfg:
                self.parse_config_update_logger(cfg)
            if "log_level" in cfg:
                self.parse_config_update_log_level(cfg)
            if "client_config" in cfg:
                self.parse_config_update(cfg)
            if "client_id" in cfg:
                self.client_id = cfg["client_id"]
        elif msg_type == MSG_TYPE_DEBUG:
            dbg = msg.get("cmd")
            LOG.debug(
                {
                    "msg": "Received: Debug command",
                    "command": msg_type,
                    "debug_cmds": dbg.keys(),
                }
            )
            if "dump_state" in dbg:
                self.dump_state()
        else:
            LOG.debug(
                {
                    "msg": "Received: Unhandled message",
                    "command": msg_type,
                    "raw_msg": msg,
                }
            )
        return {}

    def stats_merge(self, now):
        scanner_stats = {}
        try:
            scanner_stats = self.scanner.get_stats()
            scanner_stats["threads"] = self.scanner.num_threads
        except Exception as e:
            LOG.exception({"msg": "Unable to get scanner stats", "exception": str(e)})
        return scanner_stats
