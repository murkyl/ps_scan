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
import helpers.misc as misc
import helpers.user_handlers as user_handlers
import libs.hydra as Hydra
import libs.scanit as scanit


LOG = logging.getLogger()


class PSScanClient(object):
    def __init__(self, args={}):
        """Initialize PSScanClient

        Parameters
        ----------
        args: dictionary
            dir_output_interval: int - Time in seconds between each directory queue update to the server
            dir_request_interval: int - Limit how often a client can request more work directories, in seconds
            poll_interval: int - Time in seconds to wait in the select statement
            scanner_file_handler: function pointer -
            scanner_dir_chunk: int -
            scanner_dir_priority_count: int -
            scanner_dir_request_percent: float - Percentage of the queued working directories to return per request
            scanner_file_chunk: int -
            scanner_file_q_cutoff: int -
            scanner_file_q_min_cutoff: int -
            scanner_num_threads: int - Number of threads the file scanner should use
            server_addr: str - IP/FQDN of the ps_scan server process
            server_port: int - Port to connect to the ps_scan server
            stats_interval: int - Time in seconds between each statistics update to the server
        """
        self.client_config = {}
        self.debug_count = args.get("debug", 0)
        self.dir_output_count = 0
        self.dir_output_interval = args.get("dir_output_interval", DEFAULT_DIR_OUTPUT_INTERVAL)
        self.dir_request_interval = args.get("dir_request_interval", DEFAULT_DIR_REQUEST_INTERVAL)
        self.poll_interval = args.get("poll_interval", DEFAULT_CMD_POLL_INTERVAL)
        self.scanner = scanit.ScanIt()
        # TODO: Change how file handler is called
        # TODO: Fix the args.get to match CLI options
        self.scanner_file_handler = args.get("scanner_file_handler", user_handlers.file_handler_basic)
        self.scanner_dir_chunk = args.get("scanner_dir_chunk", scanit.DEFAULT_QUEUE_DIR_CHUNK_SIZE)
        self.scanner_dir_priority_count = args.get("scanner_dir_priority_count", scanit.DEFAULT_DIR_PRIORITY_COUNT)
        self.scanner_dir_request_percent = args.get("scanner_dir_request_percent", DEFAULT_DIRQ_REQUEST_PERCENTAGE)
        self.scanner_file_chunk = args.get("scanner_file_chunk", scanit.DEFAULT_QUEUE_FILE_CHUNK_SIZE)
        self.scanner_file_q_cutoff = args.get("scanner_file_q_cutoff", scanit.DEFAULT_FILE_QUEUE_CUTOFF)
        self.scanner_file_q_min_cutoff = args.get("scanner_file_q_min_cutoff", scanit.DEFAULT_FILE_QUEUE_MIN_CUTOFF)
        self.scanner_num_threads = args.get("scanner_num_threads", DEFAULT_THREAD_COUNT)
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
        self.stats_output_interval = args.get("stats_interval", DEFAULT_STATS_OUTPUT_INTERVAL)
        self.status = CLIENT_STATE_STARTING
        self.wait_list = [self.socket]
        self.want_data = time.time()
        self.work_list_count = 0
        self._init_scanner()

    def _exec_send_dir_list(self, dir_list):
        self.socket.send(
            {
                "type": MSG_TYPE_CLIENT_DIR_LIST,
                "work_item": dir_list,
            }
        )

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
        if self.debug_count > 1:
            LOG.debug("Local statistics: {stats}".format(stats=stats_data))

    def _exec_send_status_dir_count(self):
        self.socket.send(
            {
                "type": MSG_TYPE_CLIENT_STATUS_DIR_COUNT,
                "data": self.work_list_count,
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
        ]:
            setattr(s, attrib, getattr(self, "scanner_" + attrib))
        s.exit_on_idle = False
        s.processing_type = scanit.PROCESS_TYPE_ADVANCED
        # TODO: Change how custom states, init and file handler work
        s.handler_custom_stats = user_handlers.custom_stats_handler
        s.handler_init_thread = user_handlers.init_thread
        s.handler_file = self.scanner_file_handler
        # TODO: Change how the user handler is passed in and initialized
        custom_state, custom_threads_state = s.get_custom_state()
        user_handlers.init_custom_state(custom_state)

    def connect(self):
        LOG.info("Connecting to server at {svr}:{port}".format(svr=self.server_addr, port=self.server_port))
        connected = self.socket.connect()
        if not connected:
            LOG.info("Unable to connect to server")
            return
        continue_running = True
        start_wall = time.time()
        self.scanner.start()
        # Send initial empty stats block to the server
        self._exec_send_status_stats(time.time())

        # Main client processing loop
        while continue_running:
            rlist, _, xlist = select.select(self.wait_list, [], self.wait_list, self.poll_interval)
            now = time.time()
            self.work_list_count = self.scanner.get_dir_queue_size()
            if rlist:
                data = self.socket.recv()
                msg_type = data.get("type")
                if msg_type == MSG_TYPE_COMMAND:
                    cmd = data.get("cmd")
                    LOG.debug("Command received: {cmd}".format(cmd=cmd))
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
                    LOG.debug("Socket ready to read but no data was received. We should shutdown now.")
                    self.disconnect()
                    continue_running = False
                    continue
                else:
                    LOG.debug("Unexpected message received: {data}".format(data=data))
            if xlist:
                LOG.error("Socket encountered an error or was closed")
                self.disconnect()
                continue_running = False
                break

            # Determine if we should send a statistics update
            cur_stats_count = (now - start_wall) // self.stats_output_interval
            if cur_stats_count > self.stats_output_count:
                if self.debug_count > 1:
                    LOG.debug("Sending stats update")
                self.stats_output_count = cur_stats_count
                self._exec_send_status_stats(now)

            # Determine if we should send a directory queue count update
            cur_dir_count = (now - start_wall) // self.dir_output_interval
            if cur_dir_count > self.dir_output_count:
                if self.debug_count > 1:
                    LOG.debug("Sending dir count update #{dir_count:,d}".format(dir_count=cur_dir_count))
                self.dir_output_count = cur_dir_count
                self._exec_send_status_dir_count()

            # Ask parent process for more data if required, limit data requests to dir_request_interval seconds
            if (self.work_list_count == 0) and (now - self.want_data > self.dir_request_interval):
                if self.debug_count > 1:
                    LOG.debug(
                        "Asking server for more work: file_queue_size:{fq_size}".format(
                            fq_size=self.scanner.get_file_queue_size()
                        )
                    )
                self.want_data = now
                self._exec_send_req_dir_list()

            # Check if the scanner is idle
            if (
                not self.work_list_count
                and not self.scanner.get_file_queue_size()
                and not self.scanner.is_processing()
                and self.status != CLIENT_STATE_IDLE
            ):
                LOG.debug(
                    "old_state:{old_state}, new_state:{new_state}".format(
                        new_state=CLIENT_STATE_IDLE, old_state=self.status
                    )
                )
                self.status = CLIENT_STATE_IDLE
                self._exec_send_client_state_idle()
                # Send a stats update whenever we go idle
                self._exec_send_status_stats(now)

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
        LOG.critical("\nDumping state\n" + "=" * 20)
        state = {}
        for member in [
            "client_config",
            "debug_count",
            "dir_output_count",
            "dir_output_interval",
            "dir_request_interval",
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
            "work_list_count",
        ]:
            state[member] = str(getattr(self, member))
        state["dir_q_size"] = self.scanner.get_dir_queue_size()
        state["file_q_size"] = self.scanner.get_file_queue_size()
        LOG.critical(json.dumps(state, indent=2, sort_keys=True, default=lambda o: "<not serializable>"))

    def parse_config_update(self, cfg):
        # TODO: Re-architect how config updates are sent to the user handler/plug-in
        custom_state, custom_threads_state = self.scanner.get_custom_state()
        LOG.debug("Received configuration update: {cfg}".format(cfg=cfg))
        try:
            user_handlers.update_config(custom_state, cfg)
        except Exception as e:
            LOG.exception("Error during update_config")
        self.client_config = cfg
        self.debug_count = cfg.get("debug", 0)

    def parse_config_update_log_level(self, cfg):
        log_level = cfg.get("log_level")
        if not log_level:
            LOG.error("log_level missing from cfg while updating the log level.")
            return
        LOG.setLevel(log_level)

    def parse_config_update_logger(self, cfg):
        try:
            logger_block = cfg.get("logger")
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
            LOG.handlers[:] = []
            LOG.addHandler(log_handler)
            LOG.setLevel(logger_block["level"])
            self.debug_count = logger_block.get("debug_count", 0)
            LOG.debug("Logger configuration updated")
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
                "{cmd}: Received {count} work items to process".format(
                    cmd=msg_type,
                    count=len(work_items),
                )
            )
            self.scanner.add_scan_path(work_items)
            self.want_data = 0
            self.work_list_count = self.scanner.get_dir_queue_size()
            if self.status != CLIENT_STATE_RUNNING:
                LOG.debug(
                    "old_state:{old_state}, new_state:{new_state}".format(
                        new_state=CLIENT_STATE_RUNNING, old_state=self.status
                    )
                )
                self.status = CLIENT_STATE_RUNNING
                self._exec_send_client_state_running()
        elif msg_type == MSG_TYPE_CLIENT_QUIT:
            return {"cmd": PS_CMD_QUIT}
        elif msg_type == MSG_TYPE_CLIENT_REQ_DIR_LIST:
            dir_list = self.scanner.get_dir_queue_items(
                num_items=1, percentage=msg.get("pct", self.scanner_dir_request_percent)
            )
            if dir_list:
                self._exec_send_dir_list(dir_list)
            LOG.debug(
                "{cmd}: Asked to return work items. Returning {count} items.".format(
                    cmd=msg_type,
                    count=len(dir_list),
                )
            )
        elif msg_type == MSG_TYPE_CONFIG_UPDATE:
            cfg = msg.get("config")
            if "logger" in cfg:
                self.parse_config_update_logger(cfg)
            if "log_level" in cfg:
                self.parse_config_update_log_level(cfg)
            if "client_config" in cfg:
                self.parse_config_update(cfg)
        elif msg_type == MSG_TYPE_DEBUG:
            dbg = msg.get("cmd")
            if "dump_state" in dbg:
                self.dump_state()
        else:
            LOG.debug("Unhandled message: {msg}".format(msg=msg))
        return {}

    def stats_merge(self, now):
        scanner_stats = {}
        try:
            scanner_stats = self.scanner.get_stats()
        except Exception as e:
            LOG.exception("Unable to get scanner stats.")
        return scanner_stats
