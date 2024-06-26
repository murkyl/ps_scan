#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
PowerScale file scanner server
"""
# fmt: off
__title__         = "ps_scan_server"
__version__       = "0.1.0"
__date__          = "15 August 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
__all__ = [
    "PSScanServer",
]
# fmt: on
import datetime
import json
import logging
import os
try:
    import queue
except:
    import Queue as queue
import signal
import sys
import time

from helpers.constants import *
import helpers.misc as misc
import libs.hydra as Hydra
from libs.onefs_become_user import become_user
import libs.remote_run as rr
import libs.sliding_window_stats as sliding_window_stats


LOG = logging.getLogger(__name__)


class PSScanServer(Hydra.HydraServer):
    def __init__(self, args={}):
        """Initialize PSScanClient

        Parameters
        ----------
        args: dictionary
            cli_options: dict - All command line options
                queue_timeout: int - Number of seconds to wait for new messages before continuing with the processing
                        loop
                request_work_interval: int - Number of seconds between requests to a client to return work
                request_work_percentage: float - Percentage of unprocessed dirs that a client should return when the
                        server requests a client to return work items
                stats_interval: int - Number of seconds between each statistics update
            client_config: dict - Dictionary sent to clients during a configuration update command
            node_list: list - List of clients to auto-start. Format of each entry is in remote_run module
            scan_path: list - List of paths to scan
            script_path: str - Full path to script to run on clients
            server_addr: str - IP address to listen for clients on
            server_connect_addr:str - FQDN/IP that clients should use to connect
            server_port: int - Port number for server to listen on
            socket_listen_queue: int - Number of pending clients for a socket
            stats_handler: function - Function to call to output final statistics
        """
        args["async_server"] = True
        # The super class consumes the following arguments:
        # server_addr
        # server_port
        # socket_listen_queue
        super(PSScanServer, self).__init__(args=args)
        self.cli_options = args.get("cli_options", {})
        self.client_config = args.get("client_config", {})
        self.client_count = 0
        self.client_state = {}
        self.connect_addr = args.get("server_connect_addr", None)
        self.debug_count = self.cli_options.get("debug", 0)
        self.max_work_items = self.cli_options.get("max_work_items", DEFAULT_MAX_WORK_ITEMS_PER_REQUEST)
        self.msg_q = queue.Queue()
        self.node_list = args.get("node_list", None)
        self.queue_timeout = self.cli_options.get("queue_timeout", DEFAULT_QUEUE_TIMEOUT)
        self.remote_state = None
        self.request_work_interval = self.cli_options.get("request_work_interval", DEFAULT_REQUEST_WORK_INTERVAL)
        self.request_work_percentage = self.cli_options.get("request_work_percentage", DEFAULT_DIRQ_REQUEST_PERCENTAGE)
        self.script_path = args.get("script_path", None)
        self.stats = {}
        self.stats_fps_window = sliding_window_stats.SlidingWindowStats(STATS_FPS_BUCKETS)
        self.stats_handler = args.get("stats_handler", lambda *x: None)  # TODO: Temporary until stats are moved
        self.stats_last_files_processed = 0
        self.stats_output_count = 0
        self.stats_output_interval = self.cli_options.get("stats_interval", DEFAULT_STATS_OUTPUT_INTERVAL)
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

    def _exec_send_config_update(self, client, client_idx=""):
        client_logger = None
        if self.cli_options.get("log"):
            base, ext = os.path.splitext(self.cli_options.get("log", DEFAULT_LOG_FILE_PREFIX + DEFAULT_LOG_FILE_SUFFIX))
            client_logger = {
                "format": "%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d][%(threadName)s] - %(message)s",
                "debug_count": self.cli_options.get("debug", 0),
                "destination": "file",
                "filename": self.cli_options.get("log_file_format", DEFAULT_LOG_FILE_FORMAT),
                "level": LOG.getEffectiveLevel(),
                "prefix": DEFAULT_LOG_FILE_CLIENT_PREFIX + base,
                "suffix": ext,
            }
        self.send(
            client,
            {
                "type": MSG_TYPE_CONFIG_UPDATE,
                "config": {
                    "client_id": client_idx,
                    "cli_options": self.cli_options,
                    "client_config": self.client_config,
                    "logger": client_logger,
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
            LOG.error({"msg": "Work queue was not empty but unable to get a work item to send to the new client"})
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
                "pct": self.request_work_percentage,
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
        LOG.critical({"msg": "Dumping state\n" + "=" * 20})
        state = {}
        for member in [
            "client_count",
            "client_state",
            "connect_addr",
            "max_work_items",
            "node_list",
            "queue_timeout",
            "remote_state",
            "request_work_interval",
            "request_work_percentage",
            "script_path",
            "stats_last_files_processed",
            "stats_output_count",
            "stats_output_interval",
            "work_list",
        ]:
            state[member] = str(getattr(self, member))
        state["stats_fps_window"] = self.stats_fps_window.get_all_windows()
        LOG.critical(json.dumps(state, indent=2, sort_keys=True, default=lambda o: "<not serializable>"))

    def get_stats(self):
        return self.stats

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
        LOG.debug({"msg": "SIGINT signal received. Quitting program."})
        self.msg_q.put({"type": MSG_TYPE_QUIT})

    def handler_signal_usr1(self, signum, frame):
        LOG.debug({"msg": "SIGUSR1 signal received. Toggling debug."})
        self._exec_toggle_debug()

    def handler_signal_usr2(self, signum, frame):
        LOG.debug({"msg": "SIGUSR2 signal received. Dumping state."})
        self._exec_dump_state()

    def launch_remote_processes(self):
        run_cmd = [
            "python",
            self.script_path,
            "--op",
            "client",
            "--port",
            str(self.server_port),
            "--addr",
            self.connect_addr,
            "--threads",
            self.cli_options.get("threads"),
            "--type",
            self.cli_options.get("type"),
        ]
        if self.cli_options.get("es_type"):
            run_cmd.append("--es-type")
            run_cmd.append(self.cli_options.get("es_type"))
        if self.cli_options.get("log"):
            run_cmd.append("--log")
            run_cmd.append(DEFAULT_LOG_FILE_CLIENT_PREFIX + self.cli_options["log"])
        if self.cli_options.get("skip_dir_file"):
            run_cmd.append("--skip-dir-file")
            run_cmd.append(self.cli_options.get("skip_dir_file"))
        if self.node_list:
            for node in self.node_list:
                if node.get("type") != "default":
                    node["cmd"] = run_cmd
            LOG.debug({"msg": "Launching remote process", "shell_command": run_cmd})
            self.remote_state = rr.RemoteRun({"callback": self.remote_callback})
            self.remote_state.connect(self.node_list)
        LOG.debug({"msg": "All remote processes launched"})

    def output_statistics(self, now, start_wall):
        # TODO: Some external function should do all stats output including custom stats
        self.stats = misc.merge_process_stats(self.client_state) or {}
        self.stats["total_time"] = now - start_wall
        self.stats["end_time"] = 0
        custom_stats = [x["stats"].get("custom", {}) for x in self.client_state.values()]
        new_files_processed = self.stats.get("files_processed", self.stats_last_files_processed)
        self.stats_fps_window.add_sample(new_files_processed - self.stats_last_files_processed)
        self.stats_last_files_processed = new_files_processed
        self.print_statistics_interim(
            self.stats,
            now,
            start_wall,
            self.stats_fps_window,
            self.stats_output_interval,
        )
        self.stats_handler(
            "interim",
            LOG,
            self.stats,
            custom_stats,
            now,
            start_wall,
            0,
            self.stats_output_interval,
        )

    def output_statistics_final(self, now, total_time):
        # TODO: Some external function should do all stats output including custom stats
        self.stats = misc.merge_process_stats(self.client_state) or {}
        self.stats["total_time"] = total_time
        self.stats["end_time"] = now
        custom_stats = [x["stats"].get("custom", {}) for x in self.client_state.values()]
        self.print_statistics_final(self.stats, self.client_count, total_time)
        self.stats_handler(
            "final",
            LOG,
            self.stats,
            custom_stats,
            now,
            0,
            total_time,
            self.stats_output_interval,
        )

    def parse_message(self, msg, now):
        if msg["type"] == MSG_TYPE_CLIENT_DATA:
            client_idx = msg["client"]
            cur_client = self.client_state.get(client_idx)
            data = msg["data"]
            cid = cur_client["id"]
            data_type = data.get("type")
            if data_type == "cmd":
                cmd = data.get("cmd")
                LOG.debug({"msg": "Received command", "cid": cid, "command": cmd})
                if cmd == PS_CMD_QUIT:
                    return {"cmd": PS_CMD_QUIT}
                elif cmd == PS_CMD_DUMPSTATE:
                    self._exec_dump_state()
                elif cmd == PS_CMD_TOGGLEDEBUG:
                    self._exec_toggle_debug()
                else:
                    LOG.error({"msg": "Unknown command", "cid": cid, "command": cmd})
            elif data_type == MSG_TYPE_CLIENT_DIR_LIST:
                LOG.debug(
                    {
                        "msg": "Client returned work directories",
                        "cid": cid,
                        "work_dir_count": len(data["work_item"]),
                        "dir_q_count": data.get("dir_q_count", "Unknown"),
                        "command": data_type,
                    }
                )
                cur_client["sent_data"] = now
                cur_client["want_data"] = 0
                # Extend directory work list with items returned by the client
                self.work_list.extend(data["work_item"])
            elif data_type == MSG_TYPE_CLIENT_STATE_IDLE:
                LOG.debug(
                    {
                        "msg": "State change",
                        "cid": cid,
                        "old_state": cur_client["status"],
                        "new_state": CLIENT_STATE_IDLE,
                        "command": data_type,
                    }
                )
                cur_client["status"] = CLIENT_STATE_IDLE
                cur_client["want_data"] = now
            elif data_type == MSG_TYPE_CLIENT_STATE_RUNNING:
                LOG.debug(
                    {
                        "msg": "State change",
                        "cid": cid,
                        "old_state": cur_client["status"],
                        "new_state": CLIENT_STATE_RUNNING,
                        "command": data_type,
                    }
                )
                cur_client["status"] = CLIENT_STATE_RUNNING
                cur_client["want_data"] = 0
            elif data_type == MSG_TYPE_CLIENT_STATE_STOPPED:
                LOG.debug(
                    {
                        "msg": "State change",
                        "cid": cid,
                        "old_state": cur_client["status"],
                        "new_state": CLIENT_STATE_STOPPED,
                        "command": data_type,
                    }
                )
                cur_client["status"] = CLIENT_STATE_STOPPED
                cur_client["want_data"] = 0
            elif data_type == MSG_TYPE_CLIENT_STATUS_DIR_COUNT:
                if self.debug_count > 1:
                    LOG.debug(
                        {
                            "msg": "Client work directory queue size",
                            "cid": cid,
                            "dir_count": data["data"],
                            "command": data_type,
                        }
                    )
                cur_client["dir_count"] = data["data"]
            elif data_type == MSG_TYPE_CLIENT_STATUS_STATS:
                if self.debug_count > 1:
                    LOG.debug(
                        {
                            "msg": "Statistics update",
                            "cid": cid,
                            "command": data_type,
                        }
                    )
                cur_client["stats"] = data["data"]
                cur_client["stats_time"] = now
            elif data_type == MSG_TYPE_CLIENT_REQ_DIR_LIST:
                if self.debug_count > 1:
                    LOG.debug(
                        {
                            "msg": "Requested work directories",
                            "cid": cid,
                            "command": data_type,
                        }
                    )
                cur_client["want_data"] = now
            else:
                LOG.error(
                    {
                        "msg": "Unknown command",
                        "cid": cid,
                        "command": data_type,
                    }
                )
        elif msg["type"] == MSG_TYPE_CLIENT_CLOSED:
            cur_client = self.client_state.get(msg["client"])
            LOG.debug(
                {
                    "msg": "Socket closed",
                    "cid": cur_client["id"],
                    "data": msg,
                }
            )
            cur_client["dir_count"] = 0
            cur_client["sent_data"] = 0
            cur_client["status"] = CLIENT_STATE_STOPPED
            cur_client["want_data"] = 0
        elif msg["type"] == MSG_TYPE_CLIENT_CONNECT:
            client_idx = msg["client"]
            self.client_count += 1
            LOG.debug(
                {
                    "msg": "Socket connected",
                    "cid": self.client_count,
                    "data": msg,
                }
            )
            state_obj = {
                "client": client_idx,
                "dir_count": 0,
                "id": self.client_count,
                "sent_data": now - self.request_work_interval,
                "stats": {},
                "stats_time": None,
                "status": CLIENT_STATE_STARTING,
                "want_data": now,
            }
            # Send configuration to client
            self._exec_send_config_update(client_idx, self.client_count)
            # Send up to 1 directory in our work queue to each connected client
            work_sent = self._exec_send_one_work_item(client_idx)
            self.client_state[client_idx] = state_obj
        elif msg["type"] == MSG_TYPE_QUIT:
            LOG.debug({"msg": "Received internal quit command"})
            return {"cmd": PS_CMD_QUIT}
        elif msg["type"] == MSG_TYPE_REMOTE_CALLBACK:
            # This type of command is sent from the remote_run module that handles spawning processes on other machines
            # TODO: Add code to handle re-launching dead processes
            # TODO: Log any console output if there is an error
            LOG.debug(
                {
                    "msg": "Remote process message from client",
                    "cid": msg["client"],
                    "data": msg,
                }
            )
        else:
            LOG.debug({"msg": "Unhandled message received", "data": msg})
        return {}

    def print_statistics_final(self, stats, num_clients, wall_time):
        weight = (self.client_count * self.cli_options["threads"]) or 1
        output_string = """Final statistics
        Wall time (s): {wall_tm}
        Average file/dir queue wait time (s): {avg_q_tm:,.2f}
        Average time spent in dir/file handler routines across all clients (s): {d_htime:,.2f} / {f_htime:,.2f}
        Average dir scan time (s): {d_scan:,.1f}
        Processed/Queued/Skipped dirs: {d_proc:,d} / {d_queued:,d} / {d_skip:,d}
        Processed/Queued/Skipped files: {f_proc:,d} / {f_queued:,d} / {f_skip:,d}
        Total file size/physical size: {f_bytes:,d} ({f_bytes_sz})/ {f_phys_bytes:,d} ({f_phys_bytes_sz})
        Avg files/second: {fps:,.1f}
""".format(
            avg_q_tm=stats.get("q_wait_time", 0) / weight,
            d_htime=stats.get("dir_handler_time", 0) / weight,
            d_proc=stats.get("dirs_processed", 0),
            d_queued=stats.get("dirs_queued", 0),
            d_scan=stats.get("dir_scan_time", 0) / weight,
            d_skip=stats.get("dirs_skipped", 0),
            f_bytes=stats.get("file_size_total", 0),
            f_bytes_sz=misc.humanize_number(stats.get("file_size_total", 0), base=2),
            f_htime=stats.get("file_handler_time", 0) / weight,
            f_phys_bytes=stats.get("file_size_physical_total", 0),
            f_phys_bytes_sz=misc.humanize_number(stats.get("file_size_physical_total", 0), base=2),
            f_proc=stats.get("files_processed", 0),
            f_queued=stats.get("files_queued", 0),
            f_skip=stats.get("files_skipped", 0),
            fps=(stats.get("files_processed", 0) + stats.get("files_skipped", 0)) / wall_time,
            wall_tm=misc.humanize_seconds(wall_time),
        )
        LOG.info(output_string)
        sys.stdout.write(output_string)

    def print_statistics_interim(self, stats, now, start, fps_window, interval):
        buckets = [str(x) for x in fps_window.get_window_sizes()]
        fps_per_bucket = ["{fps:,.1f}".format(fps=x / interval) for x in fps_window.get_all_windows()]
        weight = (self.client_count * self.cli_options["threads"]) or 1
        output_string = """{ts} - Statistics:
        Current run time (s): {runtime}
        FPS overall / recent ({fps_buckets}) {interval}s intervals: {fps:,.1f} / {fps_per_bucket}
        Total file bytes processed / physical bytes: {f_bytes:,d} ({f_bytes_sz}) / {f_phys_bytes:,d} ({f_phys_bytes_sz})
        Files (Processed/Queued/Skipped): {f_proc:,d} / {f_queued:,d} / {f_skip:,d}
        File Q Size/Handler time: {f_q_size:,d} / {f_htime:,.1f}
        Dir scan time/Avg Q wait time: {d_scan:,.1f} / {avg_q_tm:,.1f}
        Dirs (Processed/Queued/Skipped): {d_proc:,d} / {d_queued:,d} / {d_skip:,d}
        Dir Q Size/Handler time: {d_q_size:,d} / {d_htime:,.1f}
""".format(
            avg_q_tm=stats.get("q_wait_time", 0) / weight,
            d_htime=stats.get("dir_handler_time", 0) / weight,
            d_proc=stats.get("dirs_processed", 0),
            d_q_size=stats.get("dir_q_size", 0),
            d_queued=stats.get("dirs_queued", 0),
            d_scan=stats.get("dir_scan_time", 0) / weight,
            d_skip=stats.get("dirs_skipped", 0),
            f_bytes=stats.get("file_size_total", 0),
            f_bytes_sz=misc.humanize_number(stats.get("file_size_total", 0), base=2),
            f_htime=stats.get("file_handler_time", 0) / weight,
            f_phys_bytes=stats.get("file_size_physical_total", 0),
            f_phys_bytes_sz=misc.humanize_number(stats.get("file_size_physical_total", 0), base=2),
            f_proc=stats.get("files_processed", 0),
            f_q_size=stats.get("file_q_size", 0),
            f_queued=stats.get("files_queued", 0),
            f_skip=stats.get("files_skipped", 0),
            fps=(stats.get("files_processed", 0) + stats.get("files_skipped", 0)) / (now - start),
            fps_buckets=", ".join(buckets),
            fps_per_bucket=" | ".join(fps_per_bucket),
            interval=self.stats_output_interval,
            runtime=misc.humanize_seconds(int(now - start)),
            ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        LOG.info(output_string)
        sys.stdout.write(output_string)

    def remote_callback(self, client, client_id, msg=None):
        self.msg_q.put({"type": MSG_TYPE_REMOTE_CALLBACK, "data": msg, "client_id": client_id, "client": client})

    def serve(self):
        LOG.info({"msg": "Starting server"})
        start_wall = time.time()
        # TODO: Next line should move to ps_scan to fully support become_user
        self.launch_remote_processes()
        self.start()

        # Start main processing loop
        # Wait for clients to connect, request work, and redistribute work as needed.
        continue_running = True
        while continue_running:
            now = time.time()
            try:
                queue_item = self.msg_q.get(timeout=self.queue_timeout)
            except queue.Empty as qe:
                queue_item = None
            except Exception as e:
                LOG.exception({"msg": "Unhandled exception getting item from msg_q", "exception": str(e)})
                continue_running = False
                continue
            else:
                try:
                    response = self.parse_message(queue_item, now)
                    if response.get("cmd") == PS_CMD_QUIT:
                        continue_running = False
                        continue
                except Exception as e:
                    # parse_message should handle exceptions. Any uncaught exceptions should terminate the program.
                    LOG.exception({"msg": "Unhandled exception parsing message", "exception": str(e)})
                    continue_running = False
                    continue

            try:
                # Output statistics
                #   The -2 is for a 2 second offset to allow time for stats to come from processes
                cur_stats_count = (now - start_wall - 2) // self.stats_output_interval
                if cur_stats_count > self.stats_output_count:
                    if self.debug_count > 1:
                        if cur_stats_count != (self.stats_output_count + 1):
                            LOG.debug(
                                {
                                    "msg": "Statistics count jumped",
                                    "expected_stats_count": self.stats_output_count + 1,
                                    "calculated_stats_count": cur_stats_count,
                                }
                            )
                    self.stats_output_count = cur_stats_count
                    self.output_statistics(now, start_wall)

                # TODO: Look at this loop and see if we can short circuit some of it to make the loop faster and output less junk
                # Check all our client states to gather which are idle, which have work dirs, and which want work
                continue_running = False
                idle_clients = 0
                have_dirs_clients = []
                want_work_clients = []
                client_keys = self.client_state.keys()
                for key in client_keys:
                    client = self.client_state[key]
                    if not continue_running and client["status"] != CLIENT_STATE_STOPPED:
                        continue_running = True
                    if client["status"] in (CLIENT_STATE_IDLE, CLIENT_STATE_STOPPED):
                        idle_clients += 1
                    # Check if we need to request or send any directories to existing processes
                    if client["want_data"] and not client["dir_count"]:
                        want_work_clients.append(key)
                    # Any processes that have directories are checked
                    if client["dir_count"]:
                        have_dirs_clients.append(key)
                if not continue_running and self.work_list:
                    # If there are no connected clients and there is work to do then continue running
                    continue_running = True

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
                    got_work_clients = []
                    len_dir_list = len(self.work_list)
                    len_want_work_clients = len(want_work_clients)
                    increment = (len_dir_list // len_want_work_clients) + (
                        1 * (len_dir_list % len_want_work_clients != 0)
                    )
                    # Cap the number of directories sent to any client in a single request
                    if increment > self.max_work_items:
                        increment = self.max_work_items
                    if self.debug_count > 1:
                        LOG.debug(
                            {
                                "msg": "Server has work items and clients want work",
                                "work_items_count": len_dir_list,
                                "want_work_clients_count": len_want_work_clients,
                                "increment": increment,
                            }
                        )
                    index = 0
                    for client_key in want_work_clients:
                        work_dirs = self.work_list[index : index + increment]
                        if not work_dirs:
                            continue
                        LOG.debug(
                            {
                                "msg": "Sending work to client",
                                "cid": self.client_state[client_key]["id"],
                                "work_item_count": len(work_dirs),
                            }
                        )
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
                    if self.debug_count > 1:
                        LOG.debug(
                            {
                                "msg": "Checking if server can request clients to return work",
                                "want_work_clients": ",".join(
                                    sorted([str(self.client_state[x]["id"]) for x in want_work_clients])
                                ),
                                "have_work_clients": ",".join(
                                    sorted([str(self.client_state[x]["id"]) for x in have_dirs_clients])
                                ),
                            }
                        )
                    for client_key in have_dirs_clients:
                        client = self.client_state[client_key]
                        if self.debug_count > 1:
                            LOG.debug(
                                {
                                    "msg": "Client could return work",
                                    "cid": client["id"],
                                }
                            )
                        # Limit the number of times we request data from each client to request_work_interval seconds
                        if (now - client["sent_data"]) > self.request_work_interval:
                            if self.debug_count > 1:
                                LOG.debug(
                                    {
                                        "msg": "Sending client request to return work",
                                        "cid": client["id"],
                                    }
                                )
                            self._exec_send_req_dir_list(client_key)
                            client["sent_data"] = now
            except Exception as e:
                LOG.exception({"msg": "Exception while in server loop", "exception": str(e)})
                continue_running = False
        total_wall_time = time.time() - start_wall
        self.output_statistics_final(now, total_wall_time)
        LOG.info({"msg": "{prog} shutting down.".format(prog=__title__)})
        self.shutdown()
        LOG.info({"msg": "{prog} shutdown complete.".format(prog=__title__)})

    def shutdown(self):
        super(PSScanServer, self).shutdown()
        if self.remote_state:
            self.remote_state.terminate()
