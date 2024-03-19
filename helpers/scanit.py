#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
Module description here
"""
# fmt: off
__title__         = "scanit"
__version__       = "1.0.0"
__date__          = "10 April 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
__all__ = [
    "ScanIt",
    "TerminateThread",
]
# fmt: on
import copy
import csv
import errno
import glob
import logging
import math
import multiprocessing
import os
import platform
import queue
import re
import stat
import threading
import time

import helpers.scanner as scanner
from helpers.constants import *

try:
    dir(PermissionError)
except:
    PermissionError = NotImplementedError
try:
    dir(os.scandir)
    USE_SCANDIR = 1
except:
    USE_SCANDIR = 0


def next_int(reset=False):
    global _next_int_state_
    if reset:
        _next_int_state_ = 0
    _next_int_state_ += 1
    return _next_int_state_


_next_int_state_ = 0
# ================================================================================
# Command name enumeration
CMD_PROC_CFILE = next_int(True)
CMD_PROC_FILE = next_int()
CMD_PROC_DIR = next_int()
CMD_EXIT = next_int()
# ================================================================================
# Default values
#   Size of the underlying storage block size
DEFAULT_BLOCK_SIZE = 8192
#   Number of threads that are allowed to walk directories out of the total thread count
DEFAULT_DIR_PRIORITY_COUNT = 2
#   When there are more than FILE_QUEUE_CUTOFF files chunks in the file_q, only process files
#   The number of total file names in the queue is DEFAULT_FILE_QUEUE_CUTOFF*DEFAULT_QUEUE_FILE_CHUNK_SIZE
DEFAULT_FILE_QUEUE_CUTOFF = 2000
#   When there are less than FILE_QUEUE_MIN_CUTOFF files in the file_q, just process directories
DEFAULT_FILE_QUEUE_MIN_CUTOFF = DEFAULT_FILE_QUEUE_CUTOFF // 10
#   Maximum number of work items to return in a single call to return work
DEFAULT_MAX_WORK_ITEMS = 5000
DEFAULT_POLL_INTERVAL = DEFAULT_CMD_POLL_INTERVAL
#   Size of directory or file chunks
DEFAULT_QUEUE_DIR_CHUNK_SIZE = 5
DEFAULT_QUEUE_FILE_CHUNK_SIZE = 50
#   Wait up to 5 seconds to put or get items from a queue
DEFAULT_QUEUE_MAX_TIMEOUT = 5
#   Wait 0.01 seconds when polling
DEFAULT_QUEUE_WAIT_TIMEOUT = 0.01
#   Number of times we poll quickly before starting to back off based on the DEFAULT_QUEUE_WAIT_TIMEOUT
DEFAULT_QUEUE_WAIT_TIMEOUT_COUNTDOWN = 5
#   Directory names to skip processing
DEFAULT_SKIP_DIRS = set(
    [
        ".ifsvar",
        ".snapshot",
    ]
)
#   Default number of threads to use for scanning
DEFAULT_THREAD_COUNT = DEFAULT_DIR_PRIORITY_COUNT + 8
# ================================================================================
# Thread state enumerations
S_IDLE = next_int(True)
S_STARTING = next_int()
S_RUNNING = next_int()
# ================================================================================
# Constants used for text output
T_CMD_EXIT = next_int(True)
T_EX_PROCESS_FILE = next_int()
T_EX_PROCESS_GENERAL = next_int()
T_EX_PROCESS_NEW_DIR = next_int()
T_EXIT = next_int()
T_INCONSISTENT_THREAD_STATE = next_int()
T_JOIN_THREADS = next_int()
T_LIST_DIR_NOT_FOUND = next_int()
T_LIST_DIR_PERM_ERR = next_int()
T_LIST_DIR_UNKNOWN_ERR = next_int()
T_OOM_PROCESS_FILE = next_int()
T_OOM_PROCESS_GENERAL = next_int()
T_OOM_PROCESS_NEW_DIR = next_int()
T_Q_FULL_PUT = next_int()
T_SEND_TERM_THREAD = next_int()
T_START_THREAD = next_int()
T_CLOSE_CSV_HANDLE = next_int()
TXT_STR = {
    T_CMD_EXIT: "({tid}) Thread receieved exit command. Setting run state to idle and breaking out of process loop",
    T_EX_PROCESS_FILE: "({tid}) - Exception in file handler for root: {r}",
    T_EX_PROCESS_GENERAL: "({tid}) - Exception found in thread handler: {ex}",
    T_EX_PROCESS_NEW_DIR: "({tid}) - Exception in _process_new_dir on directory: {r}/{d}",
    T_EXIT: "({tid}) Thread exiting",
    T_INCONSISTENT_THREAD_STATE: "({tid}) Dead thread did not set run state to idle. Forcing idle status.",
    T_JOIN_THREADS: "Joining threads",
    T_LIST_DIR_NOT_FOUND: "Directory not found {file}",
    T_LIST_DIR_PERM_ERR: "Permission error listing directory: {file}",
    T_LIST_DIR_UNKNOWN_ERR: "Unhandled exception error while listing directory: {file}",
    T_OOM_PROCESS_FILE: "({tid}) - Out of memory in file handler for root: {r}",
    T_OOM_PROCESS_GENERAL: "({tid}) - Out of memory in thread handler",
    T_OOM_PROCESS_NEW_DIR: "({tid}) - Out of memory in _process_new_dir on directory: {r}/{d}",
    T_Q_FULL_PUT: "Unable to PUT an item onto the queue: {q}",
    T_SEND_TERM_THREAD: "Sending thread terminate to: {tid}",
    T_START_THREAD: "Starting thread: {tid}",
    T_CLOSE_CSV_HANDLE: "Closing CSV output file for thread: {tid}",
}
# ================================================================================
LOG = logging.getLogger(__name__)
STANDARD_STATS_FIELD_NAMES = [
    "dir_scan_time",
    "dir_handler_time",
    "dirs_processed",
    "dirs_queued",
    "dirs_skipped",
    "es_queue_wait_count",
    "file_handler_time",
    "file_size_total",
    "file_size_physical_total",
    "files_not_found",
    "files_processed",
    "files_queued",
    "files_skipped",
    "q_wait_time",
    "time_es_queue",
]


class TerminateThread(Exception):
    "Raised when an inner loop needs the running thread to terminate"
    pass


class ScanIt(threading.Thread):
    def __init__(self, *args, **kwargs):
        super(ScanIt, self).__init__(args=args, kwargs=kwargs)
        self.common_stats = self._create_stats_state()
        self.block_size = DEFAULT_BLOCK_SIZE
        self.custom_state = {}
        self.daemon = True
        # process_alive is set to -1 during initialization and then set to True after all threads are initialized
        self.process_alive = -1
        self.run_start = None
        self.threads_state = []
        self.dir_q = queue.Queue()
        self.dir_q_lock = threading.Lock()
        self.dir_q_threads_count = 0
        self.file_q = queue.Queue()
        # User configurable parameters
        # The number of directories in each work block
        self.dir_chunk = DEFAULT_QUEUE_DIR_CHUNK_SIZE
        # The maximum number of threads working on processing directories
        self.dir_priority_count = DEFAULT_DIR_PRIORITY_COUNT
        # When True the run method will exit when all threads are idle
        self.exit_on_idle = True
        # Defines the number of files in each work block
        self.file_chunk = DEFAULT_QUEUE_FILE_CHUNK_SIZE
        # User defined callback function called when processing a single directory
        self.handler_dir = None
        # User defined callback function called when processing a single file. A simple default handler is provided
        # if no user defined one is given
        self.handler_file = None
        # User defined callback function called when the main processing thread is started
        self.handler_init = None
        # User defined callback function called on each thread's state initialization
        self.handler_init_thread = None
        # User defined callback function that should consolidate any statistics when get_stats is called
        self.handler_custom_stats = None
        # Number of total threads to use for scanning. This is inclusive of directory processing threads and file
        # processing threads
        self.num_threads = DEFAULT_THREAD_COUNT
        # Set with directory names to skip processing
        self.default_skip_dirs = DEFAULT_SKIP_DIRS
        #
        self.file_q_cutoff = DEFAULT_FILE_QUEUE_CUTOFF
        #
        self.file_q_min_cutoff = DEFAULT_FILE_QUEUE_MIN_CUTOFF
        #
        self.max_work_items = DEFAULT_MAX_WORK_ITEMS
        #
        self.work_q_max_timeout = DEFAULT_QUEUE_MAX_TIMEOUT
        #
        self.work_q_short_timeout = DEFAULT_QUEUE_WAIT_TIMEOUT

    def _add_common_stats(self, base, modify=False):
        if not modify:
            base = copy.deepcopy(base)
        for state in self.threads_state:
            for stat in self.common_stats.keys():
                base[stat] += state["stats"][stat]
        return base

    def _create_stats_state(self):
        stats_state = {}
        for field in STANDARD_STATS_FIELD_NAMES:
            stats_state[field] = 0
        return stats_state

    def _create_thread_instance_state(self):
        instance_state = {
            "cmd_q": queue.Queue(),
            "csv_file_handle": None,
            "custom": {},
            "handle": None,
            "run_state": S_STARTING,
            "stats": self._create_stats_state(),
            "wait_timeout_countdown": DEFAULT_QUEUE_WAIT_TIMEOUT_COUNTDOWN,
        }
        return instance_state

    def _decr_dir_q_thread_count(self):
        self.dir_q_lock.acquire()
        self.dir_q_threads_count -= 1
        self.dir_q_lock.release()

    def _enqueue_chunks(self, root, items, chunk_size, dest_q, cmd_type):
        num_items = len(items)
        # We cannot chunk an empty list so we just queue the empty list. This happens for empty leaf directories.
        if not num_items:
            try:
                dest_q.put(
                    [cmd_type, root, []],
                    block=True,
                    timeout=self.work_q_max_timeout,
                )
            except queue.Full as e:
                LOG.exception(TXT_STR[T_Q_FULL_PUT].format(q=dest_q))
            return
        # Chunk up the item list and push that onto the dest_q queue
        for i in range(0, num_items, chunk_size):
            item_chunk_list = items[i : i + chunk_size]
            try:
                dest_q.put(
                    [cmd_type, root, item_chunk_list],
                    block=True,
                    timeout=self.work_q_max_timeout,
                )
            except queue.Full as e:
                LOG.exception(TXT_STR[T_Q_FULL_PUT].format(q=dest_q))

    def _get_active_threads(self):
        alive_threads = []
        for thread_state in self.threads_state:
            if thread_state["handle"].is_alive():
                alive_threads.append(thread_state["handle"].name)
        return alive_threads

    def _get_queue_to_read(self):
        """Every time a thread is ready to perform more work it checks to see which queue, either the file queue or the
        directory queue, it should try and get a work item from. The queue to work on defaults to the file queue unless
        the following criteria are met:
        In all cases, the directory queue must not be empty to have a thread work on the directory queue
        If the number of files in the file queue are lower than the file_q_min_cutoff then the thread should get a
            directory from the directory queue to process. This is done so the scanner does not run out of files ready
            to process. An example here would be if the file_q_min_cutoff is 200 files and the number of files in the
            file queue drops down to 190, then the free thread will try and get work from the directory queue.
        If the number of queue files is less than the file_q_cutoff and the number of threads working on reading
            directories is less than the dir_priority_count. This two part condition will boost the priority of reading
            from the directory queue when the number of files is lower than a threshold and there are no more than
            dir_priority_count threads already reading directories. As an example, if the file_q_cutoff is 1000 files,
            the dir_priority_count is 2, the current number of threads processing directories is 1, and the current
            number of queued files is 800, then the free thread would be assigned to read from the directory queue. If
            the number of queued files was 1200, then the free thread would be assigned to the file queue. This is due
            to the number of queued files being above the file_q_cutoff, or 1000 file limit. If there were already 2
            threads processing directories then the free thread would also be assigned to the file queue.
        """
        dir_q_size = self.dir_q.qsize()
        file_q_size = self.file_q.qsize()
        if (dir_q_size > 0) and (
            (file_q_size < self.file_q_cutoff and self.dir_q_threads_count <= self.dir_priority_count)
            or (file_q_size < self.file_q_min_cutoff)
        ):
            self._incr_dir_q_thread_count()
            return self.dir_q
        return self.file_q

    def _glob_paths(self, paths):
        # Copy paths so we can manipulate it without altering the source list
        paths = list(paths)
        expanded_paths = []
        for p in paths:
            # If a path ends with .snapshot/some_dir then we create 2 glob patterns with * and .* appended so we can
            # avoid scanning the .snapshot/<snapshot_name> directory itself
            if re.match(r".*?/\.snapshot/[^/]*/?$", p):
                paths.append(os.path.join(p, "*"))
                paths.append(os.path.join(p, ".*"))
                continue
            # When performing globbing, an empty list can result if the path does not exist. In case the path does not
            # exist, return the unglobbed path for further processing
            expanded_paths.extend(glob.glob(p) or [p])
        return expanded_paths

    def _incr_dir_q_thread_count(self):
        self.dir_q_lock.acquire()
        self.dir_q_threads_count += 1
        self.dir_q_lock.release()

    def _process_list_dir(self, path):
        start = time.time()
        dirs_skipped = 0
        try:
            if USE_SCANDIR:
                dir_file_list = []
                for entry in os.scandir(path):
                    dir_file_list.append(entry.name)
            else:
                dir_file_list = os.listdir(path)
        except IOError as ioe:
            if ioe.errno == errno.EACCES:  # 13: No access
                LOG.info(TXT_STR[T_LIST_DIR_PERM_ERR].format(file=path))
            else:
                LOG.info(TXT_STR[T_LIST_DIR_UNKNOWN_ERR].format(file=path))
                LOG.exception(ioe)
            return False
        except PermissionError as pe:
            LOG.info(TXT_STR[T_LIST_DIR_PERM_ERR].format(file=path))
            LOG.exception(pe)
            return False
        except Exception as e:
            if e.errno == errno.ENOENT:  # 2: No such file or directory
                LOG.info(TXT_STR[T_LIST_DIR_NOT_FOUND].format(file=path))
            else:
                LOG.info(TXT_STR[T_LIST_DIR_UNKNOWN_ERR].format(file=path))
                LOG.exception(e)
            return False
        self._enqueue_chunks(path, dir_file_list, self.file_chunk, self.file_q, CMD_PROC_FILE)
        # files_queued includes all potential directories. Adjust the count when we actually know how many dirs
        return {
            "dir_scan_time": (time.time() - start),
            "dirs_queued": 0,
            "dirs_skipped": dirs_skipped,
            "files_queued": len(dir_file_list),
        }

    def _process_walk_dir(self, path):
        start = time.time()
        num_dirs = 0
        num_files = 0
        for root, dirs, files in os.walk(path):
            num_dirs = len(dirs)
            num_files = len(files)
            # Chunk up the returned file list and push that onto the processing queue
            self._enqueue_chunks(root, files, self.file_chunk, self.file_q, CMD_PROC_FILE)
            # Chunk up the returned directory list and push that onto the directory processing queue
            self._enqueue_chunks(root, dirs, self.dir_chunk, self.dir_q, CMD_PROC_DIR)
            break
        return {
            "dir_scan_time": (time.time() - start),
            "dirs_queued": num_dirs,
            "dirs_skipped": 0,
            "files_queued": num_files,
        }

    def _process_queues(self, state):
        name = state["handle"].name
        reset_idle_required = False
        state["run_state"] = S_RUNNING
        stats = state["stats"]
        wait_timeout_countdown = state["wait_timeout_countdown"]
        while True:
            # Process our command queue
            try:
                cmd_item = state["cmd_q"].get(block=False)
                cmd = cmd_item[0]
                if cmd == CMD_EXIT:
                    state["run_state"] = S_IDLE
                    LOG.debug(TXT_STR[T_CMD_EXIT].format(tid=name))
                    break
            except queue.Empty:
                pass
            except Exception as e:
                LOG.exception(e)
            # Process the normal work queues
            try:
                start = time.time()
                q_to_read = self._get_queue_to_read()
                work_item = q_to_read.get(block=True, timeout=self.work_q_short_timeout)
                stats["q_wait_time"] += time.time() - start
                if reset_idle_required:
                    reset_idle_required = False
                    state["run_state"] = S_RUNNING
                cmd = work_item[0]
                start = time.time()
                if cmd == CMD_PROC_DIR:
                    for dirname in work_item[2]:
                        # If the directory name is in our skip directory list, skip this directory
                        # or ff user supplied handler_dir returns False, we should skip this directory
                        if (dirname in self.default_skip_dirs) or (
                            self.handler_dir and not self.handler_dir(work_item[1], dirname)
                        ):
                            LOG.info({"msg": "Skipping directory", "base": work_item[1], "filename": dirname})
                            stats["dirs_skipped"] += 1
                            continue
                        try:
                            handler_stats = self._process_list_dir(os.path.join(work_item[1], dirname))
                            if not handler_stats:
                                LOG.info({"msg": "Skipping directory", "base": work_item[1], "filename": dirname})
                                stats["dirs_skipped"] += 1
                                continue
                        except MemoryError:
                            LOG.exception(TXT_STR[T_OOM_PROCESS_NEW_DIR].format(tid=name, r=work_item[1], d=dirname))
                            raise TerminateThread
                        except:
                            LOG.exception(TXT_STR[T_EX_PROCESS_NEW_DIR].format(tid=name, r=work_item[1], d=dirname))
                            raise TerminateThread
                        stats["dirs_processed"] += 1
                        for key in handler_stats.keys():
                            stats[key] += handler_stats[key]
                    stats["dir_handler_time"] += time.time() - start
                elif cmd == CMD_PROC_FILE:
                    try:
                        stat_data = self.handler_file(
                            work_item[1],  # root path
                            work_item[2],  # List of filenames in the root path
                            {
                                "custom_state": state["custom"],
                                "custom_tagging": self.custom_state["custom_tagging"],
                                "extra_attr": self.custom_state["extra_attr"],
                                "fields": self.custom_state["fields"],
                                "no_acl": self.custom_state["no_acl"],
                                "no_names": self.custom_state["no_names"],
                                "nodepool_translation": self.custom_state["nodepool_translation"],
                                "phys_block_size": self.custom_state["phys_block_size"],
                                "strip_do_snapshot": self.custom_state["fields"],
                                "user_attr": self.custom_state["user_attr"],
                            },
                        )
                        stats["files_not_found"] += stat_data["statistics"]["not_found"]
                        stats["files_processed"] += stat_data["statistics"]["processed"]
                        stats["files_skipped"] += stat_data["statistics"]["skipped"]
                        result_dir_list = stat_data["dirs"]
                        result_list = stat_data["files"]
                        for entry in result_list:
                            stats["file_size_total"] += entry["size"]
                            stats["file_size_physical_total"] += entry["size_physical"]
                        if result_list or result_dir_list:
                            if self.custom_state.get("csv_output_path") and not state.get("csv_file_handle"):
                                format_string_vars = {
                                    "hostname": platform.node(),
                                    "pid": os.getpid(),
                                    "prefix": "data",
                                    "suffix": ".csv",
                                    "tid": state["custom"].get("thread_name"),
                                }
                                csv_filename = "{prefix}-{hostname}-{pid}-{tid}{suffix}".format(**format_string_vars)
                                csv_output_path = self.custom_state.get("csv_output_path")
                                try:
                                    # On Python 3 use the newline="" option to avoid incorrect line terminators
                                    state["csv_file_handle"] = open(
                                        os.path.join(csv_output_path, csv_filename), "w", newline=""
                                    )
                                except:
                                    # Fallback to Python 2 method of opening the file in binary mode
                                    state["csv_file_handle"] = open(os.path.join(csv_output_path, csv_filename), "wb")
                                state["csv_writer"] = csv.writer(state["csv_file_handle"])
                                state["csv_writer"].writerow(scanner.convert_response_to_csv({}, headers_only=True))
                            # Output results to CSV files, 1 per processing thread
                            if state.get("csv_file_handle"):
                                for result in result_list:
                                    state["csv_writer"].writerow(scanner.convert_response_to_csv(result))
                                for result in result_dir_list:
                                    state["csv_writer"].writerow(scanner.convert_response_to_csv(result))
                            # Send results to ElasticSearch
                            if self.custom_state["client_config"].get("es_cmd_idx"):
                                time_start = time.time()
                                if result_list:
                                    self.custom_state["es_send_q"].put([CMD_SEND, result_list])
                                if result_dir_list:
                                    self.custom_state["es_send_q"].put([CMD_SEND_DIR, result_dir_list])
                                for i in range(DEFAULT_MAX_Q_WAIT_LOOPS):
                                    if self.custom_state["es_send_q"].qsize() > self.custom_state["max_send_q_size"]:
                                        stats["es_queue_wait_count"] += 1
                                        time.sleep(self.custom_state["send_q_sleep"])
                                    else:
                                        break
                                stats["time_es_queue"] += time.time() - time_start

                        dirs_to_queue = [x["file_name"] for x in result_dir_list]
                        if dirs_to_queue:
                            dir_queue_length = len(dirs_to_queue)
                            self._enqueue_chunks(work_item[1], dirs_to_queue, self.dir_chunk, self.dir_q, CMD_PROC_DIR)
                            stats["dirs_queued"] += dir_queue_length
                            # Fix the files queued count to adjust for the directories queued as files
                            stats["files_queued"] -= dir_queue_length
                        stats["file_handler_time"] += time.time() - start
                    except MemoryError:
                        LOG.exception(TXT_STR[T_OOM_PROCESS_FILE].format(tid=name, r=work_item[1]))
                        raise TerminateThread
                    except:
                        LOG.exception(TXT_STR[T_EX_PROCESS_FILE].format(tid=name, r=work_item[1]))
                        raise TerminateThread
                if cmd == CMD_EXIT:
                    break
            except TerminateThread:
                self.terminate()
                break
            except queue.Empty:
                if q_to_read == self.dir_q:
                    continue
                wait_timeout_countdown -= 1
                if wait_timeout_countdown <= 0 and self.dir_q.empty() and self.file_q.empty():
                    state["run_state"] = S_IDLE
                    wait_timeout_countdown = state["wait_timeout_countdown"]
                    reset_idle_required = True
            except MemoryError:
                LOG.exception(TXT_STR[T_OOM_PROCESS_GENERAL].format(tid=name))
                self.terminate()
                break
            except Exception as e:
                LOG.exception(TXT_STR[T_EX_PROCESS_GENERAL].format(tid=name, ex=e))
                self.terminate()
                break
            else:
                if q_to_read == self.dir_q:
                    self._decr_dir_q_thread_count()

        state["run_state"] = S_IDLE
        LOG.debug(TXT_STR[T_EXIT].format(tid=name))

    def _start_worker_threads(self):
        if self.threads_state:
            return
        for thread_id in range(self.num_threads):
            thread_instance = self._create_thread_instance_state()
            self.threads_state.append(thread_instance)
            thandle = threading.Thread(
                target=self._process_queues,
                kwargs={
                    "state": thread_instance,
                },
            )
            thandle.daemon = True
            thread_instance["handle"] = thandle
            if self.handler_init_thread:
                self.handler_init_thread(thread_id, self.custom_state, thread_instance["custom"])
            LOG.debug(TXT_STR[T_START_THREAD].format(tid=thandle.name))
            thandle.start()

    def add_scan_path(self, paths):
        """
        This method supports 3 types of input for the paths
        1) A simple single string that specifies a full path
           e.g. /ifs/some/path
        2) A list of simple strings that each specify a full path
           e.g. ["/ifs/some/path", "/ifs/other/path", ...]
        3) A list of tuples where the first element of the tuple is a root and the second element is a list of
           directory names that start at that root
           e.g. [["/ifs/root1", ["dir1", "dir2", "dir3", ...]], ["/ifs/root2", ["dirA", ...]]]

        """
        if not paths:
            return
        if not isinstance(paths, list):
            # We have a simple string path
            paths = [paths]
        elif isinstance(paths[0], list):
            # We have case 3 with a list of tuples/lists
            temp_paths = []
            for path_set in paths:
                for entry in path_set[1]:
                    temp_paths.append(os.path.join(path_set[0], entry))
            paths = temp_paths
        paths = self._glob_paths(paths)
        for p in paths:
            if p.endswith("/"):
                p = p[0:-1]
            self._enqueue_chunks(os.path.dirname(p), [os.path.basename(p)], self.dir_chunk, self.dir_q, CMD_PROC_DIR)
        self.common_stats["dirs_queued"] += len(paths)

    def is_processing(self):
        processing_count = 0
        if self.process_alive == -1:
            return 1
        for thread_state in self.threads_state:
            alive = thread_state["handle"].is_alive()
            run_state = thread_state["run_state"]
            if not alive:
                if run_state > S_STARTING:
                    LOG.critical(TXT_STR[T_INCONSISTENT_THREAD_STATE].format(tid=thread_state["handle"].name))
                    thread_state["run_state"] = S_IDLE
            elif run_state != S_IDLE:
                processing_count += 1
        return processing_count

    def get_custom_state(self):
        custom_threads_state = [x["custom"] for x in self.threads_state]
        return (self.custom_state, custom_threads_state)

    def get_dir_queue_items(self, num_items=0, percentage=0.5):
        cur_q_size = self.dir_q.qsize()
        if percentage > 1:
            percentage = percentage / 100.0
        if percentage > 1 or percentage < 0:
            perentage = 0.5
        p_items = int(cur_q_size * percentage)
        num_to_return = p_items if p_items > num_items else num_items
        if num_to_return > self.max_work_items:
            num_to_return = self.max_work_items
        dir_items = []
        for i in range(num_to_return):
            try:
                work_item = self.dir_q.get(block=False)
                dir_items.append([work_item[1], work_item[2]])
                # Decrease our directory queued count by the number of directories we return
                self.common_stats["dirs_queued"] -= len(work_item[2])
            except queue.Empty:
                break
        return dir_items

    def get_dir_queue_size(self):
        return self.dir_q.qsize()

    def get_file_queue_size(self):
        return self.file_q.qsize()

    def get_stats(self):
        stats = self._add_common_stats(self.common_stats)
        if self.handler_custom_stats:
            custom_threads_state = [x["custom"] for x in self.threads_state]
            stats["custom"] = self.handler_custom_stats(
                self.common_stats,
                self.custom_state,
                custom_threads_state,
                self.threads_state,
            )
        stats["dir_q_size"] = self.dir_q.qsize()
        stats["file_q_size"] = self.file_q.qsize()
        stats["threads"] = self.num_threads
        return stats

    def run(self):
        LOG.debug("Starting main thread")
        try:
            self.run_start = self.run_start or time.time()
            # Save the current time to have all the threads have the same reference time
            # Invoke custom init handler
            if self.handler_init:
                self.handler_init(self.custom_state)
            # Validate state variables are sane
            self.validate_state_variables()
            # Start up worker threads
            self._start_worker_threads()
            # Wait for threads to finish or we get additional commands from the calling process
            self.process_alive = True
            while self.process_alive:
                time.sleep(DEFAULT_POLL_INTERVAL)
                if self.exit_on_idle and not self.is_processing():
                    break
            self.terminate()
            LOG.debug(TXT_STR[T_JOIN_THREADS])
            for thread_instance in self.threads_state:
                thread_instance["handle"].join()
        except KeyboardInterrupt as kbe:
            self.terminate()
        except Exception as e:
            LOG.exception(e)
            self.terminate()

    def terminate(self, forced=False):
        for thread_state in self.threads_state:
            if thread_state["handle"].is_alive():
                if thread_state.get("csv_file_handle"):
                    thread_state.get("csv_file_handle").close()
                    LOG.debug(TXT_STR[T_CLOSE_CSV_HANDLE].format(tid=thread_state["handle"].name))
                LOG.debug(TXT_STR[T_SEND_TERM_THREAD].format(tid=thread_state["handle"].name))
                thread_state["cmd_q"].put([CMD_EXIT, None, None])
        self.process_alive = False
        if forced:
            # TODO: Add code to forcibly kill any threads that are not dead
            pass

    def validate_state_variables(self):
        if self.dir_chunk < 1:
            self.dir_chunk = 1
        if self.file_chunk < 1:
            self.file_chunk = 1
        if self.num_threads < 1:
            self.num_threads = 1
        if self.dir_priority_count > self.num_threads:
            if self.num_threads == 1:
                self.dir_priority_count = 0
            else:
                self.dir_priority_count = 1
        if not self.handler_file:
            raise Exception("A handler function is required")
