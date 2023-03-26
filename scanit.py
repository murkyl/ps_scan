#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
Module description here
"""
# fmt: off
__title__         = "scanit"
__version__       = "1.0.0"
__date__          = "16 March 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
__all__ = [
    "default_adv_file_handler",
    "default_file_handler",
    "ScanIt",
    "TerminateThread",
]
# fmt: on
import copy
import logging
import multiprocessing
import os
import pickle
import queue
import stat
import threading
import time
import zlib

try:
    import isi.fs.attr as isi_attr

    OS_TYPE = "PowerScale"
except:
    OS_TYPE = "Other"


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
# Selection for type of processing to perform
PROCESS_TYPE_SIMPLE = next_int(True)
PROCESS_TYPE_ADVANCED = next_int()
# ================================================================================
# Default values
#   Number of threads that are allowed to walk directories out of the total thread count
DEFAULT_DIR_PRIORITY_COUNT = 2
#   When there are more than FILE_QUEUE_CUTOFF files chunks in the file_q, only process files
#   The number of total file names in the queue is DEFAULT_FILE_QUEUE_CUTOFF*DEFAULT_QUEUE_FILE_CHUNK_SIZE
DEFAULT_FILE_QUEUE_CUTOFF = 5000
#   When there are less than FILE_QUEUE_MIN_CUTOFF files in the file_q, just process directories
DEFAULT_FILE_QUEUE_MIN_CUTOFF = DEFAULT_FILE_QUEUE_CUTOFF // 10
DEFAULT_POLL_INTERVAL = 0.1
DEFAULT_PROCESS_TYPE = PROCESS_TYPE_SIMPLE
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
T_OOM_PROCESS_FILE = next_int()
T_OOM_PROCESS_GENERAL = next_int()
T_OOM_PROCESS_NEW_DIR = next_int()
T_Q_FULL_PUT = next_int()
T_SEND_TERM_THREAD = next_int()
T_START_THREAD = next_int()
TXT_STR = {
    T_CMD_EXIT: "({tid}) Thread receieved exit command. Setting run state to idle and breaking out of process loop",
    T_EX_PROCESS_FILE: "({tid}) - Exception in file handler for root: {r}",
    T_EX_PROCESS_GENERAL: "({tid}) - Exception found in thread handler: {ex}",
    T_EX_PROCESS_NEW_DIR: "({tid}) - Exception in _process_new_dir on directory: {r}/{d}",
    T_EXIT: "({tid}) Thread exiting",
    T_INCONSISTENT_THREAD_STATE: "({tid}) Dead thread did not set run state to idle. Forcing idle status.",
    T_JOIN_THREADS: "Joining threads",
    T_OOM_PROCESS_FILE: "({tid}) - Out of memory in file handler for root: {r}",
    T_OOM_PROCESS_GENERAL: "({tid}) - Out of memory in thread handler",
    T_OOM_PROCESS_NEW_DIR: "({tid}) - Out of memory in _process_new_dir on directory: {r}/{d}",
    T_Q_FULL_PUT: "Unable to PUT an item onto the queue: {q}",
    T_SEND_TERM_THREAD: "Sending thread terminate to: {tid}",
    T_START_THREAD: "Starting thread: {tid}",
}
# ================================================================================
LOG = logging.getLogger(__name__)


class TerminateThread(Exception):
    "Raised when an inner loop needs the running thread to terminate"
    pass


def default_adv_file_handler(root, filename_list, stats, extra_args={}):
    """
    The file handler returns a dictionary:
    {
      "dirs": []
      "processed": <int>                # Number of files actually processed
      "skipped": <int>                  # Number of files skipped
    }
    """
    processed = 0
    skipped = 0
    dir_list = []
    for filename in filename_list:
        full_path = os.path.join(root, filename)
        try:
            file_stats = os.lstat(full_path)
            if stat.S_ISDIR(file_stats.st_mode):
                # Save directories to re-queue
                dir_list.append(filename)
            else:
                stats["file_size_total"] += file_stats.st_size
                processed += 1
        except:
            skipped += 1
    return {"processed": processed, "skipped": skipped, "q_dirs": dir_list}


def default_file_handler(root, filename_list, stats, extra_args={}):
    """
    The file handler returns a dictionary:
    {
      "processed": <int>                # Number of files actually processed
      "skipped": <int>                  # Number of files skipped
    }
    """
    processed = 0
    skipped = 0
    for filename in filename_list:
        full_path = os.path.join(root, filename)
        try:
            file_stats = os.lstat(full_path)
            stats["file_size_total"] += file_stats.st_size
            processed += 1
        except:
            skipped += 1
    return {"processed": processed, "skipped": skipped}


class ScanIt(threading.Thread):
    def __init__(self, *args, **kwargs):
        super(ScanIt, self).__init__(args=args, kwargs=kwargs)
        self.common_stats = self._create_stats_state()
        self.custom_state = {}
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
        # What type of handlers will be used. The default is PROCESS_TYPE_SIMPLE which uses os.walk and assumes
        # the file handler will perform a simple stat call on each file only. When the type is set to
        # PROCESS_TYPE_POWERSCALE, the file handler is expected to use the isi.fs.attr.get_dinode call and to also
        # return a list of directory entries for further processing. PROCESS_TYPE_POWERSCALE can only be used if the
        # script detects it is running on a PowerScale cluster
        self.processing_type = DEFAULT_PROCESS_TYPE
        # Set with directory names to skip processing
        self.default_skip_dirs = DEFAULT_SKIP_DIRS
        #
        self.file_q_cutoff = DEFAULT_FILE_QUEUE_CUTOFF
        #
        self.file_q_min_cutoff = DEFAULT_FILE_QUEUE_MIN_CUTOFF
        #
        self.work_q_max_timeout = DEFAULT_QUEUE_MAX_TIMEOUT
        #
        self.work_q_short_timeout = DEFAULT_QUEUE_WAIT_TIMEOUT

    def _add_stats_state(self, base, modify=False):
        if not modify:
            base = copy.deepcopy(base)
        for state in self.threads_state:
            for stat in self.common_stats.keys():
                base[stat] += state["stats"][stat]
        return base

    def _create_stats_state(self):
        stats_state = {
            "dir_scan_time": 0,
            "dir_handler_time": 0,
            "dirs_processed": 0,
            "dirs_queued": 0,
            "dirs_skipped": 0,
            "file_handler_time": 0,
            "file_size_total": 0,
            "files_processed": 0,
            "files_queued": 0,
            "files_skipped": 0,
            "q_wait_time": 0,
        }
        return stats_state

    def _create_thread_instance_state(self):
        instance_state = {
            "cmd_q": queue.Queue(),
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
        dir_q_size = self.dir_q.qsize()
        file_q_size = self.file_q.qsize()
        q_to_read = self.file_q
        if (dir_q_size > 0) and (
            (file_q_size < self.file_q_cutoff and self.dir_q_threads_count < self.dir_priority_count)
            or (file_q_size < self.file_q_min_cutoff)
        ):
            self._incr_dir_q_thread_count()
            return self.dir_q
        return self.file_q

    def _incr_dir_q_thread_count(self):
        self.dir_q_lock.acquire()
        self.dir_q_threads_count += 1
        self.dir_q_lock.release()

    def _process_new_dir(self, path):
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
            "files_queued": num_files,
        }

    def _process_ps_new_dir(self, path):
        start = time.time()
        dir_file_list = os.listdir(path)
        self._enqueue_chunks(path, dir_file_list, self.file_chunk, self.file_q, CMD_PROC_FILE)
        # files_queued includes all potential directories. Adjust the count when we actually know how many dirs
        return {
            "dir_scan_time": (time.time() - start),
            "dirs_queued": 0,
            "files_queued": len(dir_file_list),
        }

    def _process_adv_queues(self, state):
        state["run_state"] = S_RUNNING
        name = state["handle"].name
        reset_idle_required = False
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
                if cmd == CMD_PROC_DIR:
                    for dirname in work_item[2]:
                        # If the directory name is in our skip directory list, skip this directory
                        if dirname in self.default_skip_dirs:
                            stats["dirs_skipped"] += 1
                            continue
                        # If handler_dir returns True, we should skip this directory
                        if self.handler_dir and self.handler_dir(work_item[1], dirname):
                            stats["dirs_skipped"] += 1
                            continue
                        stats["dirs_processed"] += 1
                        try:
                            handler_stats = self._process_ps_new_dir(os.path.join(work_item[1], dirname))
                        except MemoryError:
                            LOG.exception(TXT_STR[T_OOM_PROCESS_NEW_DIR].format(tid=name, r=work_item[1], d=dirname))
                            raise TerminateThread
                        except:
                            LOG.exception(TXT_STR[T_EX_PROCESS_NEW_DIR].format(tid=name, r=work_item[1], d=dirname))
                            raise TerminateThread
                        for stat_item in handler_stats:
                            stats[stat_item] += handler_stats[stat_item]
                    stats["dir_handler_time"] += time.time() - start
                elif cmd == CMD_PROC_FILE:
                    try:
                        handler_stats = self.handler_file(
                            work_item[1],  # root path
                            work_item[2],  # List of filenames in the root path
                            stats,
                            {
                                "custom_state": self.custom_state,
                                "start_time": self.run_start,
                                "thread_custom_state": state["custom"],
                                "thread_state": state,
                            },
                        )
                        stats["file_handler_time"] += time.time() - start
                        stats["files_processed"] += handler_stats["processed"]
                        stats["files_skipped"] += handler_stats["skipped"]
                        dirs_to_queue = handler_stats.get("q_dirs", [])
                        if dirs_to_queue:
                            self._enqueue_chunks(work_item[1], dirs_to_queue, self.dir_chunk, self.dir_q, CMD_PROC_DIR)
                            stats["dirs_queued"] += len(dirs_to_queue)
                            # Fix the files queued count to adjust for the incorrect number of directories queued
                            stats["files_queued"] -= len(dirs_to_queue)
                    except MemoryError:
                        LOG.exception(TXT_STR[T_OOM_PROCESS_FILE].format(tid=name, r=work_item[1]))
                        raise TerminateThread
                    except:
                        LOG.exception(TXT_STR[T_EX_PROCESS_FILE].format(tid=name, r=work_item[1]))
                        raise TerminateThread
                if q_to_read == self.dir_q:
                    self._decr_dir_q_thread_count()
                if cmd == CMD_EXIT:
                    break
            except TerminateThread:
                self.terminate()
                break
            except queue.Empty:
                wait_timeout_countdown -= 1
                if wait_timeout_countdown <= 0:
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
        state["run_state"] = S_IDLE
        LOG.debug(TXT_STR[T_EXIT].format(tid=name))

    def _process_queues(self, state):
        state["run_state"] = S_RUNNING
        name = state["handle"].name
        reset_idle_required = False
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
                if cmd == CMD_PROC_DIR:
                    for dirname in work_item[2]:
                        # If the directory name is in our skip directory list, skip this directory
                        if dirname in self.default_skip_dirs:
                            stats["dirs_skipped"] += 1
                            continue
                        # If handler_dir returns True, we should skip this directory
                        if self.handler_dir and self.handler_dir(work_item[1], dirname):
                            stats["dirs_skipped"] += 1
                            continue
                        stats["dirs_processed"] += 1
                        try:
                            handler_stats = self._process_new_dir(os.path.join(work_item[1], dirname))
                        except MemoryError:
                            LOG.exception(TXT_STR[T_OOM_PROCESS_NEW_DIR].format(tid=name, r=work_item[1], d=dirname))
                            raise TerminateThread
                        except:
                            LOG.exception(TXT_STR[T_EX_PROCESS_NEW_DIR].format(tid=name, r=work_item[1], d=dirname))
                            raise TerminateThread
                        for stat_item in handler_stats:
                            stats[stat_item] += handler_stats[stat_item]
                    stats["dir_handler_time"] += time.time() - start
                elif cmd == CMD_PROC_FILE:
                    try:
                        handler_stats = self.handler_file(
                            work_item[1],  # root path
                            work_item[2],  # List of filenames in the root path
                            stats,
                            {
                                "custom_state": self.custom_state,
                                "start_time": self.run_start,
                                "thread_custom_state": state["custom"],
                                "thread_state": state,
                            },
                        )
                        stats["file_handler_time"] += time.time() - start
                        stats["files_processed"] += handler_stats["processed"]
                        stats["files_skipped"] += handler_stats["skipped"]
                    except MemoryError:
                        LOG.exception(TXT_STR[T_OOM_PROCESS_FILE].format(tid=name, r=work_item[1]))
                        raise TerminateThread
                    except:
                        LOG.exception(TXT_STR[T_EX_PROCESS_FILE].format(tid=name, r=work_item[1]))
                        raise TerminateThread
                if q_to_read == self.dir_q:
                    self._decr_dir_q_thread_count()
                if cmd == CMD_EXIT:
                    break
            except TerminateThread:
                self.terminate()
                break
            except queue.Empty:
                wait_timeout_countdown -= 1
                if wait_timeout_countdown <= 0:
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
        state["run_state"] = S_IDLE
        LOG.debug(TXT_STR[T_EXIT].format(tid=name))

    def add_scan_path(self, paths):
        if not paths:
            return
        if not isinstance(paths, list):
            paths = [paths]
        self.file_q.put([CMD_PROC_DIR, "", paths])
        self.common_stats["dirs_queued"] += len(paths)

    def is_processing(self):
        processing_count = 0
        if self.process_alive == -1:
            return 1
        for s in self.threads_state:
            alive = s["handle"].is_alive()
            run_state = s["run_state"]
            if not alive:
                if run_state > S_STARTING:
                    LOG.critical(TXT_STR[T_INCONSISTENT_THREAD_STATE].format(tid=s["handle"].name))
                    s["run_state"] = S_IDLE
            elif run_state != S_IDLE:
                processing_count += 1
        return processing_count

    def get_custom_state(self):
        custom_threads_state = [x["custom"] for x in self.threads_state]
        return (self.custom_state, custom_threads_state)

    def get_stats(self):
        stats = self._add_stats_state(self.common_stats, False)
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
        return stats

    def run(self):
        LOG.debug("Starting main thread")
        try:
            self.run_start = time.time()
            # Save the current time to have all the threads have the same reference time
            # Invoke custom init handler
            if self.handler_init:
                self.handler_init(self.custom_state)
            # Validate state variables are sane
            self.validate_state_variables()

            for thread_id in range(self.num_threads):
                instance_state = self._create_thread_instance_state()
                self.threads_state.append(instance_state)
                if self.processing_type == PROCESS_TYPE_ADVANCED:
                    thread_instance = threading.Thread(
                        target=self._process_adv_queues, kwargs={"state": instance_state}
                    )
                else:
                    thread_instance = threading.Thread(target=self._process_queues, kwargs={"state": instance_state})
                instance_state["handle"] = thread_instance
                if self.handler_init_thread:
                    self.handler_init_thread(self.custom_state, instance_state["custom"])
                LOG.debug(TXT_STR[T_START_THREAD].format(tid=thread_instance.name))
                thread_instance.start()
            # Wait for threads to finish or we get additional commands from the calling process
            self.process_alive = True
            while self.process_alive:
                time.sleep(DEFAULT_POLL_INTERVAL)
                if self.exit_on_idle and not self.is_processing():
                    break
                # TODO: Setup command loop to listen for external requests
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
        if self.handler_file is None:
            if self.processing_type == PROCESS_TYPE_ADVANCED:
                self.handler_file = default_adv_file_handler
            else:
                self.handler_file = default_file_handler
