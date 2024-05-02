#!/usr/bin/env python
# coding: utf-8
"""
User defined handlers
"""
# fmt: off
__title__         = "user_handlers"
__version__       = "1.0.0"
__date__          = "10 April 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
__all__ = [
    "custom_stats_handler",
    # Placeholder for dynamic function handler_dir
    #"handler_dir",
    "handler_dir_skip",
    "init_custom_state",
    "init_thread",
    "print_statistics",
    "shutdown",
    "update_config",
    "update_dir_skip_handler",
]
# fmt: on
import datetime
import errno
import json
import logging
import math
import os
try:
    import queue
except:
    import Queue as queue
import re
import stat
import sys
import threading
import time

from helpers.constants import *
import helpers.elasticsearch_wrapper as elasticsearch_wrapper
import helpers.scanner as scanner

LOG = logging.getLogger(__name__)
CUSTOM_STATS_FIELDS = scanner.STATS_FIELDS
DIR_SKIP_REGEX_ARRAY = None


def custom_stats_handler(common_stats, custom_state, custom_threads_state, thread_state):
    # Access all the individual thread state dictionaries in the custom_threads_state array
    # These should be initialized in the init_thread routine
    # LOG.debug("DEBUG: Custom stats handler called!")
    # LOG.debug(
    #    "DEBUG: Common stats: %s"
    #    % json.dumps(common_stats, indent=2, sort_keys=True, default=lambda o: "<not serializable>")
    # )
    # LOG.debug(
    #   "DEBUG: Custom state: %s"
    #   % json.dumps(custom_state, indent=2, sort_keys=True, default=lambda o: "<not serializable>")
    # )
    # LOG.debug(
    #   "DEBUG: Custom threads state: %s"
    #   % json.dumps(custom_threads_state, indent=2, sort_keys=True, default=lambda o: "<not serializable>")
    # )
    # LOG.debug(
    #    "DEBUG: Thread state: %s"
    #    % json.dumps(thread_state, indent=2, sort_keys=True, default=lambda o: "<not serializable>")
    # )
    custom_stats = custom_state["custom_stats"]
    for field in CUSTOM_STATS_FIELDS:
        custom_stats[field] = 0
    for thread_state in custom_threads_state:
        thread_stats = thread_state.get("stats", {})
        for field in CUSTOM_STATS_FIELDS:
            custom_stats[field] += thread_stats.get(field, 0)
    return custom_stats


# The handler_dir function can be defined with code to determine if a directory should be skipped or not
# Defining it statically will cause the code to be executed for every directory processed
# An alternative method is to define a second method and use a runtime assignment to alter the module if needed
# e.g. user_handlers.hander_dir = user_handlers.some_other_method
# def handler_dir(base, dirname):
# return True


def handler_dir_skip(base, dirname):
    global DIR_SKIP_REGEX_ARRAY
    full_path = os.path.join(base, dirname)
    for entry in DIR_SKIP_REGEX_ARRAY:
        if entry.match(full_path):
            return False
    return True


def update_dir_skip_handler(regex_array):
    global DIR_SKIP_REGEX_ARRAY
    global handler_dir
    DIR_SKIP_REGEX_ARRAY = regex_array
    handler_dir = handler_dir_skip


def init_custom_state(custom_state, options={}):
    # Add any common parameters that each processing thread should have access to
    # by adding values to the custom_state dictionary
    custom_state["csv_output_path"] = None
    custom_state["custom_stats"] = {}
    custom_state["custom_tagging"] = None  # lambda x: None
    custom_state["extra_attr"] = options.get("extra", DEFAULT_PARSE_EXTRA_ATTR)
    custom_state["es_send_q"] = queue.Queue()
    custom_state["es_send_cmd_q"] = queue.Queue()
    custom_state["fields"] = None
    custom_state["max_send_q_size"] = options.get("es_max_send_q_size", DEFAULT_ES_MAX_Q_SIZE)
    custom_state["no_acl"] = options.get("no_acl", DEFAULT_PARSE_SKIP_ACLS)
    custom_state["no_names"] = options.get("no_names", DEFAULT_PARSE_SKIP_NAMES)
    custom_state["nodepool_translation"] = options.get("nodepool_translation", {})
    custom_state["phys_block_size"] = IFS_BLOCK_SIZE
    custom_state["send_q_sleep"] = options.get("es_send_q_sleep", DEFAULT_ES_SEND_Q_SLEEP)
    custom_state["strip_dot_snapshot"] = options.get("strip_dot_snapshot", DEFAULT_STRIP_DOT_SNAPSHOT)
    custom_state["user_attr"] = options.get("user_attr", DEFAULT_PARSE_USER_ATTR)


def init_thread(tid, custom_state, thread_custom_state):
    """Initialize each scanning thread to store thread specific state

    This function is called by scanit.py when it initializes each scanning thread.
    Add any custom stats counters or values in the thread_custom_state dictionary
    and access this inside each file handler in the args["thread_state"]["custom"]
    parameter.

    Parameters
    ----------
    tid: int - Numeric identifier for a thread
    custom_state: dict - Dictionary initialized by user_handlers.init_custom_state
    thread_custom_state: dict - Empty dictionary to store any thread specific state

    Returns
    ----------
    Nothing
    """
    thread_custom_state["thread_name"] = tid
    thread_custom_state["stats"] = {}
    for field in CUSTOM_STATS_FIELDS:
        thread_custom_state["stats"][field] = 0


def print_statistics(output_type, log, stats, custom_stats, now, start_time, wall_time, output_interval):
    total_threads = stats.get("threads", 1)
    consolidated_custom_stats = {}
    for field in CUSTOM_STATS_FIELDS:
        consolidated_custom_stats[field] = 0
        for client in custom_stats:
            try:
                consolidated_custom_stats[field] += client[field]
            except:
                LOG.critical(
                    "DEBUG: FIELD: %s, CCS: %s, CLIENT: %s"
                    % (field, consolidated_custom_stats.get("field"), client.get("field"))
                )
        if "time_" in field:
            consolidated_custom_stats[field] = consolidated_custom_stats[field] / total_threads
    output_string = (
        "===== Custom stats (average over {tcount} threads) =====\n".format(tcount=total_threads)
        + json.dumps(consolidated_custom_stats, indent=2, sort_keys=True)
        + "\n"
    )
    LOG.info(output_string)
    sys.stdout.write(output_string)


def shutdown(custom_state, custom_threads_state):
    if custom_state.get("es_thread_handles"):
        LOG.debug("Waiting for Elastic Search send threads to complete and become idle.")
        for thread_handle in custom_state.get("es_thread_handles"):
            custom_state.get("es_send_cmd_q").put([CMD_EXIT, {"flush": True}])
        # Wait for up to 120 seconds for all the ES threads to terminate after sending an exit command
        wait_for_threads = list(custom_state.get("es_thread_handles"))
        flush_time = custom_state.get("es_flush_timeout")
        for i in range(flush_time):
            for thread in wait_for_threads[::-1]:
                if not thread.is_alive():
                    wait_for_threads.remove(thread)
            if not wait_for_threads:
                break
            time.sleep(1)
        if wait_for_threads:
            LOG.warn(
                "{num_threads} threads did not finish sending data after {time} seconds. Possible data loss.".format(
                    num_threads=len(wait_for_threads), time=flush_time
                )
            )
        else:
            LOG.debug({"msg": "Send queue size", "qsize": custom_state.get("es_send_cmd_q").qsize()})
            LOG.debug({"msg": "Elastic Search send threads have all shutdown"})


def update_config(custom_state, new_config):
    cli_config = new_config.get("cli_options", {})
    client_config = new_config.get("client_config", {})
    es_options = client_config.get("es_options")
    custom_state["csv_output_path"] = cli_config.get("csv_output_path", None)
    # TODO: Add code to shutdown existing threads or adjust running threads based on new config
    if es_options:
        if custom_state.get("es_thread_handles") is not None:
            # TODO: Add support for closing and reconnecting to a new ES instance
            pass
        es_threads = []
        threads_to_start = es_options.get("es_send_threads", DEFAULT_ES_THREADS)
        try:
            for i in range(threads_to_start):
                es_thread_instance = threading.Thread(
                    target=elasticsearch_wrapper.es_data_sender,
                    args=(
                        custom_state["es_send_q"],
                        custom_state["es_send_cmd_q"],
                        es_options["url"],
                        es_options["user"],
                        es_options["password"],
                        client_config["es_cmd_idx"],
                    ),
                    kwargs={
                        "bulk_count": cli_config.get("es_bulk_count", DEFAULT_ES_BULK_COUNT),
                        "compresslevel": int(cli_config.get("compression", DEFAULT_COMPRESSION_LEVEL)),
                    },
                )
                es_thread_instance.daemon = True
                es_thread_instance.start()
                es_threads.append(es_thread_instance)
        except Exception as e:
            LOG.exception("Error encountered starting up ES sender threads")
        custom_state["es_flush_timeout"] = es_options.get("es_flush_timeout", DEFAULT_ES_FLUSH_TIMEOUT)
        custom_state["es_thread_handles"] = es_threads
    custom_state["client_config"] = client_config
    custom_state["no_acl"] = cli_config.get("no_acl", custom_state["no_acl"])
    custom_state["no_names"] = cli_config.get("no_names", custom_state["no_acl"])
