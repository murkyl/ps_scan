#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
Module description here
"""
# fmt: off
__title__         = "ps_scan"
__version__       = "1.0.0"
__date__          = "16 March 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
# fmt: on
import copy
import datetime
import json
import logging
import multiprocessing as mp
import os
import queue
import socket
import stat
import sys
import threading
import time

import elasticsearchlite
import scanit
from helpers.cli_parser import *
from helpers.constants import *

try:
    import isi.fs.attr as attr
    import isi.fs.diskpool as dp
    import isi.fs.userattr as uattr

    OS_TYPE = 2
except:
    OS_TYPE = 1

DEFAULT_LOG_FORMAT = "%(asctime)s - %(module)s|%(funcName)s - %(levelname)s (%(processName)s)[%(lineno)d] %(message)s"
LOG = logging.getLogger(__name__)


def es_data_sender(send_q, cmd_q, url, username, password, index_name, poll_interval=scanit.DEFAULT_POLL_INTERVAL):
    es_client = elasticsearchlite.ElasticsearchLite()
    es_client.username = username
    es_client.password = password
    es_client.endpoint = url
    file_idx = index_name + "_file"
    dir_idx = index_name + "_dir"

    while True:
        # Process our command queue
        try:
            cmd_item = cmd_q.get(block=False)
            cmd = cmd_item[0]
            if cmd == CMD_EXIT:
                break
        except queue.Empty:
            pass
        except Exception as e:
            LOG.exception(e)
        # Process send queue
        try:
            cmd_item = send_q.get(block=True, timeout=poll_interval)
            cmd = cmd_item[0]
            if cmd == CMD_EXIT:
                break
            elif cmd & (CMD_SEND | CMD_SEND_DIR):
                # TODO: Optimize this section by using a byte buffer and writing directly into the buffer?
                bulk_data = []
                work_items = cmd_item[1]
                for i in range(len(work_items)):
                    bulk_data.append(json.dumps({"index": {"_id": work_items[i]["inode"]}}))
                    bulk_data.append(json.dumps(work_items[i]))
                    work_items[i] = None
                bulk_str = "\n".join(bulk_data)
                resp = es_client.bulk(bulk_str, index_name=file_idx if cmd == CMD_SEND else dir_idx)
                if resp["errors"]:
                    for item in resp["items"]:
                        if item["index"]["status"] != 200:
                            LOG.error(json.dumps(item["index"]["error"]))
                del bulk_str
                del bulk_data
                del cmd_item
        except queue.Empty:
            pass
        except socket.gaierror as gaie:
            LOG.critical("Elasticsearch URL is invalid")
            break
        except Exception as e:
            LOG.exception(e)
            break


def es_init_index(url, username, password, index):
    if index[-1] == "_":
        index = index[0:-1]
    LOG.debug("Creating new indices with mapping: {idx}_file and {idx}_dir".format(idx=index))
    es_client = elasticsearchlite.ElasticsearchLite()
    es_client.username = username
    es_client.password = password
    es_client.endpoint = url
    for i in [index + "_file", index + "_dir"]:
        resp = es_client.create_index(i, mapping=PS_SCAN_MAPPING)
        LOG.debug("Create index response: %s" % resp)
        if resp.get("status", 200) != 200:
            if resp["error"]["type"] == "resource_already_exists_exception":
                LOG.debug("Index {idx} already exists".format(idx=i))
            else:
                LOG.error(json.dumps(resp["error"]))


def file_handler_basic(root, filename_list, stats, args={}):
    """
    The file handler returns a dictionary:
    {
      "processed": <int>                # Number of files actually processed
      "skipped": <int>                  # Number of files skipped
      "q_dirs": [<str>]                 # List of directory names that need processing
    }
    """
    custom_state = args.get("custom_state", {})
    start_time = args.get("start_time", time.time())
    thread_state = args.get("thread_state", {})

    custom_tagging = custom_state.get("custom_tagging", None)
    max_send_q_size = custom_state.get("max_send_q_size", DEFAULT_MAX_Q_SIZE)
    send_q_sleep = custom_state.get("send_q_sleep", DEFAULT_SEND_Q_SLEEP)
    send_to_es = custom_state.get("send_to_es", False)

    processed = 0
    skipped = 0
    dir_list = []
    result_list = []
    result_dir_list = []

    for filename in filename_list:
        full_path = os.path.join(root, filename)
        try:
            btime = fstats.st_birthtime
            btime_date = datetime.date.fromtimestamp(fstats.st_btime).isoformat()
        except:
            btime = 0
            btime_date = ""
        try:
            fstats = os.lstat(full_path)
            file_info = {
                "atime": fstats.st_atime,
                "atime_date": datetime.date.fromtimestamp(fstats.st_atime).isoformat(),
                "btime": btime,
                "btime_date": btime_date,
                "ctime": fstats.st_ctime,
                "ctime_date": datetime.date.fromtimestamp(fstats.st_ctime).isoformat(),
                "file_ext": os.path.splitext(filename),
                "file_name": filename,
                "gid": fstats.st_gid,
                "hard_links": fstats.st_nlink,
                "inode": fstats.st_ino,
                "logical_size": fstats.st_size,
                "mtime": fstats.st_mtime,
                "mtime_date": datetime.date.fromtimestamp(fstats.st_mtime).isoformat(),
                # Physical size without metadata
                "physical_size": fstats.st_blocks * IFS_BLOCK_SIZE,
                "file_path": root,
                "type": FILE_TYPE[stat.S_IFMT(fstats.st_mode) & FILE_TYPE_MASK],
                "uid": fstats.st_uid,
                "perms_unix": stat.S_IMODE(fstats.st_mode),
            }
            if custom_tagging:
                # DEBUG: Add custom user tags
                # file_info["tags"] = ["tag1", "othertag"]
                # file_info["tags"] = custom_tagging(file_info)
                pass
            if stat.S_ISDIR(fstats.st_mode):
                result_dir_list.append(file_info)
                # Save directories to re-queue
                dir_list.append(filename)
                continue
            stats["file_size_total"] += fstats.st_size
            processed += 1
            result_list.append(file_info)
        except Exception as e:
            skipped += 1
            LOG.exception(e)
    if (result_list or result_dir_list) and send_to_es:
        if result_list:
            custom_state["send_q"].put([CMD_SEND, result_list])
        if result_dir_list:
            custom_state["send_q"].put([CMD_SEND_DIR, result_dir_list])
        for i in range(DEFAULT_MAX_Q_WAIT_LOOPS):
            if custom_state["send_q"].qsize() > max_send_q_size:
                time.sleep(send_q_sleep)
            else:
                break
    return {"processed": processed, "skipped": skipped, "q_dirs": dir_list}


def file_handler_pscale(root, filename_list, stats, args={}):
    """
    The file handler returns a dictionary:
    {
      "processed": <int>                # Number of files actually processed
      "skipped": <int>                  # Number of files skipped
      "q_dirs": [<str>]                 # List of directory names that need processing
    }
    """
    custom_state = args.get("custom_state", {})
    start_time = args.get("start_time", time.time())
    thread_state = args.get("thread_state", {})

    custom_tagging = custom_state.get("custom_tagging", None)
    extra_attr = custom_state.get("extra_attr", False)
    user_attr = custom_state.get("user_attr", False)
    max_send_q_size = custom_state.get("max_send_q_size", DEFAULT_MAX_Q_SIZE)
    phys_block_size = custom_state.get("phys_block_size", IFS_BLOCK_SIZE)
    pool_translate = custom_state.get("node_pool_translation", {})
    send_q_sleep = custom_state.get("send_q_sleep", DEFAULT_SEND_Q_SLEEP)
    send_to_es = custom_state.get("send_to_es", False)

    processed = 0
    skipped = 0
    dir_list = []
    result_list = []
    result_dir_list = []

    for filename in filename_list:
        full_path = os.path.join(root, filename)
        fd = None
        try:
            fd = os.open(full_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_OPENLINK)
            fstats = attr.get_dinode(fd)
            # atime call can return empty if the file does not have an atime or atime tracking is disabled
            atime = attr.get_access_time(fd)
            if atime:
                atime = atime[0]
            else:
                # If atime does not exist, use the last metadata change time as this capture the last time someone
                # modified either the data or the inode of the file
                atime = fstats["di_ctime"]
            logical_blocks = fstats["di_logical_size"] // phys_block_size
            comp_blocks = logical_blocks - fstats["di_shadow_refs"]
            compressed_file = True if (fstats["di_data_blocks"] and comp_blocks) else False
            stubbed_file = (fstats["di_flags"] & IFLAG_COMBO_STUBBED) > 0
            file_info = {
                # ========== Timestamps ==========
                "atime": atime,
                "atime_date": datetime.date.fromtimestamp(atime).isoformat(),
                "btime": fstats["di_create_time"],
                "btime_date": datetime.date.fromtimestamp(fstats["di_create_time"]).isoformat(),
                "ctime": fstats["di_ctime"],
                "ctime_date": datetime.date.fromtimestamp(fstats["di_ctime"]).isoformat(),
                "mtime": fstats["di_mtime"],
                "mtime_date": datetime.date.fromtimestamp(fstats["di_mtime"]).isoformat(),
                # ========== File and path strings ==========
                "file_path": root,
                "file_name": filename,
                "file_ext": os.path.splitext(filename)[1],
                # ========== File attributes ==========
                "file_access_pattern": ACCESS_PATTERN[fstats["di_la_pattern"]],
                "file_compression_ratio": comp_blocks / fstats["di_data_blocks"] if compressed_file else 1,
                "file_hard_links": fstats["di_nlink"],
                "file_is_ads": ((fstats["di_flags"] & IFLAGS_UF_HASADS) != 0),
                "file_is_compressed": (comp_blocks > fstats["di_data_blocks"]) if compressed_file else False,
                "file_is_dedupe_disabled": not not fstats["di_no_dedupe"],
                "file_is_deduped": (fstats["di_shadow_refs"] > 0),
                "file_is_inlined": (
                    (fstats["di_physical_blocks"] == 0)
                    and (fstats["di_shadow_refs"] == 0)
                    and (fstats["di_logical_size"] > 0)
                ),
                "file_is_packed": not not fstats["di_packing_policy"],
                "file_is_smartlinked": stubbed_file,
                "file_is_sparse": ((fstats["di_logical_size"] < fstats["di_size"]) and not stubbed_file),
                "file_type": FILE_TYPE[fstats["di_mode"] & FILE_TYPE_MASK],
                "inode": fstats["di_lin"],
                "inode_mirror_count": fstats["di_inode_mc"],
                "inode_parent": fstats["di_parent_lin"],
                "inode_revision": fstats["di_rev"],
                # ========== Storage pool targets ==========
                "pool_target_data": fstats["di_data_pool_target"],
                "pool_target_data_name": pool_translate.get(
                    int(fstats["di_data_pool_target"]), str(fstats["di_data_pool_target"])
                ),
                "pool_target_metadata": fstats["di_metadata_pool_target"],
                "pool_target_metadata_name": pool_translate.get(
                    int(fstats["di_metadata_pool_target"]), str(fstats["di_metadata_pool_target"])
                ),
                # ========== Permissions ==========
                "perms_gid": fstats["di_gid"],
                "perms_uid": fstats["di_uid"],
                "perms_unix": stat.S_IMODE(fstats["di_mode"]),
                # ========== File protection level ==========
                "protection_current": fstats["di_current_protection"],
                "protection_target": fstats["di_protection_policy"],
                # ========== File allocation size and blocks ==========
                # The apparent size of the file. Sparse files include the sparse area
                "size": fstats["di_size"],
                # Logical size in 8K blocks. Sparse files only show the real data portion
                "size_logical": fstats["di_logical_size"],
                # Physical size on disk including protection overhead and excluding metadata
                "size_physical": fstats["di_physical_blocks"] * phys_block_size,
                # Physical size on disk excluding protection overhead and excluding metadata
                "size_physical_data": fstats["di_data_blocks"] * phys_block_size,
                # Physical size on disk of the protection overhead
                "size_protection": fstats["di_protection_blocks"] * phys_block_size,
                # ========== SSD usage ==========
                "ssd_strategy": fstats["di_la_ssd_strategy"],
                "ssd_strategy_name": SSD_STRATEGY[fstats["di_la_ssd_strategy"]],
                "ssd_status": fstats["di_la_ssd_status"],
                "ssd_status_name": SSD_STATUS[fstats["di_la_ssd_status"]],
            }
            if extra_attr:
                # di_flags may have other bits we need to translate
                #     Coalescer setting (on|off|endurant all|coalescer only)
                #     IFLAGS_UF_WRITECACHE and IFLAGS_UF_WC_ENDURANT flags
                # Still need ACL
                # Still need SID
                # Do we want inode locations? how many on SSD and spinning disk?
                #   - Get data from estats["ge_iaddrs"], e.g. ge_iaddrs: [(1, 13, 1098752, 512)]
                # Extended attributes/custom attributes?
                estats = attr.get_expattr(fd)
                # Add up all the inode sizes
                metadata_size = 0
                for inode in estats["ge_iaddrs"]:
                    metadata_size += inode[3]
                # Sum of the size of all the inodes. This includes inodes that mix both 512 byte and 8192 byte inodes
                file_info["size_metadata"] = metadata_size
                file_info["file_is_manual_access"] = not not estats["ge_manually_manage_access"]
                file_info["file_is_manual_packing"] = not not estats["ge_manually_manage_packing"]
                file_info["file_is_manual_protection"] = not not estats["ge_manually_manage_protection"]
            if user_attr:
                extended_attr = {}
                keys = uattr.userattr_list(fd)
                for key in keys:
                    extended_attr[key] = uattr.userattr_get(fd, key)
                file_info["user_attributes"] = extended_attr
            if custom_tagging:
                # DEBUG: Add custom user tags
                # file_info["user_tags"] = ["tag1", "othertag"]
                # file_info["user_tags"] = custom_tagging(file_info)
                pass
            if fstats["di_mode"] & 0o040000:
                result_dir_list.append(file_info)
                # Fix size issues with dirs
                file_info["size_logical"] = 0
                # Save directories to re-queue
                dir_list.append(filename)
                continue
            else:
                result_list.append(file_info)
                if fstats["di_mode"] & 0o120000:
                    # Fix size issues with symlinks
                    file_info["size_logical"] = 0
            stats["file_size_total"] += fstats["di_size"]
            processed += 1
        except Exception as e:
            skipped += 1
            LOG.exception(e)
        finally:
            try:
                os.close(fd)
            except:
                pass
    if (result_list or result_dir_list) and send_to_es:
        if result_list:
            custom_state["send_q"].put([CMD_SEND, result_list])
        if result_dir_list:
            custom_state["send_q"].put([CMD_SEND_DIR, result_dir_list])
        for i in range(DEFAULT_MAX_Q_WAIT_LOOPS):
            if custom_state["send_q"].qsize() > max_send_q_size:
                time.sleep(send_q_sleep)
            else:
                break
    return {"processed": processed, "skipped": skipped, "q_dirs": dir_list}


def init_custom_state(custom_state, options):
    # TODO: Parse the custom tag input file and produce a parser
    custom_state["custom_tagging"] = None
    custom_state["extra_attr"] = options.extra
    custom_state["max_send_q_size"] = options.es_max_send_q_size
    custom_state["node_pool_translation"] = {}
    custom_state["phys_block_size"] = IFS_BLOCK_SIZE
    custom_state["send_q"] = queue.Queue()
    custom_state["send_q_sleep"] = options.es_send_q_sleep
    custom_state["send_to_es"] = options.es_user and options.es_pass and options.es_url and options.es_index
    custom_state["user_attr"] = options.user_attr
    if OS_TYPE == 2:
        # Query the cluster for node pool name information
        dpdb = dp.DiskPoolDB()
        groups = dpdb.get_groups()
        for g in groups:
            children = g.get_children()
            for child in children:
                custom_state["node_pool_translation"][int(child.entryid)] = g.name


def print_statistics(stats, num_threads, wall_time, es_time):
    print("Wall time (s): {tm:.2f}".format(tm=wall_time))
    print("Time to send remaining data to Elasticsearch (s): {t:.2f}".format(t=es_time))
    print("Average Q wait time (s): {dt:.2f}".format(dt=stats["q_wait_time"] / num_threads))
    print(
        "Total time spent in dir/file handler routines across all threads (s): {dht:.2f}/{fht:.2f}".format(
            dht=stats.get("dir_handler_time", 0),
            fht=stats.get("file_handler_time", 0),
        )
    )
    print(
        "Processed/Queued/Skipped dirs: {p_dirs}/{q_dirs}/{s_dirs}".format(
            p_dirs=stats.get("dirs_processed", 0),
            q_dirs=stats.get("dirs_queued", 0),
            s_dirs=stats.get("dirs_skipped", 0),
        )
    )
    print(
        "Processed/Queued/Skipped files: {p_files}/{q_files}/{s_files}".format(
            p_files=stats.get("files_processed", 0),
            q_files=stats.get("files_queued", 0),
            s_files=stats.get("files_skipped", 0),
        )
    )
    print("Total file size: {fsize}".format(fsize=stats.get("file_size_total", 0)))
    print(
        "Avg files/second: {a_fps}".format(
            a_fps=(stats.get("files_processed", 0) + stats.get("files_skipped", 0)) / wall_time
        )
    )


def subprocess(process_state, scan_paths, file_handler, options):
    global LOG
    LOG = logging.getLogger()
    setup_logger(LOG, options)
    LOG.debug("Process loop started: {pid}".format(pid=mp.current_process()))

    # Setup process local variables
    start_wall = time.time()
    # DEBUG: Need to get options values for cmd_poll_interval, dir_output_interval, dir_request_interval
    cmd_poll_interval = 0.01
    dir_output_count = 0
    dir_output_interval = 2
    dir_request_interval = 2
    es_send_thread_handles = []
    poll_interval = scanit.DEFAULT_POLL_INTERVAL
    stats_output_count = 0
    stats_output_interval = options.stats_interval
    send_data_to_es = options.es_user and options.es_pass and options.es_url and options.es_index

    # Initialize and start the scanner
    scanner = scanit.ScanIt()
    cstates = scanner.get_custom_state()
    init_custom_state(cstates[0], options)
    scanner.handler_file = file_handler
    scanner.num_threads = process_state.get("num_threads", DEFAULT_THREAD_COUNT)
    scanner.processing_type = scanit.PROCESS_TYPE_ADVANCED
    # DEBUG: scanner.exit_on_idle = True
    scanner.exit_on_idle = False
    if options.advanced:
        poll_interval = options.q_poll_interval
        scanner.dir_chunk = options.dirq_chunk
        scanner.dir_priority_count = options.dirq_priority
        scanner.file_chunk = options.fileq_chunk
        scanner.file_q_cutoff = options.fileq_cutoff
        scanner.file_q_min_cutoff = options.fileq_min_cutoff
    scanner.add_scan_path(scan_paths)
    scanner.start()

    # Setup our own process state and inform the parent process of our status
    process_state["status"] = "running"
    process_state["dirs_requested"] = 0
    conn_pipe = process_state["child_conn"]
    conn_pipe.send([CMD_STATUS_RUN, None])
    # Send empty stats to the parent process to populate the stats data
    conn_pipe.send([CMD_SEND_STATS, scanner.get_stats()])

    # Start Elasticsearch send threads
    if send_data_to_es:
        es_send_cmd_q = queue.Queue()
        send_q = scanner.get_custom_state()[0]["send_q"]
        for i in range(options.es_threads):
            es_thread_instance = threading.Thread(
                target=es_data_sender,
                args=(
                    send_q,
                    es_send_cmd_q,
                    options.es_url,
                    options.es_user,
                    options.es_pass,
                    options.es_index,
                ),
            )
            es_thread_instance.start()
            es_send_thread_handles.append(es_thread_instance)

    # Main processing loop
    try:
        while True:
            now = time.time()
            cur_dir_q_size = scanner.get_dir_queue_size()
            # Check for any commands from the parent process
            data_avail = conn_pipe.poll(cmd_poll_interval)
            if data_avail:
                work_item = conn_pipe.recv()
                cmd = work_item[0]
                if cmd == CMD_EXIT:
                    scanner.terminate()
                    break
                elif cmd == CMD_SEND_DIR:
                    # Parent process has sent directories to process
                    LOG.critical("SEND_DIR - Got %s dirs to process" % (len(work_item[1])))
                    process_state["status"] = "running"
                    process_state["want_data"] = 0
                    scanner.add_scan_path(work_item[1])
                    conn_pipe.send([CMD_STATUS_RUN, 0])
                    # Update our current queue size
                    cur_dir_q_size = scanner.get_dir_queue_size()
                elif cmd == CMD_REQ_DIR:
                    # Need to return some directories if possible
                    dir_list = scanner.get_dir_queue_items(percentage=work_item[1])
                    if dir_list:
                        conn_pipe.send([CMD_SEND_DIR, dir_list, now])
                elif cmd == CMD_REQ_DIR_COUNT:
                    # Return the number of directory chunks we have queued for processing
                    dir_count = cur_dir_q_size
                    conn_pipe.send([CMD_SEND_DIR_COUNT, dir_count])
                else:
                    LOG.error("Unknown command received in process: {s}".format(s=cmd))
            # Determine if we should send a statistics update
            cur_stats_count = (now - start_wall) // stats_output_interval
            if cur_stats_count > stats_output_count:
                stats_output_count = cur_stats_count
                conn_pipe.send([CMD_SEND_STATS, scanner.get_stats()])
            # Determine if we should send a directory queue count update
            cur_dir_count = (now - start_wall) // dir_output_interval
            if cur_dir_count > dir_output_count:
                dir_output_count = cur_dir_count
                conn_pipe.send([CMD_SEND_DIR_COUNT, cur_dir_q_size])
            # Ask parent process for more data if required, limit data requests to dir_request_interval seconds
            if (cur_dir_q_size < DEFAULT_LOW_DIR_Q_THRESHOLD) and (now -  process_state["want_data"] > dir_request_interval):
                process_state["want_data"] = now
                conn_pipe.send([CMD_REQ_DIR, 0])
            # Check if the scanner is idle
            if not cur_dir_q_size and (process_state["status"] == "running") and not scanner.is_processing():
                process_state["status"] = "idle"
                conn_pipe.send([CMD_STATUS_IDLE, 0])
            # Small sleep to throttle polling
            time.sleep(poll_interval)
        LOG.debug("Scanner finished file scan")
        # Scanner is done processing. Wait for all the data to be sent to Elasticsearch
        if send_data_to_es:
            LOG.debug("Waiting for send queue to empty")
            send_start = now
            while not send_q.empty():
                time.sleep(poll_interval)
            es_send_q_time = now - send_start
            LOG.debug("Sending exit command to send queue")
            for thread_handle in es_send_thread_handles:
                es_send_cmd_q.put([CMD_EXIT, None])
            for thread_handle in es_send_thread_handles:
                thread_handle.join()
    except KeyboardInterrupt as kbe:
        LOG.debug("Enumerate threads: %s" % threading.enumerate())
        print("Termination signal received. Shutting down scanner.")
    finally:
        for thread_handle in es_send_thread_handles:
            es_send_cmd_q.put([CMD_EXIT, None])
        scanner.terminate(True)

    # Send statistics back to parent and end process
    # DEBUG: Need to add these values to the stats being sent back
    total_wall_time = time.time() - start_wall
    if not send_data_to_es:
        es_send_q_time = 0
    conn_pipe.send([CMD_SEND_STATS, scanner.get_stats()])
    conn_pipe.send([CMD_EXIT, None])
    LOG.debug("Process loop ending: {pid}".format(pid=mp.current_process()))


def merge_process_stats(process_states):
    temp_stats = None
    for state in process_states:
        if not state["stats"]:
            # No stats for this process yet
            continue
        if temp_stats is None and state["stats"]:
            temp_stats = copy.deepcopy(state["stats"])
            continue
        for key in temp_stats.keys():
            if key in state["stats"]:
                temp_stats[key] += state["stats"][key]
    return temp_stats


def setup_logger(log_obj, options):
    debug_count = options.debug
    if (options.log is None) and (not options.quiet):
        options.console_log = True
    if debug_count > 0:
        LOG.setLevel(logging.DEBUG)
    else:
        LOG.setLevel(logging.INFO)
    if options.console_log:
        log_handler = logging.StreamHandler()
        log_handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT))
        LOG.addHandler(log_handler)
    if options.log:
        log_handler = logging.FileHandler(options.log)
        log_handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT))
        LOG.addHandler(log_handler)
    if (options.log is None) and (options.console_log is False):
        LOG.addHandler(logging.NullHandler())


def chunk_path_list(list_data, chunks):
    chunk_list = [[] for x in range(chunks)]
    chunk_size = (len(list_data) // chunks) + 1 * (len(list_data) % chunks != 0)
    for i in range(0, len(list_data), chunk_size):
        chunk_list[i] = list_data[i : i + chunk_size]
    return chunk_list


def main():
    # Setup command line parser and parse agruments
    (parser, options, args) = parse_cli(sys.argv, __version__, __date__)

    # Setup logging
    setup_logger(LOG, options)

    # Validate command line options
    cmd_line_errors = []
    if len(args) == 0:
        cmd_line_errors.append("***** A minimum of 1 path to scan is required to be specified on the command line.")
    if options.type is None:
        cmd_line_errors.append("***** You must specify a scan type with the --type option.")
    if cmd_line_errors:
        parser.print_help()
        print("\n" + "\n".join(cmd_line_errors))
        sys.exit(1)
    if options.es_cred_file:
        try:
            with open(options.es_cred_file) as f:
                lines = f.readlines()
                options.es_user = lines[0].strip()
                options.es_pass = lines[1].strip()
                if len(lines) > 2:
                    options.es_index = lines[2].strip()
                if len(lines) > 3:
                    options.es_url = lines[3].strip()
        except:
            pass
    if options.type == "onefs":
        if OS_TYPE != 2:
            print("Script is not running on a OneFS operation system. Invalid --type option, use 'basic' instead.")
            sys.exit(2)
        file_handler = file_handler_pscale
    else:
        file_handler = file_handler_basic

    if options.es_init_index and options.es_user and options.es_pass and options.es_url and options.es_index:
        es_init_index(options.es_url, options.es_user, options.es_pass, options.es_index)

    # Start scanning processes
    start_wall = time.time()
    process_states = []
    stats_output_count = 0
    stats_output_interval = options.stats_interval
    request_work_interval = 2
    request_work_dirq_percentage = 0.5
    num_procs = options.threads // options.threads_per_proc + 1 * (options.threads % options.threads_per_proc != 0)
    base_threads = options.threads // num_procs
    thread_count = options.threads
    scan_path_chunks = chunk_path_list(args, num_procs)
    LOG.debug(
        "Starting {count} processes with {threads} threads across all processes".format(
            count=num_procs, threads=options.threads
        )
    )
    for i in range(num_procs):
        parent_conn, child_conn = mp.Pipe()
        process_state = {
            "dir_count": 0,
            "id": i + 1,
            "stats_time": None,
            "stats": {},
            "status": "starting",
            "parent_conn": parent_conn,
            "child_conn": child_conn,
            "threads": base_threads if thread_count > base_threads else thread_count,
            "want_data": time.time(),
            "data_requested": 0,
        }
        proc_handle = mp.Process(
            target=subprocess,
            args=(
                process_state,
                scan_path_chunks[i],
                file_handler,
                options,
            ),
        )
        thread_count -= base_threads
        process_state["handle"] = proc_handle
        process_states.append(process_state)
        LOG.debug("Starting process: {num}".format(num=i))
        proc_handle.start()
    LOG.debug("All processes started")

    print("Statistics interval: {si} seconds".format(si=options.stats_interval))
    # Main loop
    #   * Check for any commands from the sub processes
    #   * Output statistics
    #   * Check if we should exit
    dir_list = []
    continue_running = True
    while continue_running:
        try:
            now = time.time()
            # Check if there are any commands coming from the sub processes
            idle_proc = 0
            for proc in process_states:
                data_avail = True
                # DEBUG: Change to a select call to handle sockets
                while data_avail:
                    data_avail = proc["parent_conn"].poll(0.001)
                    if not data_avail:
                        continue
                    work_item = proc["parent_conn"].recv()
                    cmd = work_item[0]
                    LOG.debug("Process cmd received ({pid}): 0x{cmd:x}".format(pid=proc["id"], cmd=cmd))
                    if cmd == CMD_EXIT:
                        proc["status"] = "stopped"
                    elif cmd == CMD_SEND_STATS:
                        proc["stats"] = work_item[1]
                        proc["stats_time"] = now
                    elif cmd == CMD_REQ_DIR:
                        # A child process is requesting directories to process
                        proc["want_data"] = now
                        LOG.critical("ROOT - PROCESS (%s) wants directories" % proc["id"])
                    elif cmd == CMD_SEND_DIR:
                        dir_list.extend(work_item[1])
                        proc["data_requested"] = 0
                        proc["want_data"] = 0
                    elif cmd == CMD_SEND_DIR_COUNT:
                        proc["dir_count"] = work_item[1]
                    elif cmd == CMD_STATUS_IDLE:
                        proc["status"] = "idle"
                        proc["want_data"] = now
                    elif cmd == CMD_STATUS_RUN:
                        proc["status"] = "running"
                if proc["status"] == "idle":
                    idle_proc += 1

            # Output statistics
            #   The -1 is for a 1 second offset to allow time for stats to come from processes
            cur_stats_count = (now - start_wall - 1) // stats_output_interval
            if cur_stats_count > stats_output_count:
                temp_stats = merge_process_stats(process_states) or {}
                print(
                    "{ts}: FPS:{fps:0.0f} - {stats}".format(
                        ts=datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"),
                        fps=temp_stats.get("files_processed", 0) / (now - start_wall),
                        stats=temp_stats,
                    )
                )
                stats_output_count = cur_stats_count
            # Check if we should exit
            continue_running = False
            for proc in process_states:
                if proc["status"] != "stopped":
                    continue_running = True
                    break
            # Check if we can terminate all the scanner processes
            if idle_proc == num_procs:
                # All sub processes are idle so we can quit the scanner
                for proc in process_states:
                    if proc["status"] != "exiting":
                        proc["parent_conn"].send([CMD_EXIT, 0])
                        proc["status"] = "exiting"
                # Skip any further processing and just wait for processes to end
                continue
            # Check if we need to request or send any directories to existing processes
            want_work_procs = []
            have_dirs_procs = []
            for proc in process_states:
                if proc["want_data"]:
                    want_work_procs.append(proc)
                if proc["dir_count"]:
                    have_dirs_procs.append(proc)
            if want_work_procs and dir_list:
                # Send out our directories to all processes that want work evenly
                want_work_count = len(want_work_procs)
                increment = len(dir_list) // want_work_count + 1 * (len(dir_list) % want_work_count != 0)
                index = 0
                for proc in want_work_procs:
                    work_dirs = dir_list[index : index + increment]
                    if not work_dirs:
                        continue
                    proc["parent_conn"].send([CMD_SEND_DIR, work_dirs])
                    proc["want_data"] = 0
                    index += increment
                    want_work_procs.remove(proc)
                    del dir_list
                    dir_list = []
            if want_work_procs and have_dirs_procs:
                for proc in have_dirs_procs:
                    # Limit the number of times we request data from each process to request_work_interval seconds
                    if (now - proc["data_requested"]) > request_work_interval:
                        proc["parent_conn"].send([CMD_REQ_DIR, request_work_dirq_percentage])
                        proc["data_requested"] = now
            # Sleep for a short interval to slow down polling
            time.sleep(poll_interval)
        except KeyboardInterrupt as kbe:
            print("***** MAIN THREAD ***** Termination signal received. Shutting down scanner.")
            break

    total_wall_time = time.time() - start_wall
    temp_stats = merge_process_stats(process_states)
    # DEBUG: Need to add ES Send Q Time back in
    print_statistics(temp_stats, options.threads, total_wall_time, 0)


if __name__ == "__main__" or __file__ == None:
    # Support scripts built into executable on Windows
    mp.freeze_support()
    if hasattr(mp, "set_start_method"):
        # Force all OS to behave the same when spawning new process
        mp.set_start_method("spawn")
    main()
