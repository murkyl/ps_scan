#!/usr/bin/env python
# -*- coding: utf8 -*-
# fmt: off
__title__         = "ps_scan"
__version__       = "1.0.0"
__date__          = "16 March 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
# fmt: on

import datetime
import elasticsearchlite
import json
import logging
import os
import queue
import socket
import stat
import sys
import scanit
import threading
import time
from helpers.cli_parser import *
from helpers.constants import *

try:
    import isi.fs.attr as attr
    import isi.fs.diskpool as dp
    import isi.fs.userattr as uattr

    OS_TYPE = 2
except:
    OS_TYPE = 1

DEFAULT_LOG_FORMAT = "%(asctime)s - %(module)s|%(funcName)s - %(levelname)s [%(lineno)d] %(message)s"
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
                "extension": os.path.splitext(filename),
                "filename": filename,
                "gid": fstats.st_gid,
                "hard_links": fstats.st_nlink,
                "inode": fstats.st_ino,
                "logical_size": fstats.st_size,
                "mtime": fstats.st_mtime,
                "mtime_date": datetime.date.fromtimestamp(fstats.st_mtime).isoformat(),
                # Physical size without metadata
                "physical_size": fstats.st_blocks * IFS_BLOCK_SIZE,
                "root": root,
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
    extra_stats = custom_state.get("extra_stats", False)
    get_user_attr = custom_state.get("get_user_attr", False)
    max_send_q_size = custom_state.get("max_send_q_size", DEFAULT_MAX_Q_SIZE)
    phys_block_size = custom_state.get("phys_block_size", IFS_BLOCK_SIZE)
    pool_translate = custom_state.get("node_pool_translation", {})
    send_q_sleep = custom_state.get("send_q_sleep", DEFAULT_SEND_Q_SLEEP)
    sent_to_es = custom_state.get("send_to_es", False)

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
                "root": root,
                "filename": filename,
                "extension": os.path.splitext(filename)[1],
                # ========== File attributes ==========
                "access_pattern": ACCESS_PATTERN[fstats["di_la_pattern"]],
                "file_compressed": (comp_blocks > fstats["di_data_blocks"]) if compressed_file else False,
                "file_compression_ratio": comp_blocks / fstats["di_data_blocks"] if compressed_file else 1,
                # File dedupe disabled - (0: can dedupe, 1: do not dedupe)
                "file_dedupe_disabled": not not fstats["di_no_dedupe"],
                # Assume the file is fully/partially deduped if it has shadow store references
                "file_deduped": (fstats["di_shadow_refs"] > 0),
                "file_has_ads": ((fstats["di_flags"] & IFLAGS_UF_HASADS) != 0),
                "file_inlined": (
                    (fstats["di_physical_blocks"] == 0)
                    and (fstats["di_shadow_refs"] == 0)
                    and (fstats["di_logical_size"] > 0)
                ),
                "file_packed": not not fstats["di_packing_policy"],
                "file_smartlinked": stubbed_file,
                "file_sparse": ((fstats["di_logical_size"] < fstats["di_size"]) and not stubbed_file),
                "file_type": FILE_TYPE[fstats["di_mode"] & FILE_TYPE_MASK],
                "hard_links": fstats["di_nlink"],
                "inode": fstats["di_lin"],
                "inode_mirror_count": fstats["di_inode_mc"],
                "inode_parent": fstats["di_parent_lin"],
                # Number of times the inode has been modified. An indicator of file change rate. Starts at 2
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
            if extra_stats:
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
                file_info["manual_access"] = not not estats["ge_manually_manage_access"]
                file_info["manual_packing"] = not not estats["ge_manually_manage_packing"]
                file_info["manual_protection"] = not not estats["ge_manually_manage_protection"]
            if get_user_attr:
                extended_attr = {}
                keys = uattr.userattr_list(fd)
                for key in keys:
                    extended_attr[key] = uattr.userattr_get(fd, key)
                file_info["user_attributes"] = extended_attr
            if custom_tagging:
                # DEBUG: Add custom user tags
                # file_info["tags"] = ["tag1", "othertag"]
                # file_info["tags"] = custom_tagging(file_info)
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
    if (result_list or result_dir_list) and sent_to_es:
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
    custom_state["extra_stats"] = options.extra
    custom_state["get_user_attr"] = options.user_attr
    custom_state["max_send_q_size"] = options.es_max_send_q_size
    custom_state["node_pool_translation"] = {}
    custom_state["phys_block_size"] = IFS_BLOCK_SIZE
    custom_state["send_q"] = queue.Queue()
    custom_state["send_q_sleep"] = options.es_send_q_sleep
    custom_state["send_to_es"] = options.es_user and options.es_pass and options.es_url and options.es_index
    if OS_TYPE == 2:
        # Query the cluster for node pool name information
        dpdb = dp.DiskPoolDB()
        groups = dpdb.get_groups()
        for g in groups:
            children = g.get_children()
            for child in children:
                custom_state["node_pool_translation"][int(child.entryid)] = g.name


def print_statistics(sstats, num_threads, wall_time, es_time):
    print("Wall time (s): {tm:.2f}".format(tm=wall_time))
    print("Time to send remaining data to Elasticsearch (s): {t:.2f}".format(t=es_time))
    print("Average Q wait time (s): {dt:.2f}".format(dt=sstats["q_wait_time"] / num_threads))
    print(
        "Total time spent in dir/file handler routines across all threads (s): {dht:.2f}/{fht:.2f}".format(
            dht=sstats["dir_handler_time"],
            fht=sstats["file_handler_time"],
        )
    )
    print(
        "Processed/Queued/Skipped dirs: {p_dirs}/{q_dirs}/{s_dirs}".format(
            p_dirs=sstats["dirs_processed"],
            q_dirs=sstats["dirs_queued"],
            s_dirs=sstats["dirs_skipped"],
        )
    )
    print(
        "Processed/Queued/Skipped files: {p_files}/{q_files}/{s_files}".format(
            p_files=sstats["files_processed"],
            q_files=sstats["files_queued"],
            s_files=sstats["files_skipped"],
        )
    )
    print("Total file size: {fsize}".format(fsize=sstats["file_size_total"]))
    print("Avg files/second: {a_fps}".format(a_fps=(sstats["files_processed"] + sstats["files_skipped"]) / wall_time))


def main():
    (parser, options, args) = parse_cli(sys.argv, __version__, __date__)

    # Setup logging
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

    # Parse arguments
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
    if options.es_user and options.es_pass and options.es_url and options.es_index:
        send_data_to_es = True
    else:
        send_data_to_es = False
    if options.type == "onefs":
        if OS_TYPE != 2:
            print("Script is not running on a OneFS operation system. Invalid command lines options.")
            sys.exit(2)
        file_handler = file_handler_pscale
    else:
        file_handler = file_handler_basic
    scan_paths = list(args)

    # Start main loop
    start_wall = time.time()
    es_send_thread_handles = []
    stats_output_count = 0
    stats_output_interval = options.stats_interval
    poll_interval = scanit.DEFAULT_POLL_INTERVAL
    # Initialize and start the scanner
    scanner = scanit.ScanIt()
    cstates = scanner.get_custom_state()
    init_custom_state(cstates[0], options)
    scanner.handler_file = file_handler
    scanner.num_threads = options.threads
    scanner.processing_type = scanit.PROCESS_TYPE_ADVANCED
    scanner.exit_on_idle = True
    if options.advanced:
        poll_interval = options.q_poll_interval
        scanner.dir_chunk = options.dirq_chunk
        scanner.dir_priority_count = options.dirq_priority
        scanner.file_chunk = options.fileq_chunk
        scanner.file_q_cutoff = options.fileq_cutoff
        scanner.file_q_min_cutoff = options.fileq_min_cutoff
    scanner.add_scan_path(scan_paths)
    scanner.start()
    # Start Elasticsearch send thread
    if send_data_to_es:
        if options.es_init_index:
            es_init_index(options.es_url, options.es_user, options.es_pass, options.es_index)
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
    try:
        print("Statistics interval: {si} seconds".format(si=stats_output_interval))
        # For a very simple run until completion, just do a join like below
        # scanner.join()
        # If you want to get status updates then use the while loop below
        while scanner.is_processing():
            cur_stats_count = (time.time() - start_wall) // stats_output_interval
            if cur_stats_count > stats_output_count:
                temp_stats = scanner.get_stats()
                print(
                    "{ts}: ({pc}) - FPS:{fps:0.0f} - {stats}".format(
                        ts=datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"),
                        pc=scanner.is_processing(),
                        fps=temp_stats["files_processed"] / (time.time() - start_wall),
                        stats=temp_stats,
                    )
                )
                stats_output_count = cur_stats_count
            time.sleep(poll_interval)
        LOG.debug("Scanner finished file scan")
        if send_data_to_es:
            # Scanner is done processing. Wait for all the data to be sent to Elasticsearch
            LOG.debug("Waiting for send queue to empty")
            send_start = time.time()
            while not send_q.empty():
                time.sleep(poll_interval)
            es_send_q_time = time.time() - send_start
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
    total_wall_time = time.time() - start_wall
    if not send_data_to_es:
        es_send_q_time = 0
    # =========== Output results ============
    sstats = scanner.get_stats()
    print_statistics(sstats, scanner.num_threads, total_wall_time, es_send_q_time)


if __name__ == "__main__" or __file__ == None:
    main()
