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
    "file_handler_basic",
    "file_handler_pscale",
    "init_custom_state",
    "init_thread",
    "print_statistics",
    "update_config",
]
# fmt: on
import datetime
import json
import logging
import os
import queue
import stat
import sys
import threading
import time

import elasticsearch_wrapper
import helpers.misc as misc
from helpers.constants import *

try:
    import helpers.onefs_acl as onefs_acl
    import isi.fs.attr as attr
    import isi.fs.diskpool as dp
    import isi.fs.userattr as uattr
except:
    pass
try:
    dir(PermissionError)
except:
    PermissionError = Exception
try:
    dir(FileNotFoundError)
except:
    FileNotFoundError = IOError


LOG = logging.getLogger(__name__)
CUSTOM_STATS_FIELDS = [
    "es_queue_time",
    "es_queue_wait_count",
    "file_not_found",
    "get_access_time_time",
    "get_acl_time",
    "get_custom_tagging_time",
    "get_dinode_time",
    "get_extra_attr_time",
    "get_user_attr_time",
    "lstat_time",
]


def custom_stats_handler(common_stats, custom_state, custom_threads_state, thread_state):
    # Access all the individual thread state dictionaries in the custom_threads_state array
    # These should be initialized in the init_thread routine
    LOG.debug("DEBUG: Custom stats handler called!")
    # LOG.debug(
    #    "DEBUG: Common stats: %s"
    #    % json.dumps(common_stats, indent=2, sort_keys=True, default=lambda o: "<not serializable>")
    # )
    # LOG.debug(
    #    "DEBUG: Custom state: %s"
    #    % json.dumps(custom_state, indent=2, sort_keys=True, default=lambda o: "<not serializable>")
    # )
    # LOG.debug(
    #    "DEBUG: Custom threads state: %s"
    #    % json.dumps(custom_threads_state, indent=2, sort_keys=True, default=lambda o: "<not serializable>")
    # )
    # LOG.debug(
    #    "DEBUG: Thread state: %s"
    #    % json.dumps(thread_state, indent=2, sort_keys=True, default=lambda o: "<not serializable>")
    # )
    num_threads = len(thread_state) or 1
    custom_stats = custom_state["custom_stats"]
    for field in CUSTOM_STATS_FIELDS:
        custom_stats[field] = 0
    for thread in thread_state:
        thread_custom_stats = thread["custom"]["stats"]
        for field in CUSTOM_STATS_FIELDS:
            custom_stats[field] += thread_custom_stats[field]
    for field in CUSTOM_STATS_FIELDS:
        custom_stats[field + "_avg"] = custom_stats[field] / num_threads
    return custom_stats


def file_handler_basic(root, filename_list, stats, now, args={}):
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
    thread_custom_state = args.get("thread_custom_state", {})
    thread_state = args.get("thread_state", {})

    custom_tagging = custom_state.get("custom_tagging", None)
    max_send_q_size = custom_state.get("max_send_q_size", DEFAULT_ES_MAX_Q_SIZE)
    send_q_sleep = custom_state.get("send_q_sleep", DEFAULT_ES_SEND_Q_SLEEP)

    processed = 0
    skipped = 0
    dir_list = []
    result_list = []
    result_dir_list = []

    for filename in filename_list:
        try:
            file_info = get_file_stat(root, filename)
            if custom_tagging:
                file_info["user_tags"] = custom_tagging(file_info)
            if file_info["file_type"] == "dir":
                file_info["_scan_time"] = now
                result_dir_list.append(file_info)
                # Save directories to re-queue
                dir_list.append(filename)
                continue
            stats["file_size_total"] += file_info["size"]
            processed += 1
            result_list.append(file_info)
        except FileNotFoundError as fnfe:
            skipped += 1
            LOG.info("File not found: {filename}".format(filename=filename))
        except Exception as e:
            skipped += 1
            LOG.exception(e)
    if (result_list or result_dir_list) and custom_state.get("es_send_q"):
        if result_list:
            custom_state["es_send_q"].put([CMD_SEND, result_list])
        if result_dir_list:
            custom_state["es_send_q"].put([CMD_SEND_DIR, result_dir_list])
        for i in range(DEFAULT_MAX_Q_WAIT_LOOPS):
            if custom_state["es_send_q"].qsize() > max_send_q_size:
                time.sleep(send_q_sleep)
            else:
                break
    return {"processed": processed, "skipped": skipped, "q_dirs": dir_list}


def file_handler_pscale(root, filename_list, stats, now, args={}):
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
    thread_custom_state = args.get("thread_custom_state", {})
    thread_state = args.get("thread_state", {})
    thread_stats = thread_state["custom"]["stats"]

    custom_tagging = custom_state.get("custom_tagging", None)
    extra_attr = custom_state.get("extra_attr", False)
    user_attr = custom_state.get("user_attr", False)
    max_send_q_size = custom_state.get("max_send_q_size", DEFAULT_ES_MAX_Q_SIZE)
    no_acl = custom_state.get("no_acl", None)
    phys_block_size = custom_state.get("phys_block_size", IFS_BLOCK_SIZE)
    pool_translate = custom_state.get("node_pool_translation", {})
    send_q_sleep = custom_state.get("send_q_sleep", DEFAULT_ES_SEND_Q_SLEEP)

    processed = 0
    skipped = 0
    dir_list = []
    result_list = []
    result_dir_list = []

    for filename in filename_list:
        try:
            full_path = os.path.join(root, filename)
            fd = None
            try:
                fd = os.open(full_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_OPENLINK)
            except FileNotFoundError:
                thread_stats["file_not_found"] += 1
                LOG.debug("File %s is not found." % (full_path))
                continue
            except Exception as e:
                if e.errno == 45:
                    thread_stats["lstat_required"] += 1
                    LOG.debug("File %s is not allowed to call os.open, use os.lstat instead." % full_path)
                    time_start = time.time()
                    file_info = get_file_stat(root, filename, IFS_BLOCK_SIZE)
                    thread_stats["lstat_time"] += time.time() - lstat_start
                    if custom_tagging:
                        time_start = time.time()
                        file_info["user_tags"] = custom_tagging(file_info)
                        thread_stats["get_custom_tagging_time"] += time.time() - time_start
                    if file_info["file_type"] == "dir":
                        file_info["_scan_time"] = now
                        result_dir_list.append(file_info)
                        # Fix size issues with dirs
                        file_info["size_logical"] = 0
                        # Save directories to re-queue
                        dir_list.append(filename)
                        continue
                    result_list.append(file_info)
                    stats["file_size_total"] += file_info["size"]
                    processed += 1
                    continue
                LOG.exception("Error found when calling os.open on %s. Error: %s" % (full_path, str(e)))
                continue
            time_start = time.time()
            fstats = attr.get_dinode(fd)
            thread_stats["get_dinode_time"] += time.time() - time_start
            # atime call can return empty if the file does not have an atime or atime tracking is disabled
            time_start = time.time()
            atime = attr.get_access_time(fd)
            thread_stats["get_access_time_time"] += time.time() - time_start
            if atime:
                atime = atime[0]
            else:
                # If atime does not exist, use the last metadata change time as this captures the last time someone
                # modified either the data or the inode of the file
                atime = fstats["di_ctime"]
            di_data_blocks = fstats.get("di_data_blocks", fstats["di_physical_blocks"] - fstats["di_protection_blocks"])
            logical_blocks = fstats["di_logical_size"] // phys_block_size
            comp_blocks = logical_blocks - fstats["di_shadow_refs"]
            compressed_file = True if (di_data_blocks and comp_blocks) else False
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
                "file_compression_ratio": comp_blocks / di_data_blocks if compressed_file else 1,
                "file_hard_links": fstats["di_nlink"],
                "file_is_ads": ((fstats["di_flags"] & IFLAGS_UF_HASADS) != 0),
                "file_is_compressed": (comp_blocks > di_data_blocks) if compressed_file else False,
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
                "perms_unix_bitmask": stat.S_IMODE(fstats["di_mode"]),
                "perms_unix_gid": fstats["di_gid"],
                "perms_unix_uid": fstats["di_uid"],
                # ========== File protection level ==========
                "protection_current": fstats["di_current_protection"],
                "protection_target": fstats["di_protection_policy"],
                # ========== File allocation size and blocks ==========
                # The apparent size of the file. Sparse files include the sparse area
                "size": fstats["di_size"],
                # Logical size in 8K blocks. Sparse files only show the real data portion
                "size_logical": fstats["di_logical_size"],
                # Physical size on disk including protection overhead, including extension blocks and excluding metadata
                "size_physical": fstats["di_physical_blocks"] * phys_block_size,
                # Physical size on disk excluding protection overhead and excluding metadata
                "size_physical_data": di_data_blocks * phys_block_size,
                # Physical size on disk of the protection overhead
                "size_protection": fstats["di_protection_blocks"] * phys_block_size,
                # ========== SSD usage ==========
                "ssd_strategy": fstats["di_la_ssd_strategy"],
                "ssd_strategy_name": SSD_STRATEGY[fstats["di_la_ssd_strategy"]],
                "ssd_status": fstats["di_la_ssd_status"],
                "ssd_status_name": SSD_STATUS[fstats["di_la_ssd_status"]],
            }
            if not no_acl:
                time_start = time.time()
                acl = onefs_acl.get_acl_dict(fd)
                thread_stats["get_acl_time"] += time.time() - time_start
                file_info["perms_acl_aces"] = misc.ace_list_to_str_list(acl.get("aces"))
                file_info["perms_acl_group"] = misc.acl_group_to_str(acl)
                file_info["perms_acl_user"] = misc.acl_user_to_str(acl)
            if extra_attr:
                # di_flags may have other bits we need to translate
                #     Coalescer setting (on|off|endurant all|coalescer only)
                #     IFLAGS_UF_WRITECACHE and IFLAGS_UF_WC_ENDURANT flags
                # Do we want inode locations? how many on SSD and spinning disk?
                #   - Get data from estats["ge_iaddrs"], e.g. ge_iaddrs: [(1, 13, 1098752, 512)]
                # Extended attributes/custom attributes?
                time_start = time.time()
                estats = attr.get_expattr(fd)
                thread_stats["get_extra_attr_time"] += time.time() - time_start
                # Add up all the inode sizes
                metadata_size = 0
                for inode in estats["ge_iaddrs"]:
                    metadata_size += inode[3]
                # Sum of the size of all the inodes. This includes inodes that mix both 512 byte and 8192 byte inodes
                file_info["size_metadata"] = metadata_size
                file_info["file_is_manual_access"] = not not estats["ge_manually_manage_access"]
                file_info["file_is_manual_packing"] = not not estats["ge_manually_manage_packing"]
                file_info["file_is_manual_protection"] = not not estats["ge_manually_manage_protection"]
                if estats["ge_coalescing_ec"] & estats["ge_coalescing_on"]:
                    file_info["file_coalescer"] = "coalescer on, ec off"
                elif estats["ge_coalescing_on"]:
                    file_info["file_coalescer"] = "coalescer on, ec on"
                elif estats["ge_coalescing_ec"]:
                    file_info["file_coalescer"] = "coalescer off, ec on"
                else:
                    file_info["file_coalescer"] = "coalescer off, ec off"
            if user_attr:
                extended_attr = {}
                time_start = time.time()
                keys = uattr.userattr_list(fd)
                for key in keys:
                    extended_attr[key] = uattr.userattr_get(fd, key)
                thread_stats["get_user_attr_time"] += time.time() - time_start
                file_info["user_attributes"] = extended_attr
            if custom_tagging:
                time_start = time.time()
                file_info["user_tags"] = custom_tagging(file_info)
                thread_stats["get_custom_tagging_time"] += time.time() - time_start

            time_start = time.time()
            lstat_required = translate_user_group_perms(full_path, file_info)
            if lstat_required:
                thread_stats["lstat_required"] += 1
                thread_stats["lstat_time"] += time.time() - lstat_start

            if fstats["di_mode"] & 0o040000:
                file_info["_scan_time"] = now
                result_dir_list.append(file_info)
                # Fix size issues with dirs
                file_info["size_logical"] = 0
                # Save directories to re-queue
                dir_list.append(filename)
                continue
            result_list.append(file_info)
            if (
                (fstats["di_mode"] & 0o010000 == 0o010000)
                or (fstats["di_mode"] & 0o120000 == 0o120000)
                or (fstats["di_mode"] & 0o140000 == 0o140000)
            ):
                # Fix size issues with symlinks, sockets, and FIFOs
                file_info["size_logical"] = 0
            stats["file_size_total"] += fstats["di_size"]
            processed += 1
        except IOError as ioe:
            skipped += 1
            if ioe.errno == 13:
                LOG.info("Permission error scanning: {file}".format(file=full_path))
            else:
                LOG.exception(ioe)
        except FileNotFoundError as fnfe:
            skipped += 1
            LOG.info("File not found: {filename}".format(filename=filename))
        except PermissionError as pe:
            skipped += 1
            LOG.info("Permission error scanning: {file}".format(file=full_path))
            LOG.exception(pe)
        except Exception as e:
            skipped += 1
            LOG.exception(e)
        finally:
            try:
                os.close(fd)
            except:
                pass
    if (result_list or result_dir_list) and custom_state.get("es_send_q"):
        time_start = time.time()
        if result_list:
            custom_state["es_send_q"].put([CMD_SEND, result_list])
        if result_dir_list:
            custom_state["es_send_q"].put([CMD_SEND_DIR, result_dir_list])
        for i in range(DEFAULT_MAX_Q_WAIT_LOOPS):
            if custom_state["es_send_q"].qsize() > max_send_q_size:
                thread_stats["es_queue_wait_count"] += 1
                # TODO: Add statistic on how many times we had to wait
                time.sleep(send_q_sleep)
                LOG.critical("DEBUG: PUSHREQUIRED - Had to wait to push data to ES queue")
            else:
                break
        thread_stats["es_queue_time"] += time.time() - time_start
    return {"processed": processed, "skipped": skipped, "q_dirs": dir_list}


def get_file_stat(root, filename, block_unit=STAT_BLOCK_SIZE):
    full_path = os.path.join(root, filename)
    fstats = os.lstat(full_path)
    file_info = {
        # ========== Timestamps ==========
        "atime": fstats.st_atime,
        "atime_date": datetime.date.fromtimestamp(fstats.st_atime).isoformat(),
        "btime": None,
        "btime_date": None,
        "ctime": fstats.st_ctime,
        "ctime_date": datetime.date.fromtimestamp(fstats.st_ctime).isoformat(),
        "mtime": fstats.st_mtime,
        "mtime_date": datetime.date.fromtimestamp(fstats.st_mtime).isoformat(),
        # ========== File and path strings ==========
        "file_path": root,
        "file_name": filename,
        "file_ext": os.path.splitext(filename),
        # ========== File attributes ==========
        "file_hard_links": fstats.st_nlink,
        "file_type": FILE_TYPE[stat.S_IFMT(fstats.st_mode) & FILE_TYPE_MASK],
        "inode": fstats.st_ino,
        # ========== Permissions ==========
        "perms_unix_bitmask": stat.S_IMODE(fstats.st_mode),
        "perms_unix_gid": fstats.st_gid,
        "perms_unix_uid": fstats.st_uid,
        # ========== File allocation size and blocks ==========
        "size": fstats.st_size,
        "size_logical": block_unit * (int(fstats.st_size / block_unit) + 1 * ((fstats.st_size % block_unit) > 0)),
        # st_blocks includes metadata blocks
        "size_physical": block_unit * (int(fstats.st_blocks * STAT_BLOCK_SIZE / block_unit)),
    }
    try:
        file_info["btime"] = fstats.st_birthtime
        file_info["btime_date"] = datetime.date.fromtimestamp(fstats.st_btime).isoformat()
    except:
        # No birthtime date so do not add those fields
        pass
    if file_info["size"] == 0 and file_info["size_physical"] == 0:
        file_info["size_physical"] = file_info["size_logical"]
    return file_info


def init_custom_state(custom_state, options={}):
    # TODO: Parse the custom tag input file and produce a parser
    # Add any common parameters that each processing thread should have access to
    # by adding values to the custom_state dictionary
    # custom_state["custom_tagging"] = lambda x: None
    custom_state["custom_stats"] = {}
    custom_state["extra_attr"] = options.get("extra", DEFAULT_PARSE_EXTRA_ATTR)
    custom_state["es_send_q"] = None
    custom_state["es_cmd_send_q"] = None
    custom_state["max_send_q_size"] = options.get("ex_max_send_q_size", DEFAULT_ES_MAX_Q_SIZE)
    custom_state["no_acl"] = options.get("no_acl", DEFAULT_PARSE_SKIP_ACLS)
    custom_state["node_pool_translation"] = {}
    custom_state["phys_block_size"] = IFS_BLOCK_SIZE
    custom_state["send_q_sleep"] = options.get("es_send_q_sleep", DEFAULT_ES_SEND_Q_SLEEP)
    custom_state["user_attr"] = options.get("user_attr", DEFAULT_PARSE_USER_ATTR)
    if misc.is_onefs_os():
        # Query the cluster for node pool name information
        try:
            dpdb = dp.DiskPoolDB()
            groups = dpdb.get_groups()
            for g in groups:
                children = g.get_children()
                for child in children:
                    custom_state["node_pool_translation"][int(child.entryid)] = g.name
        except Exception as e:
            LOG.exception("Unable to get the ID to name translation for node pools")


def init_thread(tid, custom_state, thread_custom_state):
    """Called by scanit.py for each scanning thread to store thread specific state
    
    Parameters
    ----------
    tid: int - Numeric identifier for a thread
    custom_state: dict - Dictionary initialized by user_handlers.init_custom_state
    thread_custom_state: dict - Empty dictionary to store any thread specific state
    
    Returns
    ----------
    Nothing
    """
    # Add any custom stats counters or values in the thread_custom_state dictionary
    # and access this inside each file handler in the args["thread_state"]["custom"]
    # parameter
    thread_custom_state["thread_name"] = tid
    thread_custom_state["stats"] = {}
    for field in CUSTOM_STATS_FIELDS:
        thread_custom_state["stats"][field] = 0


def print_statistics(output_type, log, stats, custom_stats, num_clients, now, start_time, wall_time, output_interval):
    # TODO: Overhaul the stats output code
    output_string = "===== Custom stats =====\n" + json.dumps(custom_stats, indent=2, sort_keys=True) + "\n"
    LOG.info(output_string)
    sys.stdout.write(output_string)


def translate_user_group_perms(full_path, file_info):
    lstat_required = False
    # Populate the perms_user and perms_group fields from the avaialble SID and UID/GID data
    # Translate the numeric values into human readable user name and group names if possible
    # TODO: Add translation to names from SID/UID/GID values
    if file_info["perms_unix_uid"] == 0xFFFFFFFF or file_info["perms_unix_gid"] == 0xFFFFFFFF:
        lstat_required = True
        LOG.debug("File requires standard lstat for UID/GID: {filename}".format(filename=full_path))
        # If the UID/GID is set to 0xFFFFFFFF then on cluster, the UID/GID is generated
        # by the cluster.
        # When this happens, use os.fstat to get the UID/GID information from the point
        # of view of the access zone that is running the script, normally the System zone
        try:
            fstats = os.lstat(full_path)
            file_info["perms_unix_gid"] = (fstats.st_gid,)
            file_info["perms_unix_uid"] = (fstats.st_uid,)
        except Exception as e:
            LOG.info("Unable to get file UID/GID properly for: {filename}".format(filename=full_path))
    if "perms_acl_user" in file_info:
        file_info["perms_user"] = file_info["perms_acl_user"]
        file_info["perms_user"].replace("uid:", "")
    else:
        file_info["perms_user"] = file_info["perms_unix_uid"]
    if "perms_acl_group" in file_info:
        file_info["perms_group"] = file_info["perms_acl_group"]
        file_info["perms_group"].replace("gid:", "")
    else:
        file_info["perms_group"] = file_info["perms_unix_gid"]
    return lstat_required


def update_config(custom_state, new_config):
    LOG.critical("CONFIG UPDATE IS: %s" % new_config)
    client_config = new_config.get("client_config", {})
    es_credentials = client_config.get("es_credentials")
    if es_credentials:
        if client_config.get("es_thread_handles") is not None:
            # TODO: Add support for closing and reconnecting to a new ES instance
            pass
        if custom_state.get("es_send_q") is None:
            custom_state["es_send_q"] = queue.Queue()
        if custom_state.get("es_send_cmd_q") is None:
            custom_state["es_send_cmd_q"] = queue.Queue()
        es_threads = []
        threads_to_start = client_config.get("es_send_threads", DEFAULT_ES_THREADS)
        for i in range(threads_to_start):
            es_thread_instance = threading.Thread(
                target=elasticsearch_wrapper.es_data_sender,
                args=(
                    custom_state["es_send_q"],
                    custom_state["es_send_cmd_q"],
                    es_credentials["url"],
                    es_credentials["user"],
                    es_credentials["password"],
                    es_credentials["index"],
                ),
            )
            es_thread_instance.daemon = True
            es_thread_instance.start()
            es_threads.append(es_thread_instance)
        custom_state["es_thread_handles"] = es_threads
    custom_state["client_config"] = client_config


def shutdown(custom_state, custom_threads_state):
    if custom_state.get("es_thread_handles"):
        # Scanner is done processing. Wait for all the data to be sent to Elasticsearch
        LOG.debug("Waiting for send queue to empty")
        send_start = time.time()
        send_q = custom_state.get("es_send_q")
        while not send_q.empty():
            time.sleep(DEFAULT_CMD_POLL_INTERVAL)
            es_send_q_time = time.time() - send_start
            if es_send_q_time > DEFAULT_SEND_Q_WAIT_TIME:
                LOG.info(
                    "Send Q was not empty after {time} seconds. Force quitting.".format(time=DEFAULT_SEND_Q_WAIT_TIME)
                )
                break
        LOG.debug("Sending exit command to send queue")
        for thread_handle in custom_state.get("es_thread_handles"):
            custom_state.get("es_send_cmd_q").put([CMD_EXIT, None])
        for thread_handle in custom_state.get("es_thread_handles"):
            thread_handle.join()
