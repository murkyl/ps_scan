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
]
# fmt: on
import datetime
import logging
import os
import queue
import stat
import time

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


LOG = logging.getLogger(__name__)


def custom_stats_handler(common_stats, custom_state, custom_threads_state, thread_state):
    # Access all the individual thread state dictionaries in the custom_threads_state array
    # These should be initialized in the init_thread routine
    pass


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
    thread_custom_state = args.get("thread_custom_state", {})
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
            fstats = os.lstat(full_path)
            try:
                btime = fstats.st_birthtime
                btime_date = datetime.date.fromtimestamp(fstats.st_btime).isoformat()
            except:
                btime = 0
                btime_date = ""
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
                file_info["user_tags"] = custom_tagging(file_info)
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
    thread_custom_state = args.get("thread_custom_state", {})
    thread_state = args.get("thread_state", {})

    acl = custom_state.get("acl", None)
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
        try:
            full_path = os.path.join(root, filename)
            fd = None
            fd = os.open(full_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_OPENLINK)
            fstats = attr.get_dinode(fd)
            # atime call can return empty if the file does not have an atime or atime tracking is disabled
            atime = attr.get_access_time(fd)
            if atime:
                atime = atime[0]
            else:
                # If atime does not exist, use the last metadata change time as this captures the last time someone
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
            if acl:
                file_info["perms_acl"]: onefs_acl.get_acl_dict(fd)
            if extra_attr:
                # di_flags may have other bits we need to translate
                #     Coalescer setting (on|off|endurant all|coalescer only)
                #     IFLAGS_UF_WRITECACHE and IFLAGS_UF_WC_ENDURANT flags
                # Still need SID for file owner???
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
                file_info["user_tags"] = custom_tagging(file_info)
            if fstats["di_mode"] & 0o040000:
                result_dir_list.append(file_info)
                # Fix size issues with dirs
                file_info["size_logical"] = 0
                # Save directories to re-queue
                dir_list.append(filename)
                LOG.critical("DIR QUEUED: %s" % filename)
                continue
            else:
                result_list.append(file_info)
                if fstats["di_mode"] & 0o120000:
                    # Fix size issues with symlinks
                    file_info["size_logical"] = 0
            stats["file_size_total"] += fstats["di_size"]
            processed += 1
        except IOError as ioe:
            skipped += 1
            if ioe.errno == 13:
                LOG.info("Permission error scanning: {file}".format(file=full_path))
            else:
                LOG.exception(ioe)
        except PermissionError:
            skipped += 1
            LOG.info("Permission error scanning: {file}".format(file=full_path))
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
    # Add any common parameters that each processing thread should have access to
    # by adding values to the custom_state dictionary
    custom_state["acl"] = options.acl
    custom_state["custom_tagging"] = lambda x: None
    custom_state["custom_stats"] = {}
    custom_state["extra_attr"] = options.extra
    custom_state["max_send_q_size"] = options.es_max_send_q_size
    custom_state["node_pool_translation"] = {}
    custom_state["phys_block_size"] = IFS_BLOCK_SIZE
    custom_state["send_q"] = queue.Queue()
    custom_state["send_q_sleep"] = options.es_send_q_sleep
    custom_state["send_to_es"] = options.es_user and options.es_pass and options.es_url and options.es_index
    custom_state["user_attr"] = options.user_attr
    if misc.is_onefs_os():
        # Query the cluster for node pool name information
        try:
            dpdb = dp.DiskPoolDB()
            groups = dpdb.get_groups()
            for g in groups:
                children = g.get_children()
                for child in children:
                    custom_state["node_pool_translation"][int(child.entryid)] = g.name
        except:
            LOG.critical("Unable to get the ID to name translation for node pools")


def init_thread(custom_state, thread_custom_state):
    # Add any custom stats counters or values in the thread_custom_state dictionary
    # and access this inside each file handler
    pass
