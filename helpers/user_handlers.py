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
    "get_file_stat",
    "init_custom_state",
    "init_thread",
    "print_statistics",
    "shutdown",
    "translate_user_group_perms",
    "update_config",
]
# fmt: on
import datetime
import errno
import json
import logging
import math
import os
import queue
import re
import stat
import sys
import threading
import time

from helpers.constants import *
import helpers.elasticsearch_wrapper as elasticsearch_wrapper
import helpers.misc as misc

try:
    import isi.fs.attr as attr
    import isi.fs.diskpool as dp
    import isi.fs.userattr as uattr
    import libs.onefs_acl as onefs_acl
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
    "lstat_required",
    "lstat_time",
]


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


def file_handler_basic(root, filename_list, stats, now, args={}):
    """
    The file handler returns a dictionary:
    {
      "processed": <int>                # Number of files actually processed
      "skipped": <int>                  # Number of files skipped
      "q_dirs": [<str>]                 # List of directory names that need processing
    }
    """
    block_size = args.get("block_size")
    custom_state = args.get("custom_state", {})
    start_time = args.get("start_time", time.time())
    thread_custom_state = args.get("thread_custom_state", {})
    thread_state = args.get("thread_state", {})

    custom_tagging = custom_state["custom_tagging"]
    max_send_q_size = custom_state["max_send_q_size"]
    send_q_sleep = custom_state["send_q_sleep"]
    strip_dot_snapshot = custom_state["strip_dot_snapshot"]

    processed = 0
    skipped = 0
    dir_list = []
    result_list = []
    result_dir_list = []

    for filename in filename_list:
        try:
            file_info = get_file_stat(root, filename, strip_dot_snapshot=strip_dot_snapshot)
            if custom_tagging:
                file_info["user_tags"] = custom_tagging(file_info)
            if file_info["file_type"] == "dir":
                file_info["_scan_time"] = now
                result_dir_list.append(file_info)
                # Save directories to re-queue
                dir_list.append(filename)
                continue
            stats["file_size_total"] += file_info["size"]
            stats["file_size_physical_total"] += block_size * int(math.ceil(file_stats.st_size / block_size))
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
    dir_stats_file_count = 0
    dir_stats_file_size = 0
    dir_stats_file_size_physical = 0
    dir_stats_subdir_count = 0
    dir_object = None
    if isinstance(root, dict):
        dir_object = root
        root = os.path.join(dir_object["file_path"], dir_object["file_name"])

    # Custom state values are guaranteed to exist due to the init routine
    custom_tagging = custom_state["custom_tagging"]
    extra_attr = custom_state["extra_attr"]
    user_attr = custom_state["user_attr"]
    max_send_q_size = custom_state["max_send_q_size"]
    no_acl = custom_state["no_acl"]
    phys_block_size = custom_state["phys_block_size"]
    pool_translate = custom_state["node_pool_translation"]
    send_q_sleep = custom_state["send_q_sleep"]
    strip_dot_snapshot = custom_state["strip_dot_snapshot"]

    processed = 0
    skipped = 0
    dir_list = []
    result_list = []
    result_dir_list = []
    update_dir_list = []

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
                if e.errno in (errno.ENOTSUP, errno.EACCES):  # 45: Not supported, 13: No access
                    thread_stats["lstat_required"] += 1
                    LOG.debug("File %s is not allowed to call os.open, use os.lstat instead." % full_path)
                    time_start = time.time()
                    file_info = get_file_stat(root, filename, phys_block_size, strip_dot_snapshot=strip_dot_snapshot)
                    thread_stats["lstat_time"] += time.time() - time_start
                    if custom_tagging:
                        time_start = time.time()
                        file_info["user_tags"] = custom_tagging(file_info)
                        thread_stats["get_custom_tagging_time"] += time.time() - time_start
                    if file_info["file_type"] == "dir":
                        file_info["_scan_time"] = now
                        # Fix size issues with dirs
                        file_info["size_logical"] = 0
                        dir_stats_subdir_count += 1
                        dir_info = {}
                        for field in ["file_name", "file_path", "inode"]:
                            dir_info[field] = file_info[field]
                        # Save directory info object to re-queue
                        dir_list.append(dir_info)
                        # Save the bulk of the directory info immediately
                        result_dir_list.append(file_info)
                        continue
                    result_list.append(file_info)
                    stats["file_size_total"] += file_info["size"]
                    stats["file_size_physical_total"] += phys_block_size * int(
                        math.ceil(file_info["size"] / phys_block_size)
                    )
                    dir_stats_file_count += 1
                    dir_stats_file_size += file_info["size"]
                    dir_stats_file_size_physical += file_info["size_physical"]
                    processed += 1
                    continue
                if e.errno in (errno.ENOENT):  # 2: File not found
                    thread_stats["file_not_found"] += 1
                    LOG.debug("File %s is not found." % (full_path))
                    continue
                LOG.exception("Error found when calling os.open on: %s Error: %s" % (full_path, str(e)))
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
            if strip_dot_snapshot:
                file_path = re.sub(RE_STRIP_SNAPSHOT, "", root, count=1)
            else:
                file_path = root
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
                "file_path": file_path,
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
                thread_stats["lstat_time"] += time.time() - time_start

            if fstats["di_mode"] & 0o040000:
                file_info["_scan_time"] = now
                file_info["file_is_inlined"] = False
                # Fix size issues with dirs
                file_info["size_logical"] = 0
                dir_stats_subdir_count += 1
                dir_info = {}
                for field in ["file_name", "file_path", "inode"]:
                    dir_info[field] = file_info[field]
                # Save directory info object to re-queue
                dir_list.append(dir_info)
                # Save the bulk of the directory info immediately
                result_dir_list.append(file_info)
                continue
            if (
                (fstats["di_mode"] & 0o010000 == 0o010000)
                or (fstats["di_mode"] & 0o120000 == 0o120000)
                or (fstats["di_mode"] & 0o140000 == 0o140000)
            ):
                # Fix size issues with symlinks, sockets, and FIFOs
                file_info["size_logical"] = 0
            result_list.append(file_info)
            stats["file_size_total"] += file_info["size"]
            stats["file_size_physical_total"] += file_info["size_physical"]
            dir_stats_file_count += 1
            dir_stats_file_size += file_info["size"]
            dir_stats_file_size_physical += file_info["size_physical"]
            processed += 1
        except IOError as ioe:
            skipped += 1
            if ioe.errno == errno.EACCES:  # 13: No access
                LOG.warn("Permission error scanning: {file}".format(file=full_path))
            else:
                LOG.exception(ioe)
        except FileNotFoundError as fnfe:
            skipped += 1
            LOG.warn("File not found: {filename}".format(filename=filename))
        except PermissionError as pe:
            skipped += 1
            LOG.warn("Permission error scanning: {file}".format(file=full_path))
            LOG.exception(pe)
        except Exception as e:
            skipped += 1
            LOG.exception(e)
        finally:
            try:
                os.close(fd)
            except:
                pass
    if dir_object:
        dir_object["dir_count_dirs"] = dir_stats_subdir_count
        dir_object["dir_count_files"] = dir_stats_file_count
        dir_object["dir_depth"] = len(dir_object["file_path"].split("/"))
        dir_object["dir_file_size"] = dir_stats_file_size
        dir_object["dir_file_size_physical"] = dir_stats_file_size_physical
        dir_object["dir_leaf"] = dir_stats_subdir_count == 0
        update_dir_list.append(dir_object)
    if (result_list or result_dir_list or update_dir_list) and custom_state["client_config"].get("es_cmd_idx"):
        time_start = time.time()
        if result_list:
            custom_state["es_send_q"].put([CMD_SEND, result_list])
        if result_dir_list:
            custom_state["es_send_q"].put([CMD_SEND_DIR, result_dir_list])
        if update_dir_list:
            custom_state["es_send_q"].put([CMD_SEND_DIR_UPDATE, update_dir_list])
        for i in range(DEFAULT_MAX_Q_WAIT_LOOPS):
            if custom_state["es_send_q"].qsize() > max_send_q_size:
                thread_stats["es_queue_wait_count"] += 1
                time.sleep(send_q_sleep)
            else:
                break
        thread_stats["es_queue_time"] += time.time() - time_start
    return {"processed": processed, "skipped": skipped, "q_dirs": dir_list}


def file_handler_pscale_diskover(root, filename_list, stats, now, args={}):
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
    dir_stats_file_count = 0
    dir_stats_file_size = 0
    dir_stats_file_size_physical = 0
    dir_stats_subdir_count = 0
    dir_object = None
    if isinstance(root, dict):
        dir_object = root
        root = os.path.join(dir_object["parent_path"], dir_object["name"])

    # Custom state values are guaranteed to exist due to the init routine
    custom_tagging = custom_state["custom_tagging"]
    extra_attr = custom_state["extra_attr"]
    user_attr = custom_state["user_attr"]
    max_send_q_size = custom_state["max_send_q_size"]
    no_acl = custom_state["no_acl"]
    phys_block_size = custom_state["phys_block_size"]
    pool_translate = custom_state["node_pool_translation"]
    send_q_sleep = custom_state["send_q_sleep"]
    strip_dot_snapshot = custom_state["strip_dot_snapshot"]

    processed = 0
    skipped = 0
    dir_list = []
    result_list = []
    result_dir_list = []
    update_dir_list = []

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
                if e.errno in (errno.ENOTSUP, errno.EACCES):  # 45: Not supported, 13: No access
                    thread_stats["lstat_required"] += 1
                    LOG.debug("File %s is not allowed to call os.open, use os.lstat instead." % full_path)
                    time_start = time.time()
                    file_info = get_file_stat_diskover(
                        root, filename, phys_block_size, strip_dot_snapshot=strip_dot_snapshot
                    )
                    thread_stats["lstat_time"] += time.time() - time_start
                    if custom_tagging:
                        time_start = time.time()
                        file_info["user_tags"] = custom_tagging(file_info)
                        thread_stats["get_custom_tagging_time"] += time.time() - time_start
                    if file_info["type"] == "directory":
                        file_info["_scan_time"] = now
                        # Fix size issues with dirs
                        file_info["size_logical"] = 0
                        dir_stats_subdir_count += 1
                        dir_info = {}
                        for field in ["ino", "name", "parent_path"]:
                            dir_info[field] = file_info[field]
                        # Save directory info object to re-queue
                        dir_list.append(dir_info)
                        # Save the bulk of the directory info immediately
                        result_dir_list.append(file_info)
                        continue
                    stats["file_size_total"] += file_info["size"]
                    stats["file_size_physical_total"] += phys_block_size * int(
                        math.ceil(file_info["size"] / phys_block_size)
                    )
                    dir_stats_file_count += 1
                    dir_stats_file_size += file_info["size"]
                    dir_stats_file_size_physical += file_info["size_du"]
                    processed += 1
                    continue
                if e.errno in (errno.ENOENT):  # 2: File not found
                    thread_stats["file_not_found"] += 1
                    LOG.debug("File %s is not found." % (full_path))
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
            if strip_dot_snapshot:
                parent_path = re.sub(RE_STRIP_SNAPSHOT, "", root, count=1)
            else:
                parent_path = root
            file_info = {
                "atime": datetime.datetime.fromtimestamp(atime).strftime(DEFAULT_TIME_FORMAT_8601),
                "ctime": datetime.datetime.fromtimestamp(fstats["di_ctime"]).strftime(DEFAULT_TIME_FORMAT_8601),
                "extension": os.path.splitext(filename)[1],
                "group": fstats["di_gid"],
                "ino": fstats["di_lin"],
                "mtime": datetime.datetime.fromtimestamp(fstats["di_mtime"]).strftime(DEFAULT_TIME_FORMAT_8601),
                "name": filename,
                "nlink": fstats["di_nlink"],
                "owner": fstats["di_uid"],
                "parent_path": parent_path,
                "size": fstats["di_size"],
                "size_du": fstats["di_physical_blocks"] * phys_block_size,
                "type": FILE_TYPE_DISKOVER[fstats["di_mode"] & FILE_TYPE_MASK],
                "pscale": {
                    # ========== Timestamps ==========
                    "atime": atime,
                    "atime_date": datetime.date.fromtimestamp(atime).isoformat(),
                    "btime": fstats["di_create_time"],
                    "btime_date": datetime.date.fromtimestamp(fstats["di_create_time"]).isoformat(),
                    "ctime": fstats["di_ctime"],
                    "ctime_date": datetime.date.fromtimestamp(fstats["di_ctime"]).isoformat(),
                    "mtime": fstats["di_mtime"],
                    "mtime_date": datetime.date.fromtimestamp(fstats["di_mtime"]).isoformat(),
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
                    ## "file_type": FILE_TYPE[fstats["di_mode"] & FILE_TYPE_MASK],
                    ## "inode": fstats["di_lin"],
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
                },
            }
            if not no_acl:
                time_start = time.time()
                acl = onefs_acl.get_acl_dict(fd)
                thread_stats["get_acl_time"] += time.time() - time_start
                file_info["pscale"]["perms_acl_aces"] = misc.ace_list_to_str_list(acl.get("aces"))
                file_info["pscale"]["perms_acl_group"] = misc.acl_group_to_str(acl)
                file_info["pscale"]["perms_acl_user"] = misc.acl_user_to_str(acl)
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
                file_info["pscale"]["size_metadata"] = metadata_size
                file_info["pscale"]["file_is_manual_access"] = not not estats["ge_manually_manage_access"]
                file_info["pscale"]["file_is_manual_packing"] = not not estats["ge_manually_manage_packing"]
                file_info["pscale"]["file_is_manual_protection"] = not not estats["ge_manually_manage_protection"]
                if estats["ge_coalescing_ec"] & estats["ge_coalescing_on"]:
                    file_info["pscale"]["file_coalescer"] = "coalescer on, ec off"
                elif estats["ge_coalescing_on"]:
                    file_info["pscale"]["file_coalescer"] = "coalescer on, ec on"
                elif estats["ge_coalescing_ec"]:
                    file_info["pscale"]["file_coalescer"] = "coalescer off, ec on"
                else:
                    file_info["pscale"]["file_coalescer"] = "coalescer off, ec off"
            if user_attr:
                extended_attr = {}
                time_start = time.time()
                keys = uattr.userattr_list(fd)
                for key in keys:
                    extended_attr[key] = uattr.userattr_get(fd, key)
                thread_stats["get_user_attr_time"] += time.time() - time_start
                file_info["pscale"]["user_attributes"] = extended_attr
            if custom_tagging:
                time_start = time.time()
                file_info["pscale"]["user_tags"] = custom_tagging(file_info)
                thread_stats["get_custom_tagging_time"] += time.time() - time_start

            time_start = time.time()
            lstat_required = translate_user_group_perms(full_path, file_info["pscale"])
            if lstat_required:
                thread_stats["lstat_required"] += 1
                thread_stats["lstat_time"] += time.time() - time_start

            if fstats["di_mode"] & 0o040000:
                file_info["pscale"]["_scan_time"] = now
                file_info["file_is_inlined"] = False
                # Fix size issues with dirs
                file_info["pscale"]["size_logical"] = 0
                dir_stats_subdir_count += 1
                dir_info = {}
                for field in ["ino", "name", "parent_path"]:
                    dir_info[field] = file_info[field]
                # Save directory info object to re-queue
                dir_list.append(dir_info)
                # Save the bulk of the directory info immediately
                result_dir_list.append(file_info)
                continue
            if (
                (fstats["di_mode"] & 0o010000 == 0o010000)
                or (fstats["di_mode"] & 0o120000 == 0o120000)
                or (fstats["di_mode"] & 0o140000 == 0o140000)
            ):
                # Fix size issues with symlinks, sockets, and FIFOs
                file_info["pscale"]["size_logical"] = 0
            result_list.append(file_info)
            stats["file_size_total"] += file_info["size"]
            stats["file_size_physical_total"] += file_info["size_du"]
            dir_stats_file_count += 1
            dir_stats_file_size += file_info["size"]
            dir_stats_file_size_physical += file_info["size_du"]
            processed += 1
        except IOError as ioe:
            skipped += 1
            dir_stats_file_count += 1
            if ioe.errno == errno.EACCES:  # 13: No access
                LOG.warn("Permission error scanning: {file}".format(file=full_path))
            else:
                LOG.exception(ioe)
        except FileNotFoundError as fnfe:
            skipped += 1
            dir_stats_file_count += 1
            LOG.warn("File not found: {filename}".format(filename=filename))
        except PermissionError as pe:
            skipped += 1
            dir_stats_file_count += 1
            LOG.warn("Permission error scanning: {file}".format(file=full_path))
            LOG.exception(pe)
        except Exception as e:
            skipped += 1
            dir_stats_file_count += 1
            LOG.exception(e)
        finally:
            try:
                os.close(fd)
            except:
                pass
    if dir_object:
        dir_object["dir_count"] = 0
        dir_object["dir_count_norecurs"] = dir_stats_subdir_count
        dir_object["dir_depth"] = len(dir_object["parent_path"].split("/")) - 1
        dir_object["file_count"] = 0
        dir_object["file_count_norecurs"] = dir_stats_file_count
        dir_object["size"] = 0
        dir_object["size_norecurs"] = dir_stats_file_size
        dir_object["size_du"] = 0
        dir_object["size_du_norecurs"] = dir_stats_file_size_physical
        dir_object["dir_leaf"] = dir_stats_subdir_count == 0
        result_dir_list.append(dir_object)
    if (result_list or result_dir_list or update_dir_list) and custom_state["client_config"].get("es_cmd_idx"):
        time_start = time.time()
        if result_list:
            custom_state["es_send_q"].put([CMD_SEND, result_list])
        if result_dir_list:
            custom_state["es_send_q"].put([CMD_SEND_DIR, result_dir_list])
        if update_dir_list:
            custom_state["es_send_q"].put([CMD_SEND_DIR_UPDATE, update_dir_list])
        for i in range(DEFAULT_MAX_Q_WAIT_LOOPS):
            if custom_state["es_send_q"].qsize() > max_send_q_size:
                thread_stats["es_queue_wait_count"] += 1
                time.sleep(send_q_sleep)
            else:
                break
        thread_stats["es_queue_time"] += time.time() - time_start
    return {"processed": processed, "skipped": skipped, "q_dirs": dir_list}


def get_file_stat(root, filename, block_unit=STAT_BLOCK_SIZE, strip_dot_snapshot=True):
    full_path = os.path.join(root, filename)
    fstats = os.lstat(full_path)
    if strip_dot_snapshot:
        file_path = re.sub(RE_STRIP_SNAPSHOT, "", root, count=1)
    else:
        file_path = root
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
        "file_path": file_path,
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


def get_file_stat_diskover(root, filename, block_unit=STAT_BLOCK_SIZE, strip_dot_snapshot=True):
    pscale_fstat = get_file_stat(root, filename, block_unit, strip_dot_snapshot)
    file_info = {
        "atime": datetime.datetime.fromtimestamp(pscale_fstat["atime"]).strftime(DEFAULT_TIME_FORMAT_8601),
        "ctime": datetime.datetime.fromtimestamp(pscale_fstat["ctime"]).strftime(DEFAULT_TIME_FORMAT_8601),
        "extension": pscale_fstat["file_ext"],
        "group": pscale_fstat["perms_unix_gid"],
        "ino": pscale_fstat["inode"],
        "mtime": datetime.datetime.fromtimestamp(pscale_fstat["mtime"]).strftime(DEFAULT_TIME_FORMAT_8601),
        "name": pscale_fstat["file_name"],
        "nlink": pscale_fstat["file_hard_links"],
        "owner": pscale_fstat["perms_unix_uid"],
        "parent_path": pscale_fstat["file_path"],
        "size": pscale_fstat["size"],
        "size_du": pscale_fstat["size_physical"],
        "type": "directory" if pscale_fstat["file_type"] == "dir" else pscale_fstat["file_type"],
        "pscale": pscale_fstat,
    }
    # Remove duplicate keys in the pscale_fstat that are moved to the outer dictionary
    for key in [
        "atime",
        "ctime",
        "file_ext",
        "perms_unix_gid",
        "inode",
        "mtime",
        "file_name",
        "file_hard_links",
        "file_path",
        "file_type",
        "perms_unix_uid",
        "size",
        "size_physical",
    ]:
        del pscale_fstat[key]
    return file_info


def init_custom_state(custom_state, options={}):
    # Add any common parameters that each processing thread should have access to
    # by adding values to the custom_state dictionary
    custom_state["custom_stats"] = {}
    custom_state["custom_tagging"] = None  # lambda x: None
    custom_state["extra_attr"] = options.get("extra", DEFAULT_PARSE_EXTRA_ATTR)
    custom_state["es_send_q"] = queue.Queue()
    custom_state["es_send_cmd_q"] = queue.Queue()
    custom_state["max_send_q_size"] = options.get("es_max_send_q_size", DEFAULT_ES_MAX_Q_SIZE)
    custom_state["no_acl"] = options.get("no_acl", DEFAULT_PARSE_SKIP_ACLS)
    custom_state["node_pool_translation"] = misc.get_nodepool_translation()
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
    consolidated_custom_stats = {}
    for field in CUSTOM_STATS_FIELDS:
        consolidated_custom_stats[field] = 0
        for client in custom_stats:
            consolidated_custom_stats[field] += client[field]
        if "_time" in field:
            consolidated_custom_stats[field] = consolidated_custom_stats[field] / stats["threads"]
    output_string = (
        "===== Custom stats (average over all threads) =====\n"
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
        # TODO: Change this to a variable time
        flush_time = 120
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


def translate_user_group_perms(full_path, file_info):
    lstat_required = False
    # Populate the perms_user and perms_group fields from the avaialble SID and UID/GID data
    # Translate the numeric values into human readable user name and group names if possible
    # TODO: Add translation to names from SID/UID/GID values
    if file_info["perms_unix_uid"] == 0xFFFFFFFF or file_info["perms_unix_gid"] == 0xFFFFFFFF:
        lstat_required = True
        LOG.debug("lstat required for UID/GID: {filename}".format(filename=full_path))
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
    cli_config = new_config.get("cli_options", {})
    client_config = new_config.get("client_config", {})
    es_options = client_config.get("es_options")
    # TODO: Add code to shutdown existing threads or adjust running threads based on new config
    if es_options:
        if client_config.get("es_thread_handles") is not None:
            # TODO: Add support for closing and reconnecting to a new ES instance
            pass
        es_threads = []
        threads_to_start = client_config.get("es_send_threads", DEFAULT_ES_THREADS)
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
                )
                es_thread_instance.daemon = True
                es_thread_instance.start()
                es_threads.append(es_thread_instance)
        except Exception as e:
            LOG.exception("Error encountered starting up ES sender threads")
        custom_state["es_thread_handles"] = es_threads
    custom_state["client_config"] = client_config
    custom_state["no_acl"] = cli_config.get("no_acl", custom_state["no_acl"])
