#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
PowerScale file scanner
"""
# fmt: off
__title__         = "scanner"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
__all__ = [
  "PSCALE_DISKOVER_FIELD_MAPPING",
  "STATS_FIELDS",
  "add_diskover_fields",
  "convert_csv_to_response",
  "convert_response_to_diskover",
  "convert_response_to_csv",
  "file_handler_basic",
  "file_handler_pscale",
  "get_file_stat",
]
# fmt: on

import datetime
import errno
import logging
import re
import os
import stat
import time

from helpers.constants import *
import helpers.misc as misc

try:
    import isi.fs.attr as attr
    import isi.fs.userattr as uattr
    import libs.onefs_acl as onefs_acl
    import libs.onefs_auth as onefs_auth
except:
    pass
try:
    dir(PermissionError)
except:
    PermissionError = NotImplementedError
try:
    dir(FileNotFoundError)
except:
    FileNotFoundError = NotImplementedError


LOG = logging.getLogger(__name__)
PSCALE_DISKOVER_FIELD_MAPPING = [
    # PScale field, Diskover field
    ["atime", "atime"],
    ["ctime", "ctime"],
    ["file_ext", "extension"],
    ["file_hard_links", "nlink"],
    ["file_name", "name"],
    ["file_path", "parent_path"],
    ["file_type", "type"],
    ["inode", "ino"],
    ["mtime", "mtime"],
    ["perms_unix_gid", "group"],
    ["perms_unix_uid", "owner"],
    ["size", "size"],
    ["size_physical", "size_du"],
]
STATS_FIELDS = [
    "lstat_required",  # Number of times lstat was called vs. internal stat call
    "not_found",  # Number of files that were not found
    "processed",  # Number of files actually processed
    "skipped",  # Number of files skipped
    "time_access_time",  # Seconds spent getting the file access time
    "time_acl",  # Seconds spent getting file ACL
    "time_custom_tagging",  # Seconds spent processing custom tags
    "time_data_save",  # Seconds spent creating response
    "time_dinode",  # Seconds spent getting OneFS metadata
    "time_extra_attr",  # Seconds spent getting extra OneFS metadata
    "time_filter",  # Seconds spent filtering fields
    "time_name",  # Seconds spent translating UID/GID/SID to names
    "time_open",  # Seconds spent in os.open
    "time_lstat",  # Seconds spent in lstat
    "time_scan_dir",  # Seconds spent scanning the entire directory
    "time_user_attr",  # Seconds spent scanning user attributes
]
CSV_CONVERSION_FIELDS = [
    ["atime", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["atime_date", lambda x: str(x), lambda x: x],
    ["btime", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["btime_date", lambda x: str(x), lambda x: x],
    ["ctime", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["ctime_date", lambda x: str(x), lambda x: x],
    ["mtime", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["mtime_date", lambda x: str(x), lambda x: x],
    ["dir_count_dirs", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["dir_count_dirs_recursive", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["dir_count_files", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["dir_count_files_recursive", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["dir_depth", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["dir_file_size", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["dir_file_size_recursive", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["dir_file_size_physical", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["dir_file_size_physical_recursive", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["dir_leaf", lambda x: str(x), lambda x: misc.parse_bool(x)],
    ["file_path", lambda x: str(x), lambda x: x],
    ["file_name", lambda x: str(x), lambda x: x],
    ["file_ext", lambda x: str(x), lambda x: x],
    ["file_access_pattern", lambda x: str(x), lambda x: x],
    ["file_coalescer", lambda x: str(x), lambda x: x],
    ["file_compression_ratio", lambda x: str(x), lambda x: misc.parse_float(x)],
    ["file_hard_links", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["file_is_ads", lambda x: str(x), lambda x: misc.parse_bool(x)],
    ["file_is_compressed", lambda x: str(x), lambda x: misc.parse_bool(x)],
    ["file_is_dedupe_disabled", lambda x: str(x), lambda x: misc.parse_bool(x)],
    ["file_is_deduped", lambda x: str(x), lambda x: misc.parse_bool(x)],
    ["file_is_inlined", lambda x: str(x), lambda x: misc.parse_bool(x)],
    ["file_is_manual_access", lambda x: str(x), lambda x: misc.parse_bool(x)],
    ["file_is_manual_packing", lambda x: str(x), lambda x: misc.parse_bool(x)],
    ["file_is_manual_protection", lambda x: str(x), lambda x: misc.parse_bool(x)],
    ["file_is_packed", lambda x: str(x), lambda x: misc.parse_bool(x)],
    ["file_is_smartlinked", lambda x: str(x), lambda x: misc.parse_bool(x)],
    ["file_is_sparse", lambda x: str(x), lambda x: misc.parse_bool(x)],
    ["file_type", lambda x: str(x), lambda x: x],
    ["inode", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["inode_mirror_count", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["inode_parent", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["inode_revision", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["pool_target_data", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["pool_target_data_name", lambda x: str(x), lambda x: x],
    ["pool_target_metadata", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["pool_target_metadata_name", lambda x: str(x), lambda x: x],
    ["perms_acl_aces", lambda x: str(x), lambda x: misc.parse_str_acl(x)],
    ["perms_acl_group", lambda x: str(x), lambda x: x],
    ["perms_acl_user", lambda x: str(x), lambda x: x],
    ["perms_group", lambda x: str(x), lambda x: x],
    ["perms_unix_bitmask", lambda x: str(x), lambda x: x],
    ["perms_unix_gid", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["perms_unix_uid", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["perms_user", lambda x: str(x), lambda x: x],
    ["protection_current", lambda x: str(x), lambda x: x],
    ["protection_target", lambda x: str(x), lambda x: x],
    ["size", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["size_logical", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["size_metadata", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["size_physical", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["size_physical_data", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["size_protection", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["ssd_strategy", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["ssd_strategy_name", lambda x: str(x), lambda x: x],
    ["ssd_status", lambda x: str(x), lambda x: misc.parse_int(x)],
    ["ssd_status_name", lambda x: str(x), lambda x: x],
    ["user_attributes", lambda x: str(x), lambda x: None],
    ["user_tags", lambda x: str(x), lambda x: x],
]

def add_diskover_fields(file_info, remove_existing=True):
    diskover_info = {"pscale": file_info}
    for mapping in PSCALE_DISKOVER_FIELD_MAPPING:
        diskover_info[mapping[1]] = file_info.get(mapping[0])
        if remove_existing and mapping[0] in file_info:
            del file_info[mapping[0]]
    return diskover_info


def convert_csv_to_response(csv_data):
    response_data = {}
    index = 0
    try:
        for field in CSV_CONVERSION_FIELDS:
            col_data = csv_data[index]
            data = field[2](col_data)
            if data is None:
                index += 1
                continue
            response_data[field[0]] = data
            index += 1
        # We have a complete record now
    except:
        LOG.error("Unable to convert field: %s, with data: %s for row: %s"%(field[0], csv_data[index], csv_data))
        return {}
    return response_data


def convert_response_to_diskover(resp_data):
    now = time.time()
    dirs_list = resp_data["contents"]["dirs"]
    files_list = resp_data["contents"]["files"]
    root = resp_data["contents"].get("root", {})
    stats = resp_data["statistics"]
    resp_data["contents"] = {"entries": [], "root": root}
    entries = resp_data["contents"]["entries"]
    skipped_files = 0
    for i in range(len(files_list)):
        if files_list[i]["file_type"] != "file":
            skipped_files += 1
            continue
        entries.append(add_diskover_fields(files_list[i]))
    for i in range(len(dirs_list)):
        entries.append(add_diskover_fields(dirs_list[i]))
    if root:
        resp_data["contents"]["root"] = add_diskover_fields(root)
    stats["skipped"] += skipped_files
    stats["processed"] -= skipped_files
    stats["time_conversion"] = time.time() - now
    return resp_data


def convert_response_to_csv(resp_data, headers_only=False):
    csv_data = []
    if headers_only:
        csv_data = [field[0] for field in CSV_CONVERSION_FIELDS]
    else:
        csv_data = [field[1](resp_data.get(field[0])) for field in CSV_CONVERSION_FIELDS]
    return csv_data


def file_handler_basic(root, filename_list, args={}):
    """Gets the metadata for the files/directories based at root and given the file/dir names in filename_list

    Parameters
    ----------
    root: <string> Root directory to start the scan
    filename_list: <list:string> List of file and directory names to retrieve metadata
    args: <dict> Dictionary containing parameters to control the scan
        {
          "custom_tagging": <bool>          # When true call a custom handler for each file
          "fields": <list|str>              # List of field names to return in the response
          "phys_block_size": <int>          # Number of bytes in a block for the underlying storage device
          "strip_dot_snapshot": <bool>      # When true, strip the .snapshot name from the file path returned
        }

    Returns
    ----------
    dict - A dictionary representing the root and files scanned
        {
          "dirs": [<dict>]                  # List of directory metadata objects
          "files": [<dict>]                 # List of file metadata objects
          "statistics": {
            "lstat_required": <bool>        # Number of times lstat was called vs. internal stat call
            "not_found": <int>              # Number of files that were not found
            "processed": <int>              # Number of files actually processed
            "skipped": <int>                # Number of files skipped
            "time_custom_tagging": <int>    # Seconds spent processing custom tags
            "time_filter": <int>            # Seconds spent filtering fields
            "time_lstat": <int>             # Seconds spent in lstat
            "time_scan_dir": <int>          # Seconds spent scanning the entire directory
          }
        }
    """
    start_scanner = time.time()
    custom_state = args.get("custom_state")
    custom_tagging = args.get("custom_tagging", False)
    filter_fields = args.get("fields")
    phys_block_size = args.get("phys_block_size", IFS_BLOCK_SIZE)
    strip_dot_snapshot = args.get("strip_dot_snapshot", DEFAULT_STRIP_DOT_SNAPSHOT)

    result_list = []
    result_dir_list = []
    stats = {}
    for stat_name in STATS_FIELDS:
        stats[stat_name] = 0

    for filename in filename_list:
        try:
            stats["lstat_required"] += 1
            time_start = time.time()
            file_info = get_file_stat(root, filename, phys_block_size, strip_dot_snapshot=strip_dot_snapshot)
            if not file_info:
                stats["skipped"] += 1
                continue
            stats["time_lstat"] += time.time() - time_start
            if custom_tagging:
                time_start = time.time()
                file_info["user_tags"] = custom_tagging(file_info)
                stats["time_custom_tagging"] += time.time() - time_start

            # Filter out keys if requested
            if filter_fields:
                time_start = time.time()
                for key in list(file_info.keys()):
                    if key not in filter_fields:
                        del file_info[key]
                stats["time_filter"] += time.time() - time_start()

            if file_info["file_type"] == "dir":
                # Set values for directories
                file_info["file_is_inlined"] = False
                file_info["size_logical"] = 0
                result_dir_list.append(file_info)
                continue

            result_list.append(file_info)
            stats["processed"] += 1
        except FileNotFoundError as fnfe:
            stats["skipped"] += 1
            LOG.warn({"msg": "File not found", "file_path": os.path.join(root, filename)})
        except Exception as e:
            stats["skipped"] += 1
            LOG.warn({"msg": "Exception during file scan", "file_path": os.path.join(root, filename)})
            LOG.exception(e)
    stats["time_scan_dir"] = time.time() - start_scanner
    if custom_state:
        custom_stats = custom_state.get("stats")
        if custom_stats:
            for field in STATS_FIELDS:
                custom_stats[field] += stats[field]
    results = {
        "dirs": result_dir_list,
        "files": result_list,
        "statistics": stats,
    }
    return results


def file_handler_pscale(root, filename_list, args={}):
    """Gets the metadata for the files/directories based at root and given the file/dir names in filename_list

    Parameters
    ----------
    root: <string> Root directory to start the scan
    filename_list: <list:string> List of file and directory names to retrieve metadata
    args: <dict> Dictionary containing parameters to control the scan
        {
          "custom_tagging": <bool>          # When true call a custom handler for each file
          "extra_attr": <bool>              # When true, gets extra OneFS metadata
          "fields": <list|str>              # List of field names to return in the response
          "no_acl": <bool>                  # When true, skip ACL parsing
          "no_names": <bool>                # When true, skip translating UID/GID/SID to names
          "phys_block_size": <int>          # Number of bytes in a block for the underlying storage device
          "nodepool_translation": <dict>    # Dictionary with a node pool number to text string translation
          "strip_dot_snapshot": <bool>      # When true, strip the .snapshot name from the file path returned
          "user_attr": <bool>               # When true, get user attribute data for files
        }

    Returns
    ----------
    dict - A dictionary representing the root and files scanned
        {
          "dirs": [<dict>]                  # List of directory metadata objects
          "files": [<dict>]                 # List of file metadata objects
          "statistics": {
            "lstat_required": <bool>        # Number of times lstat was called vs. internal stat call
            "not_found": <int>              # Number of files that were not found
            "processed": <int>              # Number of files actually processed
            "skipped": <int>                # Number of files skipped
            "time_access_time": <int>       # Seconds spent getting the file access time
            "time_acl": <int>               # Seconds spent getting file ACL
            "time_custom_tagging": <int>    # Seconds spent processing custom tags
            "time_data_save": <int>         # Seconds spent creating response
            "time_dinode": <int>            # Seconds spent getting OneFS metadata
            "time_extra_attr": <int>        # Seconds spent getting extra OneFS metadata
            "time_filter": <int>            # Seconds spent filtering fields
            "time_name": <int>              # Seconds spend translating UID/GID/SID to names
            "time_open": <int>              # Seconds required to acquire file descriptor
            "time_lstat": <int>             # Seconds spent in lstat
            "time_scan_dir": <int>          # Seconds spent scanning the entire directory
            "time_user_attr": <int>         # Seconds spent scanning user attributes
          }
        }
    """
    start_scanner = time.time()
    custom_state = args.get("custom_state")
    custom_tagging = args.get("custom_tagging", False)
    extra_attr = args.get("extra_attr", DEFAULT_PARSE_EXTRA_ATTR)
    filter_fields = args.get("fields")
    no_acl = args.get("no_acl", DEFAULT_PARSE_SKIP_ACLS)
    no_names = args.get("no_names", DEFAULT_PARSE_SKIP_NAMES)
    phys_block_size = args.get("phys_block_size", IFS_BLOCK_SIZE)
    pool_translate = args.get("nodepool_translation", {})
    strip_dot_snapshot = args.get("strip_dot_snapshot", DEFAULT_STRIP_DOT_SNAPSHOT)
    user_attr = args.get("user_attr", DEFAULT_PARSE_USER_ATTR)

    result_list = []
    result_dir_list = []
    stats = {}
    for stat_name in STATS_FIELDS:
        stats[stat_name] = 0

    for filename in filename_list:
        try:
            full_path = os.path.join(root, filename)
            fd = None
            try:
                time_start = time.time()
                fd = os.open(full_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_OPENLINK)
                stats["time_open"] += time.time() - time_start
            except FileNotFoundError:
                LOG.debug({"msg": "File not found", "file_path": full_path})
                stats["not_found"] += 1
                continue
            except Exception as e:
                if e.errno in (errno.ENOTSUP, errno.EACCES):  # 45: Not supported, 13: No access
                    # Change how the file stat data will be retrieved
                    fd = 0
                else:
                    LOG.exception({"msg": "Error found when calling os.open", "file_path": full_path, "error": str(e)})
                    raise
            if fd:  # Use OneFS specific calls
                time_start_dinode = time.time()
                fstats = attr.get_dinode(fd)
                time_end_dinode = time.time()
                stats["time_dinode"] += time_end_dinode - time_start_dinode
                # atime call can return empty if the file does not have an atime or atime tracking is disabled
                atime = attr.get_access_time(fd)
                if atime:
                    atime = atime[0]
                else:
                    # If atime does not exist, use the last metadata change time as this captures the last time someone
                    # modified either the data or the inode of the file
                    atime = fstats["di_ctime"]
                time_end_atime = time.time()
                stats["time_access_time"] += time_end_atime - time_end_dinode
                logical_blocks = fstats["di_logical_size"] // phys_block_size
                # OneFS < 9.2 does not have di_protection_blocks or di_data_blocks. Estimate these values
                protection_blocks = fstats.get("di_protection_blocks") or fstats["di_physical_blocks"] - logical_blocks
                di_data_blocks = fstats.get("di_data_blocks", fstats["di_physical_blocks"] - protection_blocks)
                comp_blocks = logical_blocks - fstats["di_shadow_refs"]
                compressed_file = di_data_blocks and comp_blocks
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
                    "file_compression_ratio": (float(comp_blocks) / float(di_data_blocks)) if compressed_file else 1,
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
                    "perms_unix_bitmask": "{0:o}".format(stat.S_IMODE(fstats["di_mode"])),
                    "perms_unix_gid": int(fstats["di_gid"]),
                    "perms_unix_uid": int(fstats["di_uid"]),
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
                    "size_protection": protection_blocks * phys_block_size,
                    # ========== SSD usage ==========
                    "ssd_strategy": fstats["di_la_ssd_strategy"],
                    "ssd_strategy_name": SSD_STRATEGY[fstats["di_la_ssd_strategy"]],
                    "ssd_status": fstats["di_la_ssd_status"],
                    "ssd_status_name": SSD_STATUS[fstats["di_la_ssd_status"]],
                }
                stats["time_data_save"] += time.time() - time_end_atime
                if not no_acl:
                    time_start = time.time()
                    acl = onefs_acl.get_acl_dict(fd)
                    file_info["perms_acl_aces"] = misc.ace_list_to_str_list(acl.get("aces"))
                    file_info["perms_acl_group"] = misc.acl_group_to_str(acl)
                    file_info["perms_acl_user"] = misc.acl_user_to_str(acl)
                    stats["time_acl"] += time.time() - time_start
                if extra_attr:
                    # di_flags may have other bits we need to translate
                    #     Coalescer setting (on|off|endurant all|coalescer only)
                    #     IFLAGS_UF_WRITECACHE and IFLAGS_UF_WC_ENDURANT flags
                    # Do we want inode locations? how many on SSD and spinning disk?
                    #   - Get data from estats["ge_iaddrs"], e.g. ge_iaddrs: [(1, 13, 1098752, 512)]
                    # Extended attributes/custom attributes?
                    time_start = time.time()
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
                    if estats["ge_coalescing_ec"] & estats["ge_coalescing_on"]:
                        file_info["file_coalescer"] = "coalescer on, ec off"
                    elif estats["ge_coalescing_on"]:
                        file_info["file_coalescer"] = "coalescer on, ec on"
                    elif estats["ge_coalescing_ec"]:
                        file_info["file_coalescer"] = "coalescer off, ec on"
                    else:
                        file_info["file_coalescer"] = "coalescer off, ec off"
                    stats["time_extra_attr"] += time.time() - time_start
                if user_attr:
                    time_start = time.time()
                    extended_attr = {}
                    keys = uattr.userattr_list(fd)
                    for key in keys:
                        extended_attr[key] = uattr.userattr_get(fd, key)
                    file_info["user_attributes"] = extended_attr
                    stats["time_user_attr"] += time.time() - time_start
            else:  # Use a standard os.lstat call
                stats["lstat_required"] += 1
                LOG.debug({"msg": "Unable to call os.open. Using os.lstat instead", "file_path": full_path})
                time_start = time.time()
                file_info = get_file_stat(root, filename, phys_block_size, strip_dot_snapshot=strip_dot_snapshot)
                stats["time_lstat"] += time.time() - time_start
            if custom_tagging:
                time_start = time.time()
                file_info["user_tags"] = custom_tagging(file_info)
                stats["time_custom_tagging"] += time.time() - time_start

            # Translate UID/GID/SID to names
            time_start_translate = time.time()
            onefs_auth.translate_user_group_perms(root, filename, file_info, fd=fd, name_lookup=not no_names)
            time_end_translate = time.time()
            stats["time_name"] += time_end_translate - time_start_translate
            # Convert string ACE into an object for Elastic
            aces = file_info.get("perms_acl_aces", [])
            if aces:
                file_info["perms_acl_aces"] = [
                    {key: value for key, value in zip(["principal", "perm_type", "perms"], ace.split())} for ace in aces
                ]

            # Filter out keys if requested
            if filter_fields:
                for key in list(file_info.keys()):
                    if key not in filter_fields:
                        del file_info[key]
            stats["time_filter"] += time.time() - time_end_translate

            if file_info["file_type"] == "dir":
                # Set values for directories
                file_info["file_is_inlined"] = False
                file_info["size_logical"] = 0
                result_dir_list.append(file_info)
                continue
            if file_info["file_type"] in ["fifo", "socket", "symlink"]:
                # Fix size issues with FIFOs, sockets, and symlinks
                file_info["size_logical"] = 0
            result_list.append(file_info)
            stats["processed"] += 1
        except IOError as ioe:
            stats["skipped"] += 1
            if ioe.errno == errno.EACCES:  # 13: No access
                LOG.warn({"msg": "Permission error", "file_path": full_path})
            else:
                LOG.exception(ioe)
        except FileNotFoundError as fnfe:
            stats["not_found"] += 1
            LOG.warn({"msg": "File not found", "file_path": full_path})
        except PermissionError as pe:
            stats["skipped"] += 1
            LOG.warn({"msg": "Permission error", "file_path": full_path})
            LOG.exception(pe)
        except Exception as e:
            stats["skipped"] += 1
            LOG.warn({"msg": "Exception during file scan", "file_path": full_path, "error": str(e)})
            LOG.exception(e)
        finally:
            try:
                os.close(fd)
            except:
                pass
    if custom_state:
        custom_stats = custom_state.get("stats")
        if custom_stats:
            for field in STATS_FIELDS:
                custom_stats[field] += stats[field]
    results = {
        "dirs": result_dir_list,
        "files": result_list,
        "statistics": stats,
    }
    stats["time_scan_dir"] = time.time() - start_scanner
    return results


def get_file_stat(root, filename, block_unit=STAT_BLOCK_SIZE, strip_dot_snapshot=True):
    full_path = os.path.join(root, filename)
    try:
        fstats = os.lstat(full_path)
    except Exception as e:
        LOG.exception("Exception in os.lstat")
        return None
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
        "perms_unix_bitmask": "{0:o}".format(stat.S_IMODE(fstats.st_mode)),
        "perms_unix_gid": int(fstats.st_gid),
        "perms_unix_uid": int(fstats.st_uid),
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
