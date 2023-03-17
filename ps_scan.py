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
import optparse
import os
import queue
import socket
import stat
import sys
import scanit
import threading
import time

try:
    import isi.fs.attr as attr
    import isi.fs.diskpool as dp
    import isi.fs.userattr as uattr

    OS_TYPE = 2
except:
    OS_TYPE = 1

USAGE = "usage: %prog [OPTION...] PATH... [PATH..]"
EPILOG = """
Quickstart
====================
The --es-cred-file or the --es-url and optionally both --es-user and --es-pass need to
be present for the script to send output to an Elasticsearch endpoint.

The --es-cred-file is a credentials file that contains up to 4 lines of data.
The first line is the user name
The second is the password
The third, optional line is the index
The fourth, optional line is the URL

If you specify the URL you must specify the index as well.
This es-cred-file is sensitive and should be properly secured.

Command line options
Some options can significantly reduce scan speed. The following options may cause scan
speeds to be reduced by more than half:
  * extra
  * tagging
  * user-attr

Custom tagging file format
====================
TBD
"""
CMD_EXIT = 0
CMD_SEND = 1
DEFAULT_ES_THREADS = 4
DEFAULT_LOG_FORMAT = "%(asctime)s - %(module)s|%(funcName)s - %(levelname)s [%(lineno)d] %(message)s"
DEFAULT_MAX_Q_SIZE = 1000
DEFAULT_MAX_Q_WAIT_LOOPS = 100
DEFAULT_SEND_Q_SLEEP = 0.5
DEFAULT_STATS_OUTPUT_INTERVAL = 30
DEFAULT_THREAD_COUNT = 6
FORMAT = "%(asctime)s %(message)s"
LOG = logging.getLogger(__name__)

ACCESS_PATTERN = [
    "concurrency",
    "streaming",
    "random",
    "disabled",
    "invalid",
]
FILE_TYPE = {
    0o010000: "fifo",
    0o020000: "char",
    0o040000: "dir",
    0o060000: "block",
    0o100000: "file",
    0o120000: "symlink",
    0o140000: "socket",
}
FILE_TYPE_MASK = 0o170000
IFS_BLOCK_SIZE = 8192
PS_SCAN_MAPPING = {
    "properties": {
        # ========== Timestamps ==========
        "atime": {"type": "long"},
        "atime_date": {"type": "date", "format": "yyyy-MM-dd"},
        "btime": {"type": "long"},
        "btime_date": {"type": "date", "format": "yyyy-MM-dd"},
        "ctime": {"type": "long"},
        "ctime_date": {"type": "date", "format": "yyyy-MM-dd"},
        "mtime": {"type": "long"},
        "mtime_date": {"type": "date", "format": "yyyy-MM-dd"},
        # ========== File and path strings ==========
        "root": {"type": "keyword"},
        "filename": {"type": "keyword"},
        "extension": {"type": "keyword"},
        # ========== File attributes ==========
        "access_pattern": {"type": "keyword"},
        "file_compressed": {"type": "boolean"},
        "file_compression_ratio": {"type": "float"},
        "file_dedupe_disabled": {"type": "boolean"},
        "file_deduped": {"type": "boolean"},
        "file_has_ads": {"type": "boolean"},
        "file_inlined": {"type": "boolean"},
        "file_packed": {"type": "boolean"},
        "file_smartlinked": {"type": "boolean"},
        "file_sparse": {"type": "boolean"},
        "file_type": {"type": "keyword"},
        "hard_links": {"type": "short"},
        "inode": {"type": "long"},
        "inode_mirror_count": {"type": "byte"},
        "inode_parent": {"type": "long"},
        "inode_revision": {"type": "long"},
        # ========== Storage pool targets ==========
        "pool_target_data": {"type": "keyword"},
        "pool_target_data_name": {"type": "keyword"},
        "pool_target_metadata": {"type": "keyword"},
        "pool_target_metadata_name": {"type": "keyword"},
        # ========== Permissions ==========
        "perms_gid": {"type": "long"},
        "perms_uid": {"type": "long"},
        "perms_unix": {"type": "short"},
        # ========== File protection level ==========
        "protection_current": {"type": "keyword"},
        "protection_target": {"type": "keyword"},
        # ========== File allocation size and blocks ==========
        "size": {"type": "long"},
        "size_logical": {"type": "long"},
        "size_physical": {"type": "long"},
        "size_physical_data": {"type": "long"},
        "size_protection": {"type": "long"},
        # ========== SSD usage ==========
        "ssd_strategy": {"type": "short"},
        "ssd_strategy_name": {"type": "keyword"},
        "ssd_status": {"type": "short"},
        "ssd_status_name": {"type": "keyword"},
        "size_metadata": {"type": "integer"},
        "manual_access": {"type": "boolean"},
        "manual_packing": {"type": "boolean"},
        "manual_protection": {"type": "boolean"},
        # ========== User attributes ==========
        "tags": {"type": "keyword"},
        "user_attributes": {"type": "object"},
    }
}
SSD_STRATEGY = [
    "metadata read",
    "avoid ssd",
    "data on ssd",
    "metadata write",
    "invalid",
]
SSD_STATUS = [
    "metadata restripe",
    "data restripe",
    "restriping",
    "complete",
    "invalid",
]
# Inode flag bit fields
# fmt: off
IFLAGS_UF_NODUMP        = 0x00000001 # do not dump file
IFLAGS_UF_IMMUTABLE     = 0x00000002 # file may not be changed
IFLAGS_UF_APPEND        = 0x00000004 # writes to file may only append
IFLAGS_UF_OPAQUE        = 0x00000008 # directory is opaque wrt. union
IFLAGS_UF_NOUNLINK      = 0x00000010 # file may not be removed or renamed
IFLAGS_UF_INHERIT       = 0x00000020 # unused but set on all files
IFLAGS_UF_WRITECACHE    = 0x00000040 # writes are cached (Is this reverse bit? 0 is enable 1 is disable?)
IFLAGS_UF_WC_INHERIT    = 0x00000080 # unused but set on all new files
IFLAGS_UF_DOS_NOINDEX   = 0x00000100
IFLAGS_UF_ADS           = 0x00000200 # file is ADS directory or stream
IFLAGS_UF_HASADS        = 0x00000400 # file has ADS dir
IFLAGS_UF_WC_ENDURANT   = 0x00000800 # write cache is endurant  (Is this reverse bit? 0 is enable 1 is disable?)
IFLAGS_UF_SPARSE        = 0x00001000
IFLAGS_UF_REPARSE       = 0x00002000
IFLAGS_UF_ISI_UNUSED1   = 0x00004000
IFLAGS_UF_DOS_OFFLINE   = 0x00008000
IFLAGS_UF_DOS_ARCHIVE   = 0x10000000
IFLAGS_UF_DOS_HIDDEN    = 0x20000000
IFLAGS_UF_DOS_RO        = 0x40000000
IFLAGS_UF_DOS_SYSTEM    = 0x80000000
# Super user changeable flags
IFLAGS_SF_ARCHIVED      = 0x00010000
IFLAGS_SF_IMMUTABLE     = 0x00020000
IFLAGS_SF_APPEND_ONLY   = 0x00040000
IFLAGS_SF_STUBBED       = 0x00080000
IFLAGS_SF_NO_UNLINK     = 0x00100000
IFLAGS_SF_SNAP_INODE    = 0x00200000
IFLAGS_SF_NO_SNAP_INODE = 0x00400000
IFLAGS_SF_STUBBED_CACHE = 0x00800000
IFLAGS_SF_HAS_NTFS_ACL  = 0x01000000
IFLAGS_SF_HAS_NTFS_OG   = 0x02000000
IFLAGS_SF_PARENT_UPGD   = 0x04000000
IFLAGS_SF_BACKUP_SPARSE = 0x08000000
# Combination flags
IFLAG_COMBO_UF_DOS_ATTR = IFLAGS_UF_DOS_OFFLINE | IFLAGS_UF_DOS_ARCHIVE | IFLAGS_UF_DOS_HIDDEN | IFLAGS_UF_DOS_RO | IFLAGS_UF_DOS_SYSTEM
IFLAG_COMBO_STUBBED     = IFLAGS_SF_STUBBED | IFLAGS_SF_STUBBED_CACHE
# fmt: on
"""
Unused dinode fields:
  di_at_r_repair
  di_at_r_repairing
  di_attr_bytes
  di_bad_sin
  di_bug_124996
  di_create_timensec
  di_ctimensec
  di_data_bytes
  di_data_depth
  di_data_fmt
  di_dir_version
  di_in_cstat
  di_inode_version
  di_istrashdir
  di_la_drivecount
  di_magic
  di_max_pg_n
  di_mtimensec
  di_packing_incomplete
  di_packing_target
  di_parent_hash
  di_recovered_flag
  di_restripe_state
  di_stream # Normal is 0, 2 is usually for CloudPools targeting the writes to local cache
  di_upgrade_async
  di_upgrading
"""


def add_parser_options(parser):
    parser.add_option(
        "-t",
        "--type",
        type="choice",
        choices=("basic", "onefs"),
        default=None,
        help="""Scan type to use.                                     
basic: Works on all file systems.                     
onefs: Works on OneFS based file systems.             
""",
    )
    parser.add_option(
        "--extra",
        action="store_true",
        default=False,
        help="Add additional file information on OneFS systems",
    )
    # parser.add_option(
    #    "--tagging",
    #    action="store",
    #    default=None,
    #    help="Turn on custom tagging based on tagging rules specified in the file. See documentation for file format",
    # )
    parser.add_option(
        "--user-attr",
        action="store_true",
        default=False,
        help="Retrieve user defined extended attributes from each file",
    )
    parser.add_option(
        "--stats-interval",
        action="store",
        type="int",
        default=DEFAULT_STATS_OUTPUT_INTERVAL,
        help="""Stats update interval in seconds.                     
Default: %default
""",
    )
    group = optparse.OptionGroup(parser, "Performance options")
    group.add_option(
        "--threads",
        action="store",
        type="int",
        default=DEFAULT_THREAD_COUNT,
        help="""Number of file scanning threads.                      
Default: %default
""",
    )
    group.add_option(
        "--advanced",
        action="store_true",
        default=False,
        help="Flag to enable advanced options",
    )
    parser.add_option_group(group)
    group = optparse.OptionGroup(parser, "Elasticsearch options")
    group.add_option(
        "--es-url",
        action="store",
        default=None,
        help="Full URL to Elasticsearch endpoint",
    )
    group.add_option(
        "--es-index",
        action="store",
        default=None,
        help="Index to insert data into",
    )
    group.add_option(
        "--es-init-index",
        action="store_true",
        default=False,
        help="When set, the script will initialize the index before uploading data",
    )
    group.add_option(
        "--es-user",
        action="store",
        default=None,
        help="Elasticsearch user",
    )
    group.add_option(
        "--es-pass",
        action="store",
        default=None,
        help="Elasticsearch password",
    )
    group.add_option(
        "--es-cred-file",
        action="store",
        default=None,
        help="File holding the user name and password, on individual lines, for Elasticsearch",
    )
    group.add_option(
        "--es-threads",
        action="store",
        type="int",
        default=DEFAULT_ES_THREADS,
        help="""Number of threads to send data to Elasticsearch.      
Default: %default
""",
    )
    group.add_option(
        "--es-max-send-q-size",
        action="store",
        type="int",
        default=DEFAULT_MAX_Q_SIZE,
        help="""Number of unsent entries in the Elasticsearch send    
queue before throttling file scanning.                
Default: %default
""",
    )
    group.add_option(
        "--es-send-q-sleep",
        action="store",
        type="int",
        default=DEFAULT_SEND_Q_SLEEP,
        help="""When max send queue size is reached, sleep each file  
scanner by this value in seconds to slow scanning.    
Default: %default
""",
    )
    parser.add_option_group(group)
    group = optparse.OptionGroup(parser, "Logging and debug options")
    group.add_option(
        "--log",
        default=None,
        help="Full path and file name for log output.  If not set, no log output to file will be generated",
    )
    group.add_option(
        "--console-log",
        action="store_true",
        default=False,
        help="When this flag is set, log output to console",
    )
    group.add_option(
        "--quiet",
        action="store_true",
        default=False,
        help="When this flag is set, do not log output to console",
    )
    group.add_option(
        "--debug",
        default=0,
        action="count",
        help="Add multiple debug flags to increase debug",
    )
    parser.add_option_group(group)


def add_parser_options_advanced(parser):
    group = optparse.OptionGroup(parser, "ADVANCED options")
    group.add_option(
        "--dirq-chunk",
        action="store",
        type="int",
        default=scanit.DEFAULT_QUEUE_DIR_CHUNK_SIZE,
        help="""Number of directories to put into each work chunk.    
Default: %default
""",
    )
    group.add_option(
        "--dirq-priority",
        action="store",
        type="int",
        default=scanit.DEFAULT_DIR_PRIORITY_COUNT,
        help="""Number of threads that are biased to process          
directories.                                          
Default: %default
""",
    )
    group.add_option(
        "--fileq-chunk",
        action="store",
        type="int",
        default=scanit.DEFAULT_QUEUE_FILE_CHUNK_SIZE,
        help="""Number of files to put into each work chunk.          
Default: %default
""",
    )
    group.add_option(
        "--fileq-cutoff",
        action="store",
        type="int",
        default=scanit.DEFAULT_FILE_QUEUE_CUTOFF,
        help="""When the number of files in the file queue is less    
than this value, bias threads to process directories. 
Default: %default
""",
    )
    group.add_option(
        "--fileq-min-cutoff",
        action="store",
        type="int",
        default=scanit.DEFAULT_FILE_QUEUE_MIN_CUTOFF,
        help="""When the number of files in the file queue is less    
than this value, only process directories if possible.
Default: %default
""",
    )
    group.add_option(
        "--q-poll-interval",
        action="store",
        type="int",
        default=scanit.DEFAULT_POLL_INTERVAL,
        help="""Number of seconds to wait in between polling events   
for the statistics and ES send loop.                  
Default: %default
""",
    )
    parser.add_option_group(group)


def es_data_sender(send_q, cmd_q, url, username, password, index_name, poll_interval=scanit.DEFAULT_POLL_INTERVAL):
    es_client = elasticsearchlite.ElasticsearchLite()
    es_client.username = username
    es_client.password = password
    es_client.endpoint = url

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
            elif cmd == CMD_SEND:
                # TODO: Optimize this section by using a byte buffer and writing directly into the buffer?
                bulk_data = []
                work_items = cmd_item[1]
                for i in range(len(work_items)):
                    bulk_data.append(json.dumps({"index": {"_id": work_items[i]["inode"]}}))
                    bulk_data.append(json.dumps(work_items[i]))
                    work_items[i] = None
                bulk_str = "\n".join(bulk_data)
                resp = es_client.bulk(bulk_str, index_name=index_name)
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
    LOG.debug("Creating new index with mapping: %s" % index)
    es_client = elasticsearchlite.ElasticsearchLite()
    es_client.username = username
    es_client.password = password
    es_client.endpoint = url
    resp = es_client.create_index(index, mapping=PS_SCAN_MAPPING)
    if resp["status"] != 200:
        if resp["error"]["type"] != "resource_already_exists_exception":
            LOG.error(json.dumps(resp["error"]))
    LOG.debug("Create index response: %s" % resp)


def file_handler_basic(root, filename_list, stats, args={}):
    """
    The file handler returns a dictionary:
    {
      "processed": <int>                # Number of files actually processed
      "skipped": <int>                  # Number of files skipped
    }
    """
    start_time = args.get("start_time", time.time())
    thread_state = args.get("thread_state", {})
    custom_state = args.get("custom_state", {})
    max_send_q_size = custom_state.get("max_send_q_size", DEFAULT_MAX_Q_SIZE)
    send_q_sleep = custom_state.get("send_q_sleep", DEFAULT_SEND_Q_SLEEP)
    processed = 0
    skipped = 0
    dir_list = []
    result_list = []
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
            if stat.S_ISDIR(fstats.st_mode):
                # Save directories to re-queue
                dir_list.append(filename)
                continue
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
            stats["file_size_total"] += fstats.st_size
            processed += 1
            result_list.append(file_info)
        except Exception as e:
            skipped += 1
            LOG.exception(e)
    if result_list:
        custom_state["send_q"].put([CMD_SEND, result_list])
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

    processed = 0
    skipped = 0
    dir_list = []
    result_list = []

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
            result_list.append(file_info)
            if fstats["di_mode"] & 0o040000:
                # Fix size issues with dirs
                file_info["size_logical"] = 0
                # Save directories to re-queue
                dir_list.append(filename)
                continue
            elif fstats["di_mode"] & 0o120000:
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
    if result_list:
        custom_state["send_q"].put([CMD_SEND, result_list])
        for i in range(DEFAULT_MAX_Q_WAIT_LOOPS):
            if custom_state["send_q"].qsize() > max_send_q_size:
                time.sleep(send_q_sleep)
            else:
                break
    return {"processed": processed, "skipped": skipped, "q_dirs": dir_list}


def init_custom_state(custom_state, options):
    custom_state["extra_stats"] = options.extra
    custom_state["max_send_q_size"] = options.es_max_send_q_size
    custom_state["node_pool_translation"] = {}
    custom_state["phys_block_size"] = IFS_BLOCK_SIZE
    custom_state["send_q"] = queue.Queue()
    custom_state["send_q_sleep"] = options.es_send_q_sleep
    custom_state["get_user_attr"] = options.user_attr
    # TODO: Parse the custom tag input file and produce a parser
    custom_state["custom_tagging"] = None
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
    # Create our command line parser. We use the older optparse library for compatibility on OneFS
    optparse.OptionParser.format_epilog = lambda self, formatter: self.epilog
    parser = optparse.OptionParser(
        usage=USAGE,
        version="%prog v" + __version__ + " (" + __date__ + ")",
        epilog=EPILOG,
    )
    add_parser_options(parser)
    if "--advanced" in sys.argv:
        add_parser_options_advanced(parser)
    (options, args) = parser.parse_args(sys.argv[1:])

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
